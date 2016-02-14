#include "storage_mgr.h"
#include "dberror.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

// macros
#define ZRO 0

/* manipulating page files */
void initStorageManager(void) {
	// TO DO;
}

RC createPageFile(char *fileName) {
	FILE *storageMgrFp = fopen(fileName, "w");
	char *str = (char *) malloc(PAGE_SIZE);

	if (storageMgrFp != NULL) {
		memset(str, '\0', PAGE_SIZE);

		// If fseek failed. Throw error.
		if (fseek(storageMgrFp, FILE_HEADER_SIZE, SEEK_SET) != 0)
			return RC_WRITE_SEEK_FAILED;

		//write a first empty block after file header
		if (PAGE_SIZE != fwrite(str, 1, PAGE_SIZE, storageMgrFp))
			return RC_CHUNK_NOT_FULL_WRITTEN;

		// If data is written successfully, write header information.
		if (fseek(storageMgrFp, 0, SEEK_SET) != 0)
			return RC_WRITE_SEEK_FAILED;

                int firstPage = 0, lastPage = 0;
                int totPages = 1, emptyBlocks = 1;
		fwrite(&firstPage, sizeof(int), 1, storageMgrFp);
                fwrite(&lastPage, sizeof(int), 1, storageMgrFp);
		fwrite(&totPages, sizeof(int), 1, storageMgrFp);
		fwrite(&emptyBlocks, sizeof(int), 1, storageMgrFp);

		fclose(storageMgrFp);
		free(str);
		return RC_OK;
	} else {
		// Failed to create a file
		// Return RC value as mentioned in the dberror.h
		free(str);
		return RC_FILE_CRT_FAILED;
	}
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {

	// Check for the existence of file. If not open throw error.
	struct stat fileCheck;
	if(stat(fileName, &fileCheck) != 0)
		return RC_FILE_NOT_FOUND;

	FILE *storageMgrFp = fopen(fileName, "r+");

	// If failed to open a file throw error.
	if (storageMgrFp == NULL)
		return RC_FILE_NOT_OPENED;

	// Allocate some memory for mgmtInfo, to store data into it.
	fHandle->mgmtInfo = (mgmtInfo_Handle *) malloc (sizeof(mgmtInfo_Handle));
	if (fseek(storageMgrFp, 0, SEEK_SET) != 0)
		return RC_WRITE_SEEK_FAILED;

	// Update mgmtInfo with information stored in file header.
        int firstPage, lastPage, totPages, emptyBlocks;
	fread(&firstPage, sizeof(int), 1, storageMgrFp);
	fread(&lastPage, sizeof(int), 1, storageMgrFp);
	fread(&totPages, sizeof(int), 1, storageMgrFp);
	fread(&emptyBlocks, sizeof(int), 1, storageMgrFp);

        fHandle->mgmtInfo->firstPagePos = firstPage;
        fHandle->mgmtInfo->lastPagePos = lastPage;
        fHandle->mgmtInfo->numOfEmptyBlocks = emptyBlocks;
        fHandle->mgmtInfo->numOfPages = totPages;

	fHandle->fileName = fileName;
	fHandle->curPagePos = 0;
	fHandle->totalNumPages = fHandle->mgmtInfo->numOfPages;
	// Set mgmtInfo
	fHandle->mgmtInfo->filePointer = storageMgrFp;
	return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {

	// If faced issues while closing file, throw error.
	if (fclose(fHandle->mgmtInfo->filePointer) != 0)
		return RC_FILE_NOT_CLOSED;

	// File closed successfully.
	free(fHandle->mgmtInfo);
	return RC_OK;
}

RC destroyPageFile(char *filename) {

	// File deleted successfully.
	if (remove(filename) == 0)
		return RC_OK;

	// Error deleting file.
	return RC_FILE_DEL_FAILED;
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If page number given is negative, throw error.
	if (pageNum < 0)
		return RC_INVALID_PAGE_NUMBER;

	// If page number given does not fit into the
	// available number pages in file, throw error.
	if (pageNum > fHandle->totalNumPages)
		return RC_READ_NON_EXISTING_PAGE;

	// Set current page position to the given page number.
	//fHandle->curPagePos = pageNum * PAGE_SIZE + FILE_HEADER_SIZE;
	fHandle->curPagePos = pageNum;
	return readCurrentBlock(fHandle, memPage);
}

int getBlockPos(SM_FileHandle *fHandle) {
	return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If current page position does not point to the first page,
	// then set it to first page.
	/*if ((fHandle->curPagePos * PAGE_SIZE) != (fHandle->mgmtInfo->firstPagePos - 1))
		fHandle->curPagePos = (fHandle->mgmtInfo->firstPagePos - 1);
	 */
	if ((fHandle->curPagePos) != (fHandle->mgmtInfo->firstPagePos))
		fHandle->curPagePos = (fHandle->mgmtInfo->firstPagePos);

	return readCurrentBlock(fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If current page position points to the first page,
	// that means we do not have previous block, throw error.
	if (fHandle->curPagePos == (fHandle->mgmtInfo->firstPagePos))
		return RC_READ_NON_EXISTING_PAGE;

	// Set current page position to the previous block.
	fHandle->curPagePos --;
	return readCurrentBlock(fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If file pointer is not initialized, throw error.
	if (fHandle->mgmtInfo->filePointer == NULL)
		return RC_FILE_POINTER_NOT_AVAILABLE;

	// If fseek failed. Throw error.
	if (fseek(fHandle->mgmtInfo->filePointer, ((fHandle->curPagePos * PAGE_SIZE) + FILE_HEADER_SIZE), SEEK_SET) != 0)
		return RC_READ_SEEK_FAILED;

	// Read a block from file.
	fread(memPage, 1, PAGE_SIZE, fHandle->mgmtInfo->filePointer);

	// If read failed, throw read error.
	if (ferror(fHandle->mgmtInfo->filePointer))
		return RC_READ_ERROR;

	// If found end of file, say so.
	//if (feof(fHandle->mgmtInfo->filePointer))
	//	return RC_EOF;

	return RC_OK;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If current page position points to the last page,
	// then we cannot read next page, as we won't have it.
	if ((fHandle->curPagePos) >= (fHandle->mgmtInfo->lastPagePos))
		return RC_READ_NON_EXISTING_PAGE;

	// Set current page position to the next block.
	fHandle->curPagePos ++;

	return readCurrentBlock(fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If current page position does no point to the last block,
	// set its value o the last page, to read it.
	if (fHandle->curPagePos != fHandle->mgmtInfo->lastPagePos)
		fHandle->curPagePos = fHandle->mgmtInfo->lastPagePos;

	return readCurrentBlock(fHandle, memPage);
}

/* writing blocks to a page file */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If page handle is null, throw error.
	if (memPage == NULL)
		return RC_PAGE_HANDLE_NOT_INIT;

	// If page number given is negative, throw error.
	if (pageNum < 0)
		return RC_INVALID_PAGE_NUMBER;

	// If page number given does not fit into the
	// available number pages in file, throw error.
	if (pageNum > fHandle->totalNumPages)
		return RC_INVALID_PAGE_NUMBER;

	// Set current page position to the given page number.
	fHandle->curPagePos = pageNum;

	return writeCurrentBlock(fHandle, memPage);
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	// If page handle is null, throw error.
	if (memPage == NULL)
		return RC_PAGE_HANDLE_NOT_INIT;
	int num;

	// If file pointer is not initialized, throw error.
	if (fHandle->mgmtInfo->filePointer == NULL)
		return RC_FILE_POINTER_NOT_AVAILABLE;

	// If fseek failed. Throw error.
	if (fseek(fHandle->mgmtInfo->filePointer, ((fHandle->curPagePos * PAGE_SIZE) + FILE_HEADER_SIZE), SEEK_SET)
			!= 0)
		return RC_WRITE_SEEK_FAILED;

	// Write a block from file.
	num = fwrite(memPage, 1, PAGE_SIZE, fHandle->mgmtInfo->filePointer);
	if (num < PAGE_SIZE) {

		return RC_WRITE_FAILED;

	} else {
		// Increment current page position by PAGE_SIZE,
		// to point to the next page.
		/*fHandle->curPagePos += PAGE_SIZE;
		if (fHandle->curPagePos > fHandle->totalNumPages * PAGE_SIZE) {
			fHandle->totalNumPages++;
			fHandle->mgmtInfo->lastPagePos = fHandle->curPagePos;
		}
		 */
		fHandle->curPagePos ++;
		if (fHandle->curPagePos > fHandle->totalNumPages ) {
			fHandle->totalNumPages++;
			fHandle->mgmtInfo->lastPagePos = fHandle->curPagePos-1;
		}

	}

	fHandle->mgmtInfo->numOfPages = fHandle->totalNumPages;
//	fHandle->mgmtInfo->current_time = time(NULL);


	// Update file header.
        if (fseek(fHandle->mgmtInfo->filePointer, sizeof(int)-1, SEEK_SET) != 0)
                return RC_WRITE_SEEK_FAILED;

        int lastPage = fHandle->mgmtInfo->lastPagePos;
        int totPages = fHandle->mgmtInfo->numOfPages; 
        int emptyBlocks = fHandle->mgmtInfo->numOfEmptyBlocks;
        fwrite(&lastPage, sizeof(int), 1, fHandle->mgmtInfo->filePointer);
        fwrite(&totPages, sizeof(int), 1, fHandle->mgmtInfo->filePointer);
        fwrite(&emptyBlocks, sizeof(int), 1, fHandle->mgmtInfo->filePointer);
	return RC_OK;
}


RC appendEmptyBlock(SM_FileHandle *fHandle) {
	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	char *str = (char *) malloc(PAGE_SIZE);
	int num;

	memset(str, '0', PAGE_SIZE);
	// If fseek failed. Throw error.
	if (fseek(fHandle->mgmtInfo->filePointer, 0, SEEK_END) != 0) {
	        free(str);
		return RC_WRITE_SEEK_FAILED;
        }

	// Write an empty block from file.
	num = fwrite(str, 1, PAGE_SIZE, fHandle->mgmtInfo->filePointer);
	if (num < PAGE_SIZE) {
	        free(str);
		return RC_WRITE_FAILED;

	} else {

		// Increase total number of pages by one
		fHandle->totalNumPages++;
		// Increment current page position by PAGE_SIZE,
                // to point to the next page.
                fHandle->curPagePos = fHandle->totalNumPages-1;
		fHandle->mgmtInfo->lastPagePos = fHandle->curPagePos;
		fHandle->mgmtInfo->numOfPages = fHandle->totalNumPages;
//		fHandle->mgmtInfo->current_time = time(NULL);

		if (fseek(fHandle->mgmtInfo->filePointer, sizeof(int)-1, SEEK_SET)
                        != 0) {
			free(str);
	                return RC_WRITE_SEEK_FAILED;
		}
		
		// Update file header.
		int lastPage = fHandle->mgmtInfo->lastPagePos;
                int totPages = fHandle->mgmtInfo->numOfPages;
		int emptyBlocks = fHandle->mgmtInfo->numOfEmptyBlocks;
                fwrite(&lastPage, sizeof(int), 1, fHandle->mgmtInfo->filePointer);
                fwrite(&totPages, sizeof(int), 1, fHandle->mgmtInfo->filePointer);
                fwrite(&emptyBlocks, sizeof(int), 1, fHandle->mgmtInfo->filePointer);

		free(str);
		return RC_OK;
	}
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

	// If file handle is null, throw error.
	if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
	//check if total number of pages is less than number of pages
	if (fHandle->totalNumPages < numberOfPages) {
		while (fHandle->totalNumPages != numberOfPages) {
			//add empty blocks
			appendEmptyBlock(fHandle);
		}
	}
	return RC_OK;
}
