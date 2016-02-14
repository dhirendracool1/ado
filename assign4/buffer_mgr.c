#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "hashTable.h"
#include "page_replacement.h"
#include "storage_mgr.h"

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Buffer Manager Interface Pool Handling

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		const int numPages, ReplacementStrategy strategy,
		void *stratData) {

        if(pageFileName == NULL)
            return RC_FILE_NAME_NOT_PROVIDED;

        if(numPages <= 0)
            return RC_WRONG_NUMBER_OF_FRAMES;

        int i;

	// Update bufferpool variables.
        bm->pageFile = (char *) malloc(strlen(pageFileName) + 1);
        strcpy(bm->pageFile, pageFileName);
	bm->numPages = numPages;
	bm->strategy = strategy;

        // Initialize mgmt  data
        bm->mgmtData = (BM_MgmtData *) malloc(sizeof(BM_MgmtData));
	bm->mgmtData->readIO = 0;
	bm->mgmtData->writeIO = 0;
	bm->mgmtData->emptyFrames = numPages;

	// Define head node for the page replacement linked list.
//	bm->mgmtData->head = (BM_PageReplaceList *) malloc(sizeof(BM_PageReplaceList));
	bm->mgmtData->head = NULL;

  //      bm->mgmtData->last = (BM_PageReplaceList *) malloc(sizeof(BM_PageReplaceList));
        bm->mgmtData->last = NULL;
//		bm->mgmtData->last = NULL;
		
  //      bm->mgmtData->refPointer = (BM_PageReplaceList *) malloc(sizeof(BM_PageReplaceList));
        bm->mgmtData->refPointer = NULL;

	// Initialize hash table details.
        bm->mgmtData->hashT = (BM_HashTable *) malloc (sizeof(BM_HashTable));
//        bm->mgmtData->hashT->hashArray = (PageInfo **) malloc (sizeof(PageInfo *));

        for(i=0; i < numPages; i++) {
                // Initialize pageInfo in the frame.
                bm->mgmtData->hashT->hashArray[i] = (PageInfo *) malloc (sizeof(PageInfo));
                bm->mgmtData->hashT->hashArray[i]->pageHandle = (BM_PageHandle *) malloc (sizeof(BM_PageHandle));
                bm->mgmtData->hashT->hashArray[i]->pageHandle->data = (char *) malloc (PAGE_SIZE);
		strcpy(bm->mgmtData->hashT->hashArray[i]->pageHandle->data, "\0");
        }

        for(i=0; i < numPages; i++) {
                bm->mgmtData->hashT->hashArray[i]->fixCount = 0;
                bm->mgmtData->hashT->hashArray[i]->dirtyFlag = false;

                // Initialize pageHandle in the frame.
//                bm->mgmtData->hashT->hashArray[i]->pageHandle = (BM_PageHandle *) malloc (sizeof(BM_PageHandle));
                bm->mgmtData->hashT->hashArray[i]->pageHandle->pageNum = NO_PAGE;

                // Allocate memory for data in the page frame.
  //              bm->mgmtData->hashT->hashArray[i]->pageHandle->data = (char *) malloc (PAGE_SIZE);
        }

	bm->mgmtData->hashT->arraySize = numPages;

        // Initialize file handle for the buffer pool.
        bm->mgmtData->fh = (SM_FileHandle *) malloc (sizeof(SM_FileHandle));
	openPageFile (bm->pageFile, bm->mgmtData->fh);

	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {

	RC retCode;
	// Get the head node of the linked list.
	BM_PageReplaceList *current = bm->mgmtData->last;

	while(current != NULL) {
		if(current->pageFrameHandle->fixCount == 0) {
			// If fix count is 0, then proceed, else throw error.
			if(current->pageFrameHandle->dirtyFlag) {
                                // If dirty flag is set, write page back to the disk.
				BM_PageHandle *const page = current->pageFrameHandle->pageHandle;
				if(forcePage(bm, page) != RC_OK) {
					return RC_FORCE_FLUSH_FAILED;
				}
			}
                        current = current->prevFrame;
		} else {
                        // Throw error as buffer pool is still in use.
			return RC_POOL_IN_USE;
		}
	}

	int i;
        // Deallocate memory allocated for frame data and page handle.
        for(i=0; i < bm->numPages; i++) {
                free(bm->mgmtData->hashT->hashArray[i]->pageHandle->data);	// Char *
                free(bm->mgmtData->hashT->hashArray[i]->pageHandle);		// BM_PageHandle
                free(bm->mgmtData->hashT->hashArray[i]);			// PageInfo
        }

//        free(bm->mgmtData->hashT->hashArray);
        // Close file and deallocate memory assigned to file handle.
        closePageFile(bm->mgmtData->fh);

        // Deallocate memory assigned for each link in the linked list.
        current = bm->mgmtData->last;
        while(current->prevFrame != NULL) {
                free(current->nextFrame);
                current = current->prevFrame;
        }
        free(current);

        free(bm->mgmtData->fh);		// Deallocate memory for SM_FileHandle
        free(bm->mgmtData->hashT);	// Deallocation of PageInfo **
        free(bm->mgmtData);		// Deallocation of BM_MgmtData
        free(bm->pageFile);		// Deallocation of page file

	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {

	int i;
	if(bm == NULL)
		return RC_BUFFER_MGR_NOT_INIT;

	// Get the head node of the linked list.
	BM_PageReplaceList *current = bm->mgmtData->head;

	while(current != NULL) {
		if(current->pageFrameHandle->fixCount == 0)
		{
			if(current->pageFrameHandle->dirtyFlag)
			{
				BM_PageHandle *const page = current->pageFrameHandle->pageHandle;
				if(forcePage(bm, page) != RC_OK)
				{
					return RC_FORCE_FLUSH_FAILED;
				}
			}
		}
		current = current->nextFrame;
	}

	return RC_OK;
}

// **************************************

// Buffer Manager Interface Access Pages

// Marks a page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	// set the dirty flag for the page frame
	int p = find(page->pageNum, bm->mgmtData->hashT);
        memcpy(bm->mgmtData->hashT->hashArray[p]->pageHandle->data, page->data, PAGE_SIZE);
//	bm->mgmtData->hashT->hashArray[p]->pageHandle->data = page->data;
	setDirtyFlag(bm->mgmtData->hashT,p,true) ;
	return RC_OK;
}

// unpins the page
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	int	p = find(page->pageNum, bm->mgmtData->hashT);
	if(p>=0) // page found
	{	
//		if(bm->mgmtData->hashT->hashArray[p]->dirtyFlag) {
//			forcePage(bm, bm->mgmtData->hashT->hashArray[p]->pageHandle);
//		}
		bm->mgmtData->hashT->hashArray[p]->fixCount--;
		return RC_OK;
	}
	else
		return RC_PAGE_NOT_FOUND;
}

// should write the current content of the page back to the page file on disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	SM_PageHandle ph, temp;
	RC retCode;

	//ph = (SM_PageHandle) malloc(PAGE_SIZE);
	//ph = page->data;
	retCode = writeBlock (page->pageNum, bm->mgmtData->fh, page->data);

	if(retCode == RC_OK)
	{
		//current->pageFrameHandle->dirtyFlag = false;
		int p = find(page->pageNum, bm->mgmtData->hashT);
		setDirtyFlag(bm->mgmtData->hashT,p,false);
		bm->mgmtData->writeIO++;
		return RC_OK;
	}
	else
		return RC_FORCE_FAILED;
}


// pins the page with page number given
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
		const PageNumber pageNum){

	// check if bm has been initialized
	if(bm == NULL)
		return RC_BUFFER_MGR_NOT_INIT;

	// check if PageNumber passed is a valid one
	if(pageNum < 0)
		return RC_INVALID_PAGENUMBER;

	int	frameNum = find(pageNum, bm->mgmtData->hashT);
	if(frameNum>=0) // page found
	{
		page->pageNum = pageNum;
		page->data = (char *) malloc(PAGE_SIZE);//strlen(bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data) + 1);
		memcpy(page->data, bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data, PAGE_SIZE);
//		page->data = bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data;
		bm->mgmtData->hashT->hashArray[frameNum]->fixCount++;
		// update the linked list for the page pinned
		updateList(bm,frameNum);
		return RC_OK;
	}
	else  // page not found
	{
		// check if empty frames are present
		if(bm->mgmtData->emptyFrames==0){
			// call page replacement function
			frameNum = replacePage(bm,pageNum);
                        if(frameNum == -1)
                            return RC_NO_FRAME_WAIT;
		}
		else
		{
			// insert new pageFrame into Hash Table
			//int frameNum;
			frameNum = findPageFrame(pageNum,bm->mgmtData->hashT);
			updatePageFrameList(bm,pageNum,frameNum,true);
			bm->mgmtData->emptyFrames--;
		}

                page->pageNum = pageNum;
                page->data = (char *) malloc(PAGE_SIZE);//strlen(bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data) + 1);
                memcpy(page->data, bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data, PAGE_SIZE);
//		page->data = bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data;

		return RC_OK;
	}
}

// **************************************

// Statistics Interface

PageNumber *getFrameContents (BM_BufferPool *const bm) {
    int i;
    PageNumber *pnArray = (PageNumber *) malloc (sizeof(PageNumber) * bm->numPages);
    for (i = 0; i < bm->numPages; ++i) {
        pnArray[i] = bm->mgmtData->hashT->hashArray[i]->pageHandle->pageNum;
    }
    return pnArray;
}

bool *getDirtyFlags (BM_BufferPool *const bm) {
    int i;
    bool *dArray = (bool *) malloc(sizeof(bool) * bm->numPages);
    for (i = 0; i <  bm->numPages; ++i) {
        dArray[i] = bm->mgmtData->hashT->hashArray[i]->dirtyFlag;
    }
    return dArray;
}

int *getFixCounts (BM_BufferPool *const bm) {
    int i;
    int *fcArray = (int *) malloc(sizeof(int) * bm->numPages);
    for (i = 0; i <  bm->numPages; ++i) {
        fcArray[i] = bm->mgmtData->hashT->hashArray[i]->fixCount;
    }
    return fcArray;
}

int getNumReadIO (BM_BufferPool *const bm) {
    return bm->mgmtData->readIO;
}

int getNumWriteIO (BM_BufferPool *const bm) {
    return bm->mgmtData->writeIO;
}
// **************************************

