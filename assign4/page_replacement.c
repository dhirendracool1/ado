#include <stdio.h>
#include <stdlib.h>
#include "page_replacement.h"
#include "dberror.h"
#include "storage_mgr.h"

RC addLink(BM_BufferPool *const bm, BM_PageReplaceList *newLink) {

    if(bm->mgmtData->head == NULL) {
        // If this link is the first link in the list
        newLink->nextFrame = NULL;
        bm->mgmtData->last = newLink;
        bm->mgmtData->refPointer = newLink;
    } else {
        // If not first link, then
        newLink->nextFrame = bm->mgmtData->head;    // Assign next link of newLink to head
        bm->mgmtData->head->prevFrame = newLink;    // Assign heads prev link to this newLink
}
    newLink->prevFrame = NULL;        // Assign previous link of newLink to NULL
    bm->mgmtData->head = newLink;                   // Make newLink as head.
    newLink->refBit = 1;		// Set refBit as 1, for CLOCK pg rplc strategy

    return RC_OK;
}

void updateRefBit(BM_BufferPool *const bm, int frameNum, int pageNum) {
    BM_PageReplaceList *current = bm->mgmtData->head;

    while(current != NULL) {

        if((current->frameNum != frameNum) || (current->pageNum != pageNum)) {
            current = current->nextFrame;
        } else {
            current->refBit = 1;
            break;
        }
    }
}

RC deleteLink(BM_BufferPool *const bm, BM_PageReplaceList *linkHandle) {

    if(bm->mgmtData->head == NULL)
        return RC_NULL_LIST;        // Empty linked list.

    BM_PageReplaceList *current = bm->mgmtData->head;

    while(current != NULL) {

        if((current->frameNum != linkHandle->frameNum) || (current->pageNum != linkHandle->pageNum)) {
            current = current->nextFrame;
        } else {

            if((current == bm->mgmtData->head) && (current == bm->mgmtData->last)) {
                // Only one node present in the list.
                bm->mgmtData->head = NULL;
                bm->mgmtData->last = NULL;

            } else if(current == bm->mgmtData->head) {
                // If node to be removed is head node.
                bm->mgmtData->head = current->nextFrame;
                current->nextFrame->prevFrame = NULL;

            } else if(current->nextFrame == NULL) {
                // If node to be deleted is last node.
                bm->mgmtData->last = current->prevFrame;
                current->prevFrame->nextFrame = NULL;
            } else {
                // Middle node.
                current->prevFrame->nextFrame = current->nextFrame;
                current->nextFrame->prevFrame = current->prevFrame;
            }

            free(current);
            return RC_OK;
        }
    }

    return RC_NO_SUCH_LINK;
}

void updateList(BM_BufferPool *const bm, int frameNum){

        BM_PageReplaceList *newLink = (BM_PageReplaceList *) malloc(sizeof(BM_PageReplaceList));

        switch(bm->strategy)
        {
        case RS_FIFO:
                break;
        case RS_LRU:
                newLink->frameNum = frameNum;
                newLink->pageNum = bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->pageNum;
                newLink->pageFrameHandle = bm->mgmtData->hashT->hashArray[frameNum];
                deleteLink(bm,newLink);
                addLink(bm,newLink);
                break;
        case RS_CLOCK:
                updateRefBit(bm, frameNum, bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->pageNum);
                break;
        default:
                break;
        }
}

BM_PageReplaceList *findLink(BM_PageReplaceList *head, int frameNum, int pageNum) {

    if(head == NULL)
        return NULL;

    BM_PageReplaceList *current = head;

    while(current->nextFrame != NULL) {
        if(current->frameNum == frameNum && current->pageNum == pageNum)
            return current;
        else
            current = current->nextFrame;
    }

    return NULL;    // Node not found.
}

int replacePage(BM_BufferPool *const bm, int pageNum) {

    int frameNum;

    switch(bm->strategy) {
        case RS_FIFO:
        case RS_LRU:
            frameNum = findPageToReplace(bm);
            if(frameNum < 0)
                return RC_NO_FRAME_AVAILABLE;
            break;
        case RS_CLOCK:
            frameNum = findPageToReplaceClock(bm);
            if(frameNum < 0)
                return RC_NO_FRAME_AVAILABLE;
            break;
        default:
            return RC_INVALID_REPLACEMENT_STRATEGY;
            break;
    }

    if(updatePageFrameList(bm, pageNum, frameNum, false) == RC_OK)
        return frameNum;

    return -1;

}
    
RC updatePageFrameList(BM_BufferPool *const bm,int pageNum,int frameNum, bool newFrame) {

    RC retCode;
    SM_FileHandle *tempFh = bm->mgmtData->fh;
    retCode = readBlock(pageNum, tempFh, bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data);

    // Update BM_PageHandle in hash table.
    bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->pageNum = pageNum;    // BM_PageHandle
//    bm->mgmtData->hashT->hashArray[frameNum]->pageHandle->data = ph;    // BM_PageHandle
    bm->mgmtData->hashT->hashArray[frameNum]->fixCount = 1;    // BM_PageInfo
    bm->mgmtData->hashT->hashArray[frameNum]->dirtyFlag = false;    // BM_PageInfo
    bm->mgmtData->readIO++;

    if((bm->strategy == RS_CLOCK) && (!newFrame)) {
        BM_PageReplaceList *link = bm->mgmtData->refPointer->nextFrame;
        if(bm->mgmtData->refPointer->nextFrame == NULL)
            link = bm->mgmtData->head;

        link->frameNum = frameNum;
        link->pageNum = pageNum;
        link->pageFrameHandle = bm->mgmtData->hashT->hashArray[frameNum];
        link->refBit = 1;
    } else {
        // Update page replacement linked list with this new information.
        BM_PageReplaceList *newLink = (BM_PageReplaceList *) malloc(sizeof(BM_PageReplaceList));
        newLink->frameNum = frameNum;
        newLink->pageNum = pageNum;
        newLink->pageFrameHandle = bm->mgmtData->hashT->hashArray[frameNum];

        addLink(bm,newLink);
    }
    return RC_OK;
}

int findPageToReplace(BM_BufferPool *const bm) {

    int retFrameNum = -1;
    BM_PageReplaceList *current = bm->mgmtData->last;

    while(current != NULL) {

        // Check whether the fix count of the page is 0 or not.
        if(current->pageFrameHandle->fixCount == 0) {

            // If fix count of the page is 0, then we will use this page to replace.
            BM_PageHandle *const page = current->pageFrameHandle->pageHandle;
            if(current->pageFrameHandle->dirtyFlag) {

                // If dirty flag for this page is set, then write it down to the disk
            	if(forcePage(bm, page) != RC_OK)
                {
                    return RC_PAGE_REPLACE_FAIL;    // Failed to write page back to the disk.
                                                    // NOTE: Make this RC code as negative.
                }
            }
            retFrameNum = current->frameNum;
            deleteLink(bm, current);
            return retFrameNum;        // Found the frame to be used for replacement.
        } else {
            /* As current page, i.e. probable page to be replaced is being used by client.
             * Do not replace it. See the next link. */
            current = current->prevFrame;
        }
    }

    return RC_CANNOT_REPLACE_PAGE;        // No frame found to replace.
                                          // NOTE: Make this RC code as negative.
}

int findPageToReplaceClock(BM_BufferPool *const bm) {

    int retFrameNum = -1, counter = 0;
    BM_PageReplaceList *current = bm->mgmtData->refPointer;

    while((current != NULL) && (counter <= bm->numPages)) {
        if(current->pageFrameHandle->fixCount == 0) {

            // If fix count of the page is 0, then we will use this page to replace.
            if(current->refBit == 0) {
                BM_PageHandle *const page = current->pageFrameHandle->pageHandle;
                if(current->pageFrameHandle->dirtyFlag) {

                    // If dirty flag for this page is set, then write it down to the disk
                    if(forcePage(bm, page) != RC_OK)
                    {
                        return RC_PAGE_REPLACE_FAIL;    // Failed to write page back to the disk.
                                                        // NOTE: Make this RC code as negative.
                    }
                }
                retFrameNum = current->frameNum;
                if(current->prevFrame == NULL) {	// refPointer will point to the next page, so that it will be used evict that page.
                    bm->mgmtData->refPointer = bm->mgmtData->last;
                } else {
                    bm->mgmtData->refPointer = current->prevFrame;
                }
                return retFrameNum;        // Found the frame to be used for replacement.
            } else {
                current->refBit = 0;
                if(current->prevFrame == NULL) {
                    current = bm->mgmtData->last;	// Wraparound
                } else {
                    current = current->prevFrame;	// Next Frame
                }
            }
        } else {
            /* As current page, i.e. probable page to be replaced is being used by client.
             * Do not replace it. See the next link. */
            if(current->prevFrame == NULL) {
                current = bm->mgmtData->last;		// Wraparound
            } else {
                current = current->prevFrame;		// Next Frame
            }
            counter++;
        }
    }

    return RC_CANNOT_REPLACE_PAGE;        // No frame found to replace.
                                          // NOTE: Make this RC code as negative.
}
