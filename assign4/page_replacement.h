# include <stdio.h>
# include "buffer_mgr.h"

/****************************************************************
 *	Interfaces to deal with replacement strategies		*
 ***************************************************************/

extern RC addLink(BM_BufferPool *const bm, BM_PageReplaceList *newLink);
extern RC deleteLink(BM_BufferPool *const bm, BM_PageReplaceList *linkHandle);
extern BM_PageReplaceList *findLink(BM_PageReplaceList *head, int frameNum, int pageNum);
extern int replacePage(BM_BufferPool *const bm, int pageNum);
extern void updateList(BM_BufferPool *const bm, int frameNum);
int findPageToReplace(BM_BufferPool *const bm);
int findPageToReplaceClock(BM_BufferPool *const bm);
extern RC updatePageFrameList(BM_BufferPool *const bm,int pageNum,int frameNum, bool newFrame);
void updateRefBit(BM_BufferPool *const bm, int frameNum, int pageNum);
