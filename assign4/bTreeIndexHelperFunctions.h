#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"
#include "storage_mgr.h"
#include "hashTable.h"
#include "page_replacement.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "tables.h"
#include "btree_mgr.h"

extern int findKeyIntLeafNode(Value *nodeKeys, Value *key, int n);
extern int findKeyFloattLeafNode(Value *nodeKeys, Value *key, int n);
extern int findKeyStringLeafNode(Value *nodeKeys, Value *key, int n);
extern int findChildPageIntKey(Value *nodeKeys, Value *key, int n);
extern int findChildPageFloatKey(Value *nodeKeys, Value *key, int n);
extern int findChildPageStringtKey(Value *nodeKeys, Value *key, int n);
extern RC findKeyIntNode(BTreeNodePage *nodePage, Value *key, int *keyIndex);
extern RC findKeyFloattNode(BTreeNodePage *nodePage, Value *key, int *keyIndex);
extern RC findKeyStringNode(BTreeNodePage *nodePage, Value *key, int *keyIndex);
extern RC putIntKey(BTreeNodePage *nodePage, Value *key, RID rid, int keyIndex);
extern RC putFloatKey(BTreeNodePage *nodePage, Value *key, RID rid, int keyIndex);
extern RC putStringKey(BTreeNodePage *nodePage, Value *key, RID rid, int keyIndex);
extern RC updateBTreeManagementPage(BTreeHandle *tree);
extern RC freeBMPageHandle(BM_PageHandle *ph);
extern bool checkNodeFull(int numKeys, int maxNumKeys);
extern BTreeNodePage *createNodePage(int pageNum, DataType keyType, int n);
extern RC freeBTreeNodePage(BTreeNodePage *nodePage);
extern RC updateBTreeNodePageInBufManager(BTreeHandle *tree, BTreeNodePage *nodePage, BM_PageHandle *ph);
extern bool updateOldNode(BTreeHandle *tree, Value *key, RID rid,
                   BTreeNodePage *nodePage, int nodeMaxElements, bool newKeyPlaced, int oldIndex);
extern bool updateNewNode(BTreeHandle *tree, Value *key, RID rid,
                   BTreeNodePage *oldNodePage, BTreeNodePage *newNodePage,
                   int newNodeMaxElements, bool newKeyPlaced, int *lastIndexVisited);
