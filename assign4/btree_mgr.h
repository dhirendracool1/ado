#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

// structure for accessing btrees
typedef struct BTreeMgmtData {
    BM_BufferPool *bm;
    char *pageData;
    int rootNodePageNum;
    int maxNumKeys;
    int treeNumNodes;
    int treeNumEntries;
} BTreeMgmtData;

typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  BTreeMgmtData *mgmtData;
} BTreeHandle;

typedef struct BT_ScanMgmtData {
       int currentPage;
       int currentPointer;
}BT_ScanMgmtData;

typedef struct BT_ScanHandle {
       BTreeHandle *tree;
       BT_ScanMgmtData *mgmtData;
} BT_ScanHandle;

typedef struct Node {
    Value *keys;       // array of keys pointed by the node
    RID *recordsPtr;  // pointers to nodes array
} Node;

typedef struct BTreeNodePage {
    int pageNum;        // Page number containing node.
    DataType keyType;   // Type of keys the node contains.
    int numKeys;        // Total number of keys present in this node.
    int maxNumKeys;
    int numPointers;    // Total number of pointers in the node.
    bool isLeaf;        // True, if leaf node.
    bool isRoot;        // True, if root node. False if not.
    Node *bTreeNode;    // BTree node in a page.
} BTreeNodePage;

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

extern RC searchAndInsertKey(BTreeHandle *tree, Value *key, RID rid, int nodePageNum, Value *returnKey, int *returnNewPageNum, bool *noOverFlow);
extern RC performSimpleInsert(BTreeHandle *tree, Value *key, RID rid, BTreeNodePage *nodePage);
extern RC handleRootOverflow(BTreeHandle *tree, Value *rKey, RID rid, BTreeNodePage *nodePage, bool isFromLeaf);
extern RC handleNonLeafOverflow(BTreeHandle *tree, Value *rKey, RID rid, BTreeNodePage *nodePage, Value *returnKey, int *returnNewPageNum);
extern RC handleLeafOverFlow(BTreeHandle *tree, Value *key, RID rid, BTreeNodePage *nodePage, Value *returnKey, int *returnNewPageNum);

extern BTreeNodePage *deSerializeBTreeNodePage(char *pageInfo);
extern RC serializeBTreeNodePage(BTreeNodePage *nodePage, char *pageInfo);

extern int findKeyIntLeafNode(Value *nodeKeys, Value *key, int n);
extern int findKeyFloatLeafNode(Value *nodeKeys, Value *key, int n);
extern int findKeyStringLeafNode(Value *nodeKeys, Value *key, int n);
extern int findChildPageIntKey(Value *nodeKeys, Value *key, int n);
extern int findChildPageFloatKey(Value *nodeKeys, Value *key, int n);
extern int findChildPageStringKey(Value *nodeKeys, Value *key, int n);
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
                   BTreeNodePage *nodePage, int nodeMaxElements, bool newKeyPlaced, int oldIndex, bool fromNonLeaf);
extern bool updateNewNode(BTreeHandle *tree, Value *key, RID rid,
                   BTreeNodePage *oldNodePage, BTreeNodePage *newNodePage,
                   int newNodeMaxElements, bool newKeyPlaced, int *lastIndexVisited, bool fromNonLeaf);
extern void printNodeData(BTreeNodePage *nodePage);
extern void printTreeInfor(BTreeHandle *tree);
#endif // BTREE_MGR_H
