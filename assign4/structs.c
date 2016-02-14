// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  BTreeMgmtData *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;

typedef struct BTreeMgmtData {
    BM_BufferPool *bm;
    char *pageData;
    int rootNodePageNum;
    int maxNumKeys;
    int treeNumNodes;
    int treeNumEntries;
}

typedef struct BTreeNodePage {
    int pageNum;	// Page number containing node.
    DataType keyType;	// Type of keys the node contains.
    int numKeys;	// Total number of keys present in this node.
    int numPointers;	// Total number of pointers in the node.
    bool isLeaf;	// True, if leaf node.
    bool isRoot;	// True, if root node. False if not.
    Node *bTreeNode;    // BTree node in a page.
} BTreeNodePage;

typedef struct Node {
    Value *keys;       // array of keys pointed by the node
    RID **recordsPtr;  // pointers to nodes array
} Node;


    // Allocate memory for a node page.
    BTreeNodePage *nodePage = (BTreeNodePage *) malloc (sizeof(BTreeNodePage *));
    nodePage->pageNum = 0;              // Initial Page Num.
    nodePage->keyType = keyType;
    nodePage->numKeys = 0;        // Initially 0 keys.
    nodePage->numPointers = 0;    // Initially 0 pointers.
    nodePage->maxNumKeys = n;     // Max number of keys a node can accomodate.
    nodePage->isLeaf = true;        // True, leaf node.
    nodePage->isRoot = true;        // True, root node.

    // Allocate memory for tree node structure.
    nodePage->bTreeNode = (Node *) malloc(sizeof(Node));

    // Allocate memory for keys.
    nodePage->bTreeNode->keys = (Value *) malloc(sizeof(Value) * n);

    // Allocate memory for pointers in the tree.
    nodePage->bTreeNode->recordsPtr = (RID *) malloc(sizeof(RID) * n);
    int i;
    for(i=0; i < n; i++) {
        nodePage->bTreeNode->recordsPtr[i].page = -1;
        nodePage->bTreeNode->recordsPtr[i].slot = -1;
    }

