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
#include "btree_mgr.h"


// init and shutdown index manager
RC initIndexManager (void *mgmtData) {
    // TO DO
    return RC_OK;
}

RC shutdownIndexManager () {
    // TO DO
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n) {

    // Create a pagefile for B-Tree index structure.
    if (createPageFile(idxId) != RC_OK)
        return RC_TABLE_CREATION_FAILED;

    // Initialize bufferpool for the this page file/Table.
    BM_BufferPool *bm = MAKE_POOL();
    if (initBufferPool(bm, idxId, 2, RS_FIFO, NULL) != RC_OK)
        return RC_BUFFER_INIT_FAILED;

    // Initialize tree data.
    int rootNodePageNum = -1;	// Initially no root when file created.
    int maxNumKeys = n;		// Max num keys a node can have.
    int treeNumNodes = 0;	// Initially no nodes in the tree.
    int treeNumEntries = 0;	// Initially 0 entries in the tree.

    // Initial page data is ready. Put it into the page 0.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    pinPage(bm, ph, 0);

    // Copy required data on the page.
    size_t copyFromByte = 0;
    memmove(ph->data + copyFromByte, &keyType, sizeof(DataType));
    copyFromByte += sizeof(DataType);
    memmove(ph->data + copyFromByte, &rootNodePageNum, sizeof(int));
    copyFromByte += sizeof(int);
    memmove(ph->data + copyFromByte, &maxNumKeys, sizeof(int));
    copyFromByte += sizeof(int);
    memmove(ph->data + copyFromByte, &treeNumNodes, sizeof(int));
    copyFromByte += sizeof(int);
    memmove(ph->data + copyFromByte, &treeNumEntries, sizeof(int));

    // Mark page 0 as dirty page and write page with table info.
    if (markDirty(bm, ph) != RC_OK)
        return RC_CLDNT_WRITE_PAGE;

    // Unpin page 0.
    if (unpinPage(bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    // Write it down to the disk.
    if (forcePage(bm, ph) != RC_OK)
        return RC_FORCE_FLUSH_FAILED;

    // Shut down bufferpool.
    if (shutdownBufferPool(bm) != RC_OK)
        return RC_BUFFERPOOL_SHUTDOWN_FALED;

    if(bm)
        free(bm); // Free buffer pool manager
    freeBMPageHandle(ph);

    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId) {

    if(idxId == NULL)
        return RC_FILENAME_NOT_GIVEN;

    // Allocate memory for BTreeHandle
//    tree = (BTreeHandle **) malloc(sizeof(BTreeHandle *));
    (*tree) = (BTreeHandle *) malloc(sizeof(BTreeHandle));

    // Allocate memory for BTree file name.
    (*tree)->idxId = (char *) malloc(strlen(idxId) + 1);
    strcpy((*tree)->idxId, idxId);

    // Allocate memory for BTree mgmtData
    (*tree)->mgmtData = (BTreeMgmtData *) malloc(sizeof(BTreeMgmtData));

    // Allocate memory for bufferpool
    (*tree)->mgmtData->bm = MAKE_POOL();
    if (initBufferPool((*tree)->mgmtData->bm, idxId, 10, RS_FIFO, NULL) != RC_OK)
        return RC_BUFFER_INIT_FAILED;

    // Read in first page and get the root node/first node.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    if (pinPage((*tree)->mgmtData->bm, ph, 0) != RC_OK) // Page 0 contains schema
        return RC_READ_BTREE_INFO_FAILED;

    // Read in BTree mgmt data from first page.
    size_t leaveSizeBytes = 0;
    memcpy(&(*tree)->keyType, ph->data + leaveSizeBytes, sizeof(DataType));
    leaveSizeBytes += sizeof(DataType);
    memcpy(&(*tree)->mgmtData->rootNodePageNum, ph->data + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);
    memcpy(&(*tree)->mgmtData->maxNumKeys, ph->data + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);
    memcpy(&(*tree)->mgmtData->treeNumNodes, ph->data + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);
    memcpy(&(*tree)->mgmtData->treeNumEntries, ph->data + leaveSizeBytes, sizeof(int));

    if (unpinPage((*tree)->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;
    
    // Free rest of the data
    freeBMPageHandle(ph);

    return RC_OK;
    
}

RC closeBtree (BTreeHandle *tree) {

    if(tree == NULL)
        return RC_ARG_NULL;

    // Bufferpool no longer required, shutdown bufferpool
    if (shutdownBufferPool(tree->mgmtData->bm) != RC_OK)
        return RC_BUFFERPOOL_SHUTDOWN_FAILED;

    // Free buffer pool manager.
    if(tree->mgmtData->bm)
        free(tree->mgmtData->bm);

    // Free memory allocated for pageData
//    if(tree->mgmtData->pageData)
  //      free(tree->mgmtData->pageData);

    // Free mgmtData memory
    if(tree->mgmtData)
        free(tree->mgmtData);

    // Free memory allocated for file name.
    if(tree->idxId)
        free(tree->idxId);

    // Free memory allocated for tree handle.
    if(tree)
        free(tree);

    return RC_OK;
}

RC deleteBtree (char *idxId) {

    // Check file name is passed or not.
    if(idxId == NULL)
        return RC_FILE_NOT_PROVIDED;

    if(destroyPageFile(idxId) != RC_OK)
        return RC_INDEX_NOT_DELETED;

    return RC_OK;
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result) {
    *result = tree->mgmtData->treeNumNodes;
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    *result = tree->mgmtData->treeNumEntries;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result) {
    // Put the datatype of the key used in the result.
    *result = tree->keyType;
    return RC_OK;
}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result) {

    if(tree == NULL)
        return RC_TREEHANDLE_NOT_INITIALIZED;

    if(key == NULL)
        return RC_NULL_KEY;

    // Access page related to the required node. Initially, root node.
    int nodePageNum = tree->mgmtData->rootNodePageNum;
    bool keyFound = false;

    while(!keyFound) {
        BM_PageHandle *ph = MAKE_PAGE_HANDLE();
        RC retCode = pinPage(tree->mgmtData->bm, ph, nodePageNum);
        if (retCode != RC_OK) {
            freeBMPageHandle(ph);        // Free BM page handle.
            return retCode;
        }

        BTreeNodePage *nodePage = deSerializeBTreeNodePage(ph->data);
        if(nodePage->isLeaf) {
            // Check for the key, in this node.
            int keyIndex = -1;
            switch(key->dt) {
                case DT_INT:
                    // Int value comparison.
                    keyIndex = findKeyIntLeafNode(nodePage->bTreeNode->keys, key, tree->mgmtData->maxNumKeys);
                    break;
                case DT_FLOAT:
                    // Float value comparison.
                    keyIndex = findKeyFloatLeafNode(nodePage->bTreeNode->keys, key, tree->mgmtData->maxNumKeys);
                    break;
                case DT_STRING:
                    // String Value comparison.
                    keyIndex = findKeyStringLeafNode(nodePage->bTreeNode->keys, key, tree->mgmtData->maxNumKeys);
                    break;
                case DT_BOOL:
                    // No indexing for bool => Useless indexing.
                    freeBMPageHandle(ph);        // Free BM page handle.
                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return RC_BOOL_INDEX_NOT_SUPPORTED;
                default:
                    freeBMPageHandle(ph);        // Free BM page handle.
                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return RC_WRONG_DATATYPE;
            }
            if(keyIndex == -1) { 
                // No key found.
                if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;
                freeBMPageHandle(ph);        // Free BM page handle.
                if(nodePage)
                    freeBTreeNodePage(nodePage);
                return RC_IM_KEY_NOT_FOUND;
            } else {
                // Key found.
//                result = (RID *) malloc(sizeof(RID));
                (*result).page = nodePage->bTreeNode->recordsPtr[keyIndex].page;
                (*result).slot = nodePage->bTreeNode->recordsPtr[keyIndex].slot;

                if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;

                freeBMPageHandle(ph);        // Free BM page handle.
                if(nodePage)
                    freeBTreeNodePage(nodePage);

                return RC_OK;
            }
        } else {
            // Find the next node to look for the key.
            int keyIndex = -1;
            switch(key->dt) {
                case DT_INT:
                    // Int value comparison.
                    keyIndex = findChildPageIntKey(nodePage->bTreeNode->keys, key, nodePage->numKeys);//tree->mgmtData->maxNumKeys);
                    break;
                case DT_FLOAT:
                    // Float value comparison.
                    keyIndex = findChildPageFloatKey(nodePage->bTreeNode->keys, key, tree->mgmtData->maxNumKeys);
                    break;
                case DT_STRING:
                    // String Value comparison.
                    keyIndex = findChildPageStringKey(nodePage->bTreeNode->keys, key, tree->mgmtData->maxNumKeys);
                    break;
                case DT_BOOL:
                    // No indexing for bool => Useless indexing.
                    freeBMPageHandle(ph);        // Free BM page handle.
                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return RC_BOOL_INDEX_NOT_SUPPORTED;
                default:
                    freeBMPageHandle(ph);        // Free BM page handle.
                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return RC_WRONG_DATATYPE;
            }
            // Update child page to be read.
            nodePageNum = nodePage->bTreeNode->recordsPtr[keyIndex].page;
            if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                return RC_UNPIN_FAILED;

            if(nodePage)
                freeBTreeNodePage(nodePage);

            freeBMPageHandle(ph);        // Free BM page handle.
        }
    }
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid) {

    if(tree == NULL)
        return RC_TREEHANDLE_NOT_INITIALIZED;

    if(key == NULL)
        return RC_NULL_KEY;

    BTreeNodePage *nodePage;
    printf("\n ---- INSERTING KEY: %d ----", key->v.intV);
    if(tree->mgmtData->rootNodePageNum == -1) {
        // New node/first node in the BTree. Only for root node.
        nodePage = createNodePage(1, tree->keyType, tree->mgmtData->maxNumKeys);

        // Update BTreeHandle.
        tree->mgmtData->rootNodePageNum = 1;
        tree->mgmtData->treeNumNodes = 1;
        tree->mgmtData->treeNumEntries = 1;

        // Update node data.
        nodePage->numKeys = 1;
        nodePage->numPointers = 2;	// 1 RID and 1 tree (null) pointer.
        nodePage->isLeaf = true;
        nodePage->isRoot = true;

        // Update node with key and respective RID.
        nodePage->bTreeNode->keys[0].dt = key->dt;
        switch(key->dt) {
            case DT_INT:
                nodePage->bTreeNode->keys[0].v.intV = key->v.intV;
                break;
            case DT_FLOAT:
                nodePage->bTreeNode->keys[0].v.floatV = key->v.floatV;
                break;
            case DT_STRING:
                nodePage->bTreeNode->keys[0].v.stringV = (char *) malloc(sizeof(char) * strlen(key->v.stringV));
                strcpy(nodePage->bTreeNode->keys[0].v.stringV, key->v.stringV);
                break;
            case DT_BOOL:
                // No indexing for bool => Useless indexing.
                return RC_BOOL_INDEX_NOT_SUPPORTED;
            default:
                return RC_WRONG_DATATYPE;
        }
        nodePage->bTreeNode->recordsPtr[0] = rid;

        int k=0;
//        printf("\nNode Num: %d\nIndex Num: %d", nodePage->pageNum, k);
//print("\n===> ROOT NODE NUM: %d", tree->mgmtData->rootNodePageNum);
printNodeData(nodePage);      
printTreeInfor(tree);

        BM_PageHandle *ph = MAKE_PAGE_HANDLE();
        ph->pageNum = nodePage->pageNum;
        if (pinPage(tree->mgmtData->bm, ph, nodePage->pageNum) != RC_OK)
            return RC_READ_BTREE_INFO_FAILED;

//        ph->data = (char *) malloc(PAGE_SIZE);
        
        // Serialize and write new node data in case of root node (initial case)
        // or old node in other cases.
        if(serializeBTreeNodePage(nodePage, ph->data) != RC_OK)
            return RC_SERIALIZATION_FAILED;

        // make page dirty
        RC retCode;
        if ((retCode = markDirty(tree->mgmtData->bm, ph)) != RC_OK)
            return retCode;

        // Unpin page.
        if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
            return RC_UNPIN_FAILED;

        // Free BM_PageHandle
        retCode = freeBMPageHandle(ph);
        if(retCode != RC_OK)
            return retCode;

        if(nodePage)
            freeBTreeNodePage(nodePage);

        // Update BTree management page.
        retCode = updateBTreeManagementPage(tree);
        if(retCode != RC_OK)
            return retCode;

        return RC_OK;
    } else {
        // Already there are few nodes present in the BTree, let's find the
        // appropriate node and insert the key into it.

        // Access page related to the required node. Initially, root node.
        int nodePageNum = tree->mgmtData->rootNodePageNum;
        Value returnKey;
        int newNodePageNum;
        bool noOverFlow;
printf("Calling searchAndInsertKey.... From nodePageNum: %d", nodePageNum);
        RC retCode = searchAndInsertKey(tree, key, rid, nodePageNum, &returnKey, &newNodePageNum, &noOverFlow);
        if(retCode != RC_OK) {
            return retCode;
        }

        return RC_OK;
    }
}


RC deleteKey (BTreeHandle *tree, Value *key) {

    /*
        Algo:
          1. Access root node using tree->mgmtData->rootNodePageNum.
          2. Find the leaf node where the given key exists. 
		a. Traverse tree.
		b. Page contains isLeaf var, which suggests node is leaf or not.
		c. Use deSerialize function to get page data in node struct. (Now access isLeaf var).
          3. Delete the key.
          4. Check for node data conditions.
		a. Condition satisfied, reorder keys (move all keys and respective RID's to left in the array.
		b. If not step 5.
          5. Either redistribute or merge nodes/keys.
    */
    return RC_OK;
}


RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {

    if (tree==NULL)
        return RC_ARG_NULL;

    // Allocate memory for BT_ScanHandle handle
    *handle = (BT_ScanHandle *) malloc(sizeof(BT_ScanHandle));

    // Allocate memory for BTreeHandle in scan handle
    (*handle)->tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
    // update BTreeHandle file name in handle's BTreeHandle
    (*handle)->tree->idxId = (char *) malloc (sizeof(char) * (strlen(tree->idxId) + 1));
    strcpy((*handle)->tree->idxId, tree->idxId);
    // update BTreeHandle's DataType in scan handle's BTreeHandle
    (*handle)->tree->keyType = tree->keyType;
    // Allocate memory for BTreeHandle's mgmtData in scan handle's BTreeHandle
    (*handle)->tree->mgmtData = (BTreeMgmtData *) malloc(sizeof(BTreeMgmtData));
    (*handle)->tree->mgmtData->bm = tree->mgmtData->bm;
//    if (initBufferPool((*handle)->tree->mgmtData->bm, tree->idxId, 10, RS_FIFO, NULL) != RC_OK)
  //      return RC_BUFFER_INIT_FAILED;
    (*handle)->tree->mgmtData->pageData = (char *) malloc(PAGE_SIZE);

    // Allocate memory for scan handle related mgmtData
    (*handle)->mgmtData = (BT_ScanMgmtData *) malloc(sizeof(BT_ScanMgmtData));

    // Initialize elements for scan handle
    (*handle)->mgmtData->currentPage = -1;
    (*handle)->mgmtData->currentPointer = -1;
    (*handle)->tree->keyType = tree->keyType;
    (*handle)->tree->mgmtData->rootNodePageNum = tree->mgmtData->rootNodePageNum;
    (*handle)->tree->mgmtData->maxNumKeys = tree->mgmtData->maxNumKeys;
    (*handle)->tree->mgmtData->treeNumNodes = tree->mgmtData->treeNumNodes;
    (*handle)->tree->mgmtData->treeNumEntries = tree->mgmtData->treeNumEntries;

    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    if(handle == NULL)
        return RC_NULL_SCANHANDLE;
    if(handle->tree->mgmtData->rootNodePageNum == -1)
        return RC_IM_NO_MORE_ENTRIES;

    bool checkNode = true;
    // Point to the next pointer
    if(handle->mgmtData->currentPage==-1)
        handle->mgmtData->currentPage = handle->tree->mgmtData->rootNodePageNum;

    /* Infinite loop - breakout only if
          1. No more tuples to read. (RETURN)
          2. If we found a tuple to read. (RETURN)*/
    while(checkNode){
        handle->mgmtData->currentPointer++;
        // pin nodePage
        BM_PageHandle *ph = MAKE_PAGE_HANDLE();
        RC retCode = pinPage(handle->tree->mgmtData->bm, ph, handle->mgmtData->currentPage);
        if(retCode != RC_OK){
            // free memory
            free(ph->data);
            free(ph);
            return RC_READ_NODE_PAGE_FAILED;
        }

        // Get the node page data
        BTreeNodePage *nodePage = deSerializeBTreeNodePage(ph->data);

printf("\n ---------- Scanning Current Node Data ----------");
printf("\n (%d, %d)", handle->mgmtData->currentPage, handle->mgmtData->currentPointer);
        while(!nodePage->isLeaf){
            freeBMPageHandle(ph);
            ph = MAKE_PAGE_HANDLE();
            handle->mgmtData->currentPage = nodePage->bTreeNode->recordsPtr[0].page;
            //pin page
printf("\n Message: This is not a leaf node. Pinning page: %d", handle->mgmtData->currentPage);
            retCode = pinPage(handle->tree->mgmtData->bm, ph, handle->mgmtData->currentPage);
            if(retCode != RC_OK){
                // free memory
                freeBMPageHandle(ph);
                freeBTreeNodePage(nodePage);
                return RC_READ_NODE_PAGE_FAILED;
            }
            freeBTreeNodePage(nodePage);
            nodePage = deSerializeBTreeNodePage(ph->data);
        }

        // check for out of bound error for currentPointer
        //.......

        int page = nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].page;
        int slot = nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].slot;

        if(handle->mgmtData->currentPointer == handle->tree->mgmtData->maxNumKeys) {
            // If current pointer is the sibling pointer.
            if(nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].page == -1) {
                // No more data or keys.
                freeBMPageHandle(ph);
                freeBTreeNodePage(nodePage);
                return RC_IM_NO_MORE_ENTRIES;
            } else {
                // Update current page and pointer values and jump to that page.
                handle->mgmtData->currentPage = nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].page;
                handle->mgmtData->currentPointer = -1;
            }
        } else {
            if(nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].page == -1) {
                handle->mgmtData->currentPointer = -1;
                handle->mgmtData->currentPage = handle->tree->mgmtData->maxNumKeys;
            } else {
                result->page = nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].page;
                result->slot = nodePage->bTreeNode->recordsPtr[handle->mgmtData->currentPointer].slot;
                // free memory
                //freeBMPageHandle(ph);
                freeBTreeNodePage(nodePage);
                return RC_OK;
            }
        }
    }
}

RC closeTreeScan (BT_ScanHandle *handle) {
    if(handle == NULL)
        return RC_NULL_SCANHANDLE;
    free(handle->mgmtData);
    free(handle->tree->mgmtData->pageData);
    free(handle->tree->idxId);
    free(handle->tree->mgmtData);
    free(handle->tree);
    if(handle)
        free(handle);
    return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree) {
    return NULL;
}























/**
 * This function will search for the leaf node for the key to insert and 
 * it will either insert key in that node or will create new node and 
 * will take action accordingly.
 * @param tree - BTreeHandle
 * @param key - Key to be inserted
 * @param rid - RID of the record structure
 * @param nodePageNum - Node to look for.
 * @param returnKey - New key for the parent will be returned by this variable
 * @param returnNewPageNum - Page number of the new node.
 * @return bool - Specifies, whether split was done or not.
 */
RC searchAndInsertKey(BTreeHandle *tree, Value *key, RID rid, int nodePageNum, Value *returnKey, int *returnNewPageNum, bool *noOverFlow) {

    // Allocate memory to read data from BTree page file.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    RC retCode = pinPage(tree->mgmtData->bm, ph, nodePageNum);
    if (retCode != RC_OK) {
        freeBMPageHandle(ph);        // Free BM page handle.
        return retCode;
    }

    BTreeNodePage *nodePage = deSerializeBTreeNodePage(ph->data);
printf("\n---------- Current Node Data: ---------");
printNodeData(nodePage);
    if(nodePage->isLeaf) {
printf("\nMessage: THIS LEAF NODE");
        // This is leaf node, insert the key, in this node.

        // Perform check to see whether we require split to insert key or not
        if(checkNodeFull(nodePage->numKeys, tree->mgmtData->maxNumKeys)) {
            // This means that, we need to split the node and perform insertion.
printf("\nMessage: NODE IS FULL");
            // Update returnValues to be sent to the parent node.
            *noOverFlow = false;        // Node split 

            // New key to be updated in parent.
            //returnKey = (Value *) malloc(sizeof(Value));
            returnKey->dt = key->dt;
            RC retCode;
            //if(nodePage->isRoot) {
              //  retCode = handleRootOverflow(tree, key, rid, nodePage, true);
                //return retCode;
            //} else {
printf("\nMessage: HANDLING LEAF OVERFLOW");
            retCode = handleLeafOverFlow(tree, key, rid, nodePage, returnKey, returnNewPageNum);
            if(retCode != RC_OK) {
                if(nodePage)
                    freeBTreeNodePage(nodePage);
                freeBMPageHandle(ph);        // Free BM page handle.
                return retCode;
            }

            // Update new node data, old node data and management information.
            tree->mgmtData->treeNumEntries++;
            tree->mgmtData->treeNumNodes++;

printf("\n----- Updated NODE and TREE DATA ----");
printNodeData(nodePage);
printTreeInfor(tree);
            // Update BTree node information in the buffer manager.
            retCode = updateBTreeNodePageInBufManager(tree, nodePage, ph);
            if(retCode != RC_OK)
                return retCode;

            // Free BM_PageHandle
            retCode = freeBMPageHandle(ph);
            if(retCode != RC_OK)
                return retCode;

            if(nodePage->isRoot) {
printf("\nIMP: NODE IS ALSO A ROOT NODE");
printf("\nCreating new root..........");
                int newNodePageNum = tree->mgmtData->treeNumNodes + 1;
                BTreeNodePage *newRootNodePage = createNodePage(newNodePageNum, returnKey->dt, tree->mgmtData->maxNumKeys);

                // Update new root node information.    
                newRootNodePage->isLeaf = false;
                newRootNodePage->isRoot = true;
                newRootNodePage->numKeys = 1;
                newRootNodePage->numPointers = 2;
                // Childs of the new root node.
                newRootNodePage->bTreeNode->recordsPtr[0].page = nodePage->pageNum;
                newRootNodePage->bTreeNode->recordsPtr[1].page = *returnNewPageNum;
                    
                newRootNodePage->bTreeNode->keys[0].dt = returnKey->dt;
                switch(returnKey->dt) {
                    case DT_INT:
                        newRootNodePage->bTreeNode->keys[0].v.intV = returnKey->v.intV;
                        break;
                    case DT_FLOAT:
                        newRootNodePage->bTreeNode->keys[0].v.floatV = returnKey->v.floatV;
                        break;
                    case DT_STRING:
                        newRootNodePage->bTreeNode->keys[0].v.stringV = (char *) malloc(strlen(returnKey->v.stringV) + 1);
                        strcpy(newRootNodePage->bTreeNode->keys[0].v.stringV, returnKey->v.stringV);
                        break;
                    case DT_BOOL:
                        // No indexing for bool => Useless indexing.
                        return RC_BOOL_INDEX_NOT_SUPPORTED;
                    default:
                        return RC_WRONG_DATATYPE;
                }

                tree->mgmtData->treeNumNodes++;
                tree->mgmtData->rootNodePageNum = newRootNodePage->pageNum;

                // Update old root node.
                nodePage->isRoot = false;

printf("\n ++++++++++ NEW ROOT DATA ++++++++++");
printNodeData(newRootNodePage);
printTreeInfor(tree);
                // Update new root on the page file.
                ph = MAKE_PAGE_HANDLE();
                ph->pageNum = newRootNodePage->pageNum;
                if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK)
                    return RC_READ_BTREE_INFO_FAILED;

                // Update BTree node information in the buffer manager.
                retCode = updateBTreeNodePageInBufManager(tree, newRootNodePage, ph);
                if(retCode != RC_OK)
                    return retCode;

                // Free BM_PageHandle
                retCode = freeBMPageHandle(ph);
                if(retCode != RC_OK)
                    return retCode;

                if(newRootNodePage)
                    freeBTreeNodePage(newRootNodePage);
            }

            ph = MAKE_PAGE_HANDLE();
            ph->pageNum = nodePage->pageNum;
            if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK)
                    return RC_READ_BTREE_INFO_FAILED;
            // Update BTree node information in the buffer manager.
            retCode = updateBTreeNodePageInBufManager(tree, nodePage, ph);
            if(retCode != RC_OK)
                return retCode;

            // Free BM_PageHandle
            retCode = freeBMPageHandle(ph);
            if(retCode != RC_OK)
                return retCode;

            // Update BTree management page.
            retCode = updateBTreeManagementPage(tree);
            if(retCode != RC_OK)
                return retCode;

            if(nodePage)
                freeBTreeNodePage(nodePage);

            return RC_OK;
            //}
        } else {
            // No need to split. Insert key here only.
printf("\nMessage: NO SPLIT REQUIRED");
            *noOverFlow = true;
            RC retCode = performSimpleInsert(tree, key, rid, nodePage);
            if(retCode == RC_OK) {
                tree->mgmtData->treeNumEntries++;        // Num of entries in tree.
                nodePage->numKeys++;           // Num of keys in the nodes.
                nodePage->numPointers++;       // Num of pointers in the nodes.
printf("\n ------- Updated Node --------");
printNodeData(nodePage);
printTreeInfor(tree);
                // Update BTree node information in the buffer manager.
                retCode = updateBTreeNodePageInBufManager(tree, nodePage, ph);
                if(retCode != RC_OK)
                    return retCode;

                // Free BM_PageHandle
                retCode = freeBMPageHandle(ph);
                if(retCode != RC_OK)
                    return retCode;

                // Update BTree management page.
                retCode = updateBTreeManagementPage(tree);
                if(retCode != RC_OK)
                    return retCode;

                if(nodePage)
                    freeBTreeNodePage(nodePage);

                return RC_OK;
            } else {
                if(nodePage)
                    freeBTreeNodePage(nodePage);
                if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;
                freeBMPageHandle(ph);        // Free BM page handle.
                return retCode;
            }
        }
    } else {
        // This is a non-leaf node.
printf("\nMessage: THIS IS A NON LEAF NODE.");
        // Search for appropriate child node and go there.
        // Find the next node to look for the key.
        int keyIndex = -1;
        switch(key->dt) {
            case DT_INT:
                // Int value comparison.
                keyIndex = findChildPageIntKey(nodePage->bTreeNode->keys, key, nodePage->numKeys);//tree->mgmtData->maxNumKeys);
                break;
            case DT_FLOAT:
                // Float value comparison.
                keyIndex = findChildPageFloatKey(nodePage->bTreeNode->keys, key, nodePage->numKeys);//tree->mgmtData->maxNumKeys);
                break;
            case DT_STRING:
                // String Value comparison.
                keyIndex = findChildPageStringKey(nodePage->bTreeNode->keys, key, nodePage->numKeys);//tree->mgmtData->maxNumKeys);
                break;
            case DT_BOOL:
                // No indexing for bool => Useless indexing.
                freeBMPageHandle(ph);
                if(nodePage)
                    freeBTreeNodePage(nodePage);
                return RC_BOOL_INDEX_NOT_SUPPORTED;
            default:
                freeBMPageHandle(ph);
                if(nodePage)
                    freeBTreeNodePage(nodePage);
                return RC_WRONG_DATATYPE;
        }


        // Call the same function recursively.
        Value *rKey = (Value *) malloc(sizeof(Value));
        rKey->dt = key->dt;
        bool noSplit;		// Helps to identify whether the child was split or not.
        RID newKeyRID;		// In case, of child node is split, help to return pageNum of new node.
        newKeyRID.slot = -1;

        int childPageNum = nodePage->bTreeNode->recordsPtr[keyIndex].page;
        RC retCode;
printf("\n Calling searchAndInsertKey... to work with childPageNum: %d", childPageNum);
        retCode = searchAndInsertKey(tree, key, rid, childPageNum, rKey, &newKeyRID.page, &noSplit);
        if(retCode != RC_OK) {
            freeBMPageHandle(ph);
            if(rKey)
                free(rKey);
            if(nodePage)
                freeBTreeNodePage(nodePage);
            return retCode;
        }


        if(noSplit) {
            // Child has successfully inserted key in it, w/o split.
printf("\nMessage: NO CHILD SPLIT");
printf("\n ---------- Current Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
            *noOverFlow = true;
            if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                return RC_UNPIN_FAILED;
            if(rKey)
                free(rKey);
            if(nodePage)
                freeBTreeNodePage(nodePage);
            if(ph)
                freeBMPageHandle(ph);
            return RC_OK;
        } else {
            // Child was split, now need to insert proper key in this parent.
printf("\nMessage: CHILD SPLIT");
printf("\n ---------- Current Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
            bool nonLeafSplit;
            if(!checkNodeFull(nodePage->numKeys, tree->mgmtData->maxNumKeys)) {
printf("\n INFO: NOT FULL. NO SPLIT");
                *noOverFlow = true;	// No split for non-leaf node.
                RC retCode = performSimpleInsert(tree, rKey, newKeyRID, nodePage);
                if(retCode == RC_OK) {
//                    tree->treeNumEntries++;        // Num of entries in tree.
                    nodePage->numKeys++;           // Num of keys in the nodes.
                    nodePage->numPointers++;       // Num of pointers in the nodes.

printf("\n ---------- Updated Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
                    // Update BTree node information in the buffer manager.
                    retCode = updateBTreeNodePageInBufManager(tree, nodePage, ph);
                    if(retCode != RC_OK)
                        return retCode;

                    // Free BM_PageHandle
                    retCode = freeBMPageHandle(ph);
                    if(retCode != RC_OK)
                        return retCode;

                    // Update BTree management page.
                    retCode = updateBTreeManagementPage(tree);
                    if(retCode != RC_OK)
                        return retCode;

                    if(rKey)
                        free(rKey);

                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return RC_OK;
                } else {
                    if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                        return RC_UNPIN_FAILED;
                    freeBMPageHandle(ph);        // Free BM page handle.
                    if(rKey)
                        free(rKey);
                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return retCode;
                }
            } else {
                *noOverFlow = false;    // Split is required in non-leaf node.
/*
                int medianKeyIndex, numKeysOldNode, numKeysNewNode;
                if((tree->mgmtData->maxNumKeys % 2) == 0) {
                    // If n is even.
                    medianKeyIndex = (tree->mgmtData->maxNumKeys / 2);
                    numKeysOldNode = medianKeyIndex;    // No. of keys in old node after split (if n=6, then 3)
                    numKeysNewNode = numKeysOldNode;    // No. of keys in new node after split (if n=6, then 3)
                } else {
                    medianKeyIndex = (tree->mgmtData->maxNumKeys / 2) + 1;
                    numKeysOldNode = medianKeyIndex;    // No. of keys in old node after split (if n=5, then 3)
                    numKeysNewNode = numKeysOldNode-1;  // No. of keys in new node after split (if n=5, then 2)
                }

                /* Now let's check where this new key from the child will be put.
                   Whether it will be put in the new node, old node or it itself
                   is a median key. *
                if(keyIndex == medianKeyIndex) {
                    /* New key itself is a new index. So we will put this
                       key in the parent. *
                    /* This means that, new node's 0th index will point to 
                       the new node created. Rest will remain same. *
                    int newNodeZeroIndexRID = newKeyRID.page;
                    bool doNotOperateOnOldNode = true;
                } else if (keyIndex > medianKeyIndex) {
                    newNodeZeroIndexRID = medianKeyIndex+1;
                    doNotOperateOnOldNode = true;
                } else {
                    newNodeZeroIndexRID = medianKeyIndex;
                    doNotOperateOnOldNode = false;
                }*/












printf("\nINFO: NON-LEAF SPLIT IS REQUIRED");
                if(nodePage->isRoot) {
printf("\nINFO: THIS IS ROOT");
printf("\n ---------- Current Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
                    retCode = handleRootOverflow(tree, rKey, newKeyRID, nodePage, false);
printf("\n ---------- Updated Tree Info ----------");
//printNodeData(nodePage);
printTreeInfor(tree);
                    if(rKey)
                        free(rKey);
                    if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
                        return RC_UNPIN_FAILED;
                    if(ph)
                        freeBMPageHandle(ph);
                    return retCode;
                } else {
printf("INFO: NORMAL NON-LEAF SLPIT");
                    retCode = handleNonLeafOverflow(tree, rKey, newKeyRID, nodePage, returnKey, returnNewPageNum);
                    if(rKey)
                        free(rKey);
                    if(retCode != RC_OK) {
                        freeBMPageHandle(ph);        // Free BM page handle.
                        return retCode;
                    }

                    // Update new node data, old node data and management information.
           //     tree->treeNumEntries++;
                    tree->mgmtData->treeNumNodes++;
printf("\n ---------- Updated Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
                    // Update BTree node information in the buffer manager.
                    retCode = updateBTreeNodePageInBufManager(tree, nodePage, ph);
                    if(retCode != RC_OK)
                        return retCode;

                    // Free BM_PageHandle
                    retCode = freeBMPageHandle(ph);
                    if(retCode != RC_OK)
                        return retCode;

                    // Update BTree management page.
                    retCode = updateBTreeManagementPage(tree);
                    if(retCode != RC_OK)
                        return retCode;

                    if(nodePage)
                        freeBTreeNodePage(nodePage);
                    return RC_OK;
                }
            }
        }
    }
}


/**
 * This function will simply find the appropriate index for the key in the current node.
 * and will put the data into the node.
 * @param tree - BTreeHandle
 * @param key - key to be inserted.
 * @param rid - RID of the key to be inserted.
 * @param nodePage - pointer the node in the B+Tree to be operated upon.
 */
RC performSimpleInsert(BTreeHandle *tree, Value *key, RID rid, BTreeNodePage *nodePage) {

    // Find appropriate place or index to put in this key.
    int keyIndex = -1;
    RC retCode;
    switch (key->dt) {
        case DT_INT:
            // Int value comparison.
            retCode = findKeyIntNode(nodePage, key, &keyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }

            retCode = putIntKey(nodePage, key, rid, keyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }

            break;
        case DT_FLOAT:
            // Float value comparison.
            retCode = findKeyFloattNode(nodePage, key, &keyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }

            retCode = putFloatKey(nodePage, key, rid, keyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }

            break;
        case DT_STRING:
            // String Value comparison.
            retCode = findKeyStringNode(nodePage, key, &keyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }

            retCode = putStringKey(nodePage, key, rid, keyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }

            break;
    }

    return RC_OK;
}

RC handleRootOverflow(BTreeHandle *tree, Value *rKey, RID rid, BTreeNodePage *nodePage, bool isFromLeaf) {
    // Create new nodes which are required.
//    BTreeNodePage *newNodePage = createNodePage(tree->mgmtData->bm->mgmtData->fh->totalNumPages, rKey->dt, tree->mgmtData->maxNumKeys);
    int newNodePageNum = tree->mgmtData->treeNumNodes + 1;
    BTreeNodePage *newNodePage = createNodePage(newNodePageNum, rKey->dt, tree->mgmtData->maxNumKeys);

    // Update new node information.    
    if(isFromLeaf)
        newNodePage->isLeaf = true;
    else
        newNodePage->isLeaf = false;
    newNodePage->isRoot = false;
    tree->mgmtData->treeNumNodes++;

//    BTreeNodePage *newRootNodePage = createNodePage(tree->mgmtData->bm->mgmtData->fh->totalNumPages, rKey->dt, tree->mgmtData->maxNumKeys);
    newNodePageNum = tree->mgmtData->treeNumNodes + 1;
    BTreeNodePage *newRootNodePage = createNodePage(newNodePageNum, rKey->dt, tree->mgmtData->maxNumKeys);

    // Update new root node information.    
    newRootNodePage->isLeaf = false;
    newRootNodePage->isRoot = true;
    newRootNodePage->numKeys = 1;
    newRootNodePage->numPointers = 2;
    // Childs of the new root node.
    newRootNodePage->bTreeNode->recordsPtr[0].page = nodePage->pageNum;
    newRootNodePage->bTreeNode->recordsPtr[1].page = newNodePage->pageNum;
    tree->mgmtData->treeNumNodes++;
    tree->mgmtData->rootNodePageNum = newRootNodePage->pageNum;

    // Update old root node.
    nodePage->isRoot = false;

    /*
     * For root overflow, we will split the node in two nodes and then
     * we will put the, median key in its parent.
     * Thus,
     * For n = even (8), num keys to be inserted in non-leaf in case of overflow are 9
     * thus, medianKey will be 5 and index will be 4.
     * For n = odd (5) num keys to be inserted in non-leaf are 6,
     * First three in old, next one (median) in its parent and rest two in new node.
     * thus medianKey will be 4 and index will be 3.
     */
    int medianKeyIndex, numKeysOldNode, numKeysNewNode;
    if((tree->mgmtData->maxNumKeys % 2) == 0) {
        // If n is even.
            medianKeyIndex = (tree->mgmtData->maxNumKeys / 2);
            numKeysOldNode = medianKeyIndex;    // No. of keys in old node after split (if n=6, then 3)
            numKeysNewNode = numKeysOldNode;    // No. of keys in new node after split (if n=6, then 3)
    } else {
        // If n is odd.
            medianKeyIndex = (tree->mgmtData->maxNumKeys / 2) + 1;
            numKeysOldNode = medianKeyIndex;    // No. of keys in old node after split (if n=5, then 3)
            numKeysNewNode = numKeysOldNode-1;  // No. of keys in new node after split (if n=5, then 2)
    }

    bool newKeyPlaced = false;
    int oldIndex;
    newKeyPlaced = updateNewNode(tree, rKey, rid, nodePage, newNodePage, numKeysNewNode, newKeyPlaced, &oldIndex, true);

    /* Now, we have updated the new node and data in it. Now, we have reached
     * to the point of median key index. So, let's find the which is the median
     * key and let's use it to put in the new root node.
     */
    newRootNodePage->bTreeNode->keys[0].dt = rKey->dt;
    switch(rKey->dt) {
        case DT_INT:
            if(newKeyPlaced) {
                newRootNodePage->bTreeNode->keys[0].v.intV = nodePage->bTreeNode->keys[oldIndex].v.intV;
                newNodePage->bTreeNode->recordsPtr[0].page = nodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                newNodePage->bTreeNode->recordsPtr[0].slot = nodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
                nodePage->bTreeNode->recordsPtr[oldIndex+1].page = -1;
                nodePage->bTreeNode->recordsPtr[oldIndex+1].slot = -1;
                oldIndex--;
            } else {
                if(nodePage->bTreeNode->keys[oldIndex].v.intV > rKey->v.intV) {
                    newRootNodePage->bTreeNode->keys[0].v.intV = nodePage->bTreeNode->keys[oldIndex].v.intV;
                    newNodePage->bTreeNode->recordsPtr[0].page = nodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                    newNodePage->bTreeNode->recordsPtr[0].slot = nodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
                    nodePage->bTreeNode->recordsPtr[oldIndex+1].page = -1;
                    nodePage->bTreeNode->recordsPtr[oldIndex+1].slot = -1;
                    oldIndex--;
                } else {
                    newRootNodePage->bTreeNode->keys[0].v.intV = rKey->v.intV;
                    newNodePage->bTreeNode->recordsPtr[0].page = rid.page;
                    newNodePage->bTreeNode->recordsPtr[0].slot = rid.slot;
                }
            }
            break;
        case DT_FLOAT:
            if(newKeyPlaced) {
                newRootNodePage->bTreeNode->keys[0].v.floatV = nodePage->bTreeNode->keys[oldIndex].v.floatV;
                oldIndex--;
            } else {
                if(nodePage->bTreeNode->keys[oldIndex].v.floatV > rKey->v.floatV) {
                    newRootNodePage->bTreeNode->keys[0].v.floatV = nodePage->bTreeNode->keys[oldIndex].v.floatV;
                    oldIndex--;
                } else {
                    newRootNodePage->bTreeNode->keys[0].v.floatV = rKey->v.floatV;
                }
            }
//            newRootNodePage->bTreeNode->keys[0].v.floatV = newNodePage->bTreeNode->keys[0].v.floatV;
            break;
        case DT_STRING:
            if(newKeyPlaced) {
                newRootNodePage->bTreeNode->keys[0].v.stringV = (char *) malloc(strlen(nodePage->bTreeNode->keys[0].v.stringV) + 1);
                strcpy(newRootNodePage->bTreeNode->keys[0].v.stringV, nodePage->bTreeNode->keys[0].v.stringV);
                oldIndex--;
            } else {
                if(strcmp(nodePage->bTreeNode->keys[0].v.stringV, rKey->v.stringV) > 0) {
                    newRootNodePage->bTreeNode->keys[0].v.stringV = (char *) malloc(strlen(nodePage->bTreeNode->keys[0].v.stringV) + 1);
                    strcpy(newRootNodePage->bTreeNode->keys[0].v.stringV, nodePage->bTreeNode->keys[0].v.stringV);
                    oldIndex--;
                } else {
                    newRootNodePage->bTreeNode->keys[0].v.stringV = (char *) malloc(strlen(rKey->v.stringV) + 1);
                    strcpy(newRootNodePage->bTreeNode->keys[0].v.stringV, rKey->v.stringV);
                }
            }
            break;
        case DT_BOOL:
            // No indexing for bool => Useless indexing.
            return RC_BOOL_INDEX_NOT_SUPPORTED;
        default:
            return RC_WRONG_DATATYPE;
    }

    /* Now, we have updated new node data, old nodes prev data and the median key,
     * it's time to update rest of the old node.
     */
    newKeyPlaced = updateOldNode(tree, rKey, rid, nodePage, numKeysOldNode, newKeyPlaced, oldIndex, true);

    if(isFromLeaf)
        tree->mgmtData->treeNumEntries++;

printf("\n ---------- New Node Data ----------");
printNodeData(newNodePage);
printf("\n ---------- New Root Data ----------");
printNodeData(newRootNodePage);
printf("\n ---------- Old Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);

    // Update information of this page in the page file.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->pageNum = newNodePage->pageNum;
    if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK)
        return RC_READ_BTREE_INFO_FAILED;

    // Update BTree new node information in the buffer manager.
    RC retCode;
    retCode = updateBTreeNodePageInBufManager(tree, newNodePage, ph);
    if(retCode != RC_OK)
        return retCode;

    // Free BM_PageHandle
    retCode = freeBMPageHandle(ph);
    if(retCode != RC_OK)
        return retCode;

    freeBTreeNodePage(newNodePage);

    // Update old root node on the page file.
    ph = MAKE_PAGE_HANDLE();
    ph->pageNum = nodePage->pageNum;
    if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK) 
        return RC_READ_BTREE_INFO_FAILED;

    // Update BTree node information in the buffer manager.
    retCode = updateBTreeNodePageInBufManager(tree, nodePage, ph);
    if(retCode != RC_OK)
        return retCode;

    // Free BM_PageHandle
    retCode = freeBMPageHandle(ph);
    if(retCode != RC_OK)
        return retCode;

    freeBTreeNodePage(nodePage);

    // Update new root on the page file.
    ph = MAKE_PAGE_HANDLE();
    ph->pageNum = newRootNodePage->pageNum;
    if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK) 
        return RC_READ_BTREE_INFO_FAILED;

    // Update BTree node information in the buffer manager.
    retCode = updateBTreeNodePageInBufManager(tree, newRootNodePage, ph);
    if(retCode != RC_OK)
        return retCode;

    // Free BM_PageHandle
    retCode = freeBMPageHandle(ph);
    if(retCode != RC_OK)
        return retCode;

    freeBTreeNodePage(newRootNodePage);
    return RC_OK;
}

/**
 * This functions handle the non-leaf node overflow.
 * This function will split the non-leaf node and will put the median key, in the parent.
 * @param tree - BTree handle
 * @param rKey - Contains new key sent from child to be updated in the parent.
 * @param rid - Contains new node page num of the new child (new node created) at 1 level below this.
 * @param nodePage - Node which is being handled for the overflow.
 * @return returnKey - Contains key that has to be put in the parent.
 * @return returnNewPageNum - Page number of the new node created.
 * @return RC code - Notify success or failure of this operation.
 */
RC handleNonLeafOverflow(BTreeHandle *tree, Value *rKey, RID rid, BTreeNodePage *nodePage, Value *returnKey, int *returnNewPageNum) {
    // Create new node to handle overflow.
    int newNodePageNum = tree->mgmtData->treeNumNodes + 1;
    BTreeNodePage *newNodePage = createNodePage(newNodePageNum, rKey->dt, tree->mgmtData->maxNumKeys);

    // Update new node information.    
    newNodePage->isLeaf = false;
    newNodePage->isRoot = false;
    *returnNewPageNum = newNodePage->pageNum;

    // Let's find the median key, which will be placed in the parent of this node.
    int medianKeyIndex;
    Value medianKey;
    returnKey->dt = rKey->dt;

    /*
     * For non-leaf overflow, we will split the node in two nodes and then
     * we will put the, median key in its parent.
     * Thus,
     * For n = even (8), num keys to be inserted in non-leaf in case of overflow are 9
     * thus, medianKey will be 5 and index will be 4.
     * For n = odd (5) num keys to be inserted in non-leaf are 6,
     * First three in old, next one (median) in its parent and rest two in new node.
     * thus medianKey will be 4 and index will be 3.
     */
    int numKeysOldNode, numKeysNewNode;
    if((tree->mgmtData->maxNumKeys % 2) == 0) {
        // If n is even.
        medianKeyIndex = (tree->mgmtData->maxNumKeys / 2);
        numKeysOldNode = medianKeyIndex;    // No. of keys in old node after split (if n=6, then 3)
        numKeysNewNode = numKeysOldNode;    // No. of keys in new node after split (if n=6, then 3)
    } else {
        medianKeyIndex = (tree->mgmtData->maxNumKeys / 2) + 1;
        numKeysOldNode = medianKeyIndex;    // No. of keys in old node after split (if n=5, then 3)
        numKeysNewNode = numKeysOldNode-1;  // No. of keys in new node after split (if n=5, then 2)
    }

    bool newKeyPlaced = false;
    int oldIndex;
    newKeyPlaced = updateNewNode(tree, rKey, rid, nodePage, newNodePage, numKeysNewNode, newKeyPlaced, &oldIndex, true);

    /* Now, we have updated the new node and data in it. Now, we have reached
     * to the point of median key index. So, lets find the which is the median
     * key and let's use it to put in the parent.
     */
    returnKey->dt = rKey->dt;
    switch(rKey->dt) {
        case DT_INT:
            if((nodePage->bTreeNode->keys[oldIndex].v.intV > rKey->v.intV) || (newKeyPlaced)) {
                returnKey->v.intV = nodePage->bTreeNode->keys[oldIndex].v.intV;
                newNodePage->bTreeNode->recordsPtr[0].page = nodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                newNodePage->bTreeNode->recordsPtr[0].slot = nodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
                nodePage->bTreeNode->recordsPtr[oldIndex+1].page = -1;
                nodePage->bTreeNode->recordsPtr[oldIndex+1].slot = -1;
                oldIndex--;
            } else {
                returnKey->v.intV = rKey->v.intV;
                newNodePage->bTreeNode->recordsPtr[0].page = rid.page;
                newNodePage->bTreeNode->recordsPtr[0].slot = rid.slot;
                newKeyPlaced = true;
            }
            //returnKey->v.intV = newNodePage->bTreeNode->keys[0].v.intV;
            break;
        case DT_FLOAT:
            if((nodePage->bTreeNode->keys[oldIndex].v.floatV > rKey->v.floatV) || (newKeyPlaced)) {
                returnKey->v.floatV = nodePage->bTreeNode->keys[oldIndex].v.floatV;
                oldIndex--;
            } else {
                returnKey->v.intV = rKey->v.floatV;
                newKeyPlaced = true;
            }
            break;
        case DT_STRING:
            if((strcmp(nodePage->bTreeNode->keys[oldIndex].v.stringV, rKey->v.stringV) > 0) || (newKeyPlaced)) {
                returnKey->v.stringV = (char *) malloc(strlen(nodePage->bTreeNode->keys[oldIndex].v.stringV) + 1);
                strcpy(returnKey->v.stringV, nodePage->bTreeNode->keys[oldIndex].v.stringV);
                oldIndex--;
            } else {
                returnKey->v.stringV = (char *) malloc(strlen(rKey->v.stringV) + 1);
                strcpy(returnKey->v.stringV, rKey->v.stringV);
                newKeyPlaced = true;
            }
            break;
        case DT_BOOL:
            // No indexing for bool => Useless indexing.
            return RC_BOOL_INDEX_NOT_SUPPORTED;
        default:
            return RC_WRONG_DATATYPE;
    }

    /* Now, we have updated new node data, old nodes prev data and the median key,
     * it's time to update rest of the old node.
     */
    newKeyPlaced = updateOldNode(tree, rKey, rid, nodePage, numKeysOldNode, newKeyPlaced, oldIndex, true);

printf("\n ---------- New Node Data ----------");
printNodeData(newNodePage);
printf("\n ---------- Old Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
printf("\n Return Key: %d", returnKey->v.intV);

    // Update information of this page in the page file.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->pageNum = newNodePage->pageNum;
    if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK) 
        return RC_READ_BTREE_INFO_FAILED;
//    ph->data = (char *) malloc(PAGE_SIZE);

    // Update BTree new node information in the buffer manager.
    RC retCode;
    retCode = updateBTreeNodePageInBufManager(tree, newNodePage, ph);
    if(retCode != RC_OK)
        return retCode;
    
    // Free BM_PageHandle
    retCode = freeBMPageHandle(ph);
    if(retCode != RC_OK)
        return retCode;

    freeBTreeNodePage(newNodePage);
    return RC_OK;
}

RC handleLeafOverFlow(BTreeHandle *tree, Value *key, RID rid, BTreeNodePage *nodePage, Value *returnKey, int *returnNewPageNum) {
    // Create new node to handle leaf overflow.
//    BTreeNodePage *newNodePage = createNodePage(tree->mgmtData->bm->mgmtData->fh->totalNumPages, key->dt, tree->mgmtData->maxNumKeys);
    int newNodePageNum = tree->mgmtData->treeNumNodes + 1;
    BTreeNodePage *newNodePage = createNodePage(newNodePageNum, key->dt, tree->mgmtData->maxNumKeys);

    // Update new node information.    
    newNodePage->isLeaf = true;
    *returnNewPageNum = newNodePage->pageNum;

    // We must split old node keys between new node and old node.
    int numKeysOldNode, numKeysNewNode;
    if((tree->mgmtData->maxNumKeys + 1) % 2 != 0) {
        // If num of keys in the node are even.
        numKeysOldNode = (tree->mgmtData->maxNumKeys/2) + 1;
        numKeysNewNode = numKeysOldNode - 1;
    } else {
        // If num of keys in the node are odd.
        numKeysOldNode = ((tree->mgmtData->maxNumKeys + 1)/2);
        numKeysNewNode = numKeysOldNode;
    }

    // Find index where we can put this key in old node.
    int newKeyIndex = -1;
    RC retCode;
    switch (key->dt) {
        case DT_INT:
            // Int value comparison.
            retCode = findKeyIntNode(nodePage, key, &newKeyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }
            break;
        case DT_FLOAT:
            // Float value comparison.
            retCode = findKeyFloattNode(nodePage, key, &newKeyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }
            break;
        case DT_STRING:
            // String Value comparison.
            retCode = findKeyStringNode(nodePage, key, &newKeyIndex);
            if(retCode != RC_OK) {
                return retCode;
            }
            break;
    }


    // Start updating new node first, from the last index.
    bool newKeyPlaced = false;
    int oldIndex;
    newKeyPlaced = updateNewNode(tree, key, rid, nodePage, newNodePage, numKeysNewNode, newKeyPlaced, &oldIndex, false);

    /* Now we have updated the new node. So let's update the key that
     * we will return to the parent, so that parent can take action based
     * on that 
     */
    returnKey->dt = key->dt;
    switch(key->dt) {
        case DT_INT:
            returnKey->v.intV = newNodePage->bTreeNode->keys[0].v.intV;
            break;
        case DT_FLOAT:
            returnKey->v.floatV = newNodePage->bTreeNode->keys[0].v.floatV;
            break;
        case DT_STRING:
            returnKey->v.stringV = (char *) malloc(strlen(newNodePage->bTreeNode->keys[0].v.stringV) + 1);
            strcpy(returnKey->v.stringV, newNodePage->bTreeNode->keys[0].v.stringV);
            break;
        case DT_BOOL:
            // No indexing for bool => Useless indexing.
            return RC_BOOL_INDEX_NOT_SUPPORTED;
        default:
            return RC_WRONG_DATATYPE;
    }

    /* Now let's update the old node.
       If new key is already placed in the new node, then no need to take
       any action. */
    if(!newKeyPlaced)
        newKeyPlaced = updateOldNode(tree, key, rid, nodePage, numKeysOldNode, newKeyPlaced, oldIndex, false);

    // Update sibling pointers.
    newNodePage->bTreeNode->recordsPtr[tree->mgmtData->maxNumKeys].page = 
                                        nodePage->bTreeNode->recordsPtr[tree->mgmtData->maxNumKeys].page;
    newNodePage->bTreeNode->recordsPtr[tree->mgmtData->maxNumKeys].slot = 
                                        nodePage->bTreeNode->recordsPtr[tree->mgmtData->maxNumKeys].slot;

    nodePage->bTreeNode->recordsPtr[tree->mgmtData->maxNumKeys].page = newNodePage->pageNum;
    nodePage->bTreeNode->recordsPtr[tree->mgmtData->maxNumKeys].slot = -1;


printf("\n ---------- New Node Data ----------");
printNodeData(newNodePage);
printf("\n ---------- Old Node Data ----------");
printNodeData(nodePage);
printTreeInfor(tree);
printf("\n Return Key: %d", returnKey->v.intV);

    // Update information of this page in the page file.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    ph->pageNum = newNodePage->pageNum;
    if (pinPage(tree->mgmtData->bm, ph, ph->pageNum) != RC_OK) 
        return RC_READ_BTREE_INFO_FAILED;
//    ph->data = (char *) malloc(PAGE_SIZE);

    // Update BTree new node information in the buffer manager.
    retCode = updateBTreeNodePageInBufManager(tree, newNodePage, ph);
    if(retCode != RC_OK)
        return retCode;

    // Free BM_PageHandle
    retCode = freeBMPageHandle(ph);
    if(retCode != RC_OK)
        return retCode;

    freeBTreeNodePage(newNodePage);
    return RC_OK;
}



// Following set of functions are used to find the index of the key we are looking for.
int findKeyIntLeafNode(Value *nodeKeys, Value *key, int n) {
    int i=0;
    for(i=0; i < n; i++) {
        // Parse through all keys to find the key.
        if(nodeKeys[i].v.intV == key->v.intV)
            return i;           // Key found.
    }
    return -1;          // Key not found.
}

int findKeyFloatLeafNode(Value *nodeKeys, Value *key, int n) {
    int i=0;
    for(i=0; i < n; i++) {
        // Parse through all keys to find the key.
        if(nodeKeys[i].v.floatV == key->v.floatV)
            return i;           // Key found.
    }
    return -1;          // Key not found.
}

int findKeyStringLeafNode(Value *nodeKeys, Value *key, int n) {
    int i=0;
    for(i=0; i < n; i++) {
        // Parse through all keys to find the key.
        if(strcmp(nodeKeys[i].v.stringV, key->v.stringV) == 0)
            return i;           // Key found.
    }
    return -1;          // Key not found.
}


// Following set of functions are used to compare the key that we want to
// insert, delete or find and traverse the BTree (Find the appropriate
// child node.
int findChildPageIntKey(Value *nodeKeys, Value *key, int n) {
    // Check where our key fits.
    int i=0;
    for(i=0; i < n; i++) {
        if(nodeKeys[i].v.intV > key->v.intV)
            return i;
        if(nodeKeys[i].v.intV == key->v.intV)
            return (i+1);
    }
    return i;
}

int findChildPageFloatKey(Value *nodeKeys, Value *key, int n) {
    // Check where our key fits.
    int i=0;
    for(i=0; i < n; i++) {
        if(nodeKeys[i].v.floatV >  key->v.intV)
            return i;
        if(nodeKeys[i].v.floatV == key->v.intV)
            return (i+1);
    }
    return i;
}

int findChildPageStringKey(Value *nodeKeys, Value *key, int n) {
    // Check where our key fits.
    int i=0;
    for(i=0; i < n; i++) {
        if(strcmp(nodeKeys[i].v.stringV, key->v.stringV) > 0)
            return i;
        if(strcmp(nodeKeys[i].v.stringV, key->v.stringV) == 0)
            return (i+1);
    }
    return i;
}


// Following set of functions are used to find the index of the key we are
// looking for in a node.
RC findKeyIntNode(BTreeNodePage *nodePage, Value *key, int *keyIndex) {
    int i=0;
    for (i=0; i < nodePage->numKeys; i++) {
        if(nodePage->bTreeNode->keys[i].v.intV == key->v.intV)
            return RC_IM_KEY_ALREADY_EXISTS;
        else if(nodePage->bTreeNode->keys[i].v.intV > key->v.intV) {
            *keyIndex = i;
            return RC_OK;
        }
    }
    *keyIndex = i;
    return RC_OK;
}

RC findKeyFloattNode(BTreeNodePage *nodePage, Value *key, int *keyIndex) {
    // Check where our key fits.
    int i=0;
    for (i=0; i < nodePage->numKeys; i++) {
        if(nodePage->bTreeNode->keys[i].v.floatV == key->v.floatV)
            return RC_IM_KEY_ALREADY_EXISTS;
        else if (nodePage->bTreeNode->keys[i].v.floatV > key->v.floatV) {
            *keyIndex = i;
            return RC_OK;
        }
    }
    *keyIndex = i;
    return RC_OK;
}

RC findKeyStringNode(BTreeNodePage *nodePage, Value *key, int *keyIndex) {
    // Check where our key fits.
    int i=0;
    for (i=0; i < nodePage->numKeys; i++) {
        if(nodePage->bTreeNode->keys[i].v.stringV == key->v.stringV)
            return RC_IM_KEY_ALREADY_EXISTS;
        else if(strcmp(nodePage->bTreeNode->keys[i].v.stringV, key->v.stringV) > 0) {
            *keyIndex = i;
            return RC_OK;
        }
    }
    *keyIndex = i;
    return RC_OK;
}


// Following set of functions are used to put key in the node page array.
RC putIntKey(BTreeNodePage *nodePage, Value *key, RID rid, int keyIndex) {
    int index = nodePage->numKeys;
    if(keyIndex == index) {
        // Insertion at the last index, as this is being the largest key
        // in the given node.
        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.intV = key->v.intV;

        if(!nodePage->isLeaf)
            index++;
        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        return RC_OK;
    } else {
        // Move other nodes one place up the array.
        for(index; index > keyIndex; index--) {
            // Move key
            nodePage->bTreeNode->keys[index].dt = key->dt;
            nodePage->bTreeNode->keys[index].v.intV = nodePage->bTreeNode->keys[index-1].v.intV;
            // Move respective RID
            int ridIndex = index;
            if(!nodePage->isLeaf)
                ridIndex++;
            nodePage->bTreeNode->recordsPtr[ridIndex].page = nodePage->bTreeNode->recordsPtr[ridIndex-1].page;
            nodePage->bTreeNode->recordsPtr[ridIndex].slot = nodePage->bTreeNode->recordsPtr[ridIndex-1].slot;
        }

        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.intV = key->v.intV;

        if(!nodePage->isLeaf)
            index++;
        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        return RC_OK;
    }
}

RC putFloatKey(BTreeNodePage *nodePage, Value *key, RID rid, int keyIndex) {
    int index = nodePage->numKeys;
    if(keyIndex == index) {
        // Insertion at the last index, as this is being the largest key
        // in the given node.
        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;

        if(!nodePage->isLeaf)
            index++;
        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        return RC_OK;
    } else {
        // Move other nodes one place up the array.
        for(index; index > keyIndex; index--) {
            // Move key
            nodePage->bTreeNode->keys[index].dt = key->dt;
            nodePage->bTreeNode->keys[index].v.floatV = nodePage->bTreeNode->keys[index-1].v.floatV;
            // Move respective RID
            int ridIndex = index;
            if(!nodePage->isLeaf)
                ridIndex++;
            nodePage->bTreeNode->recordsPtr[ridIndex].page = nodePage->bTreeNode->recordsPtr[ridIndex-1].page;
            nodePage->bTreeNode->recordsPtr[ridIndex].slot = nodePage->bTreeNode->recordsPtr[ridIndex-1].slot;
        }

        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;

        if(!nodePage->isLeaf)
            index++;
        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        return RC_OK;
    }
}

RC putStringKey(BTreeNodePage *nodePage, Value *key, RID rid, int keyIndex) {
    int index = nodePage->numKeys;
    if(keyIndex == index) {
        // Insertion at the last index, as this is being the largest key
        // in the given node.
        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV)+1);
        strcpy(nodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);

        if(!nodePage->isLeaf)
            index++;
        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        return RC_OK;
    } else {
        // Move other nodes one place up the array.
        for(index; index > keyIndex; index--) {
            // Move key
            nodePage->bTreeNode->keys[index].dt = key->dt;
            if(nodePage->bTreeNode->keys[index].v.stringV)
                free(nodePage->bTreeNode->keys[index].v.stringV);
            nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(nodePage->bTreeNode->keys[index-1].v.stringV)+1);
            strcpy(nodePage->bTreeNode->keys[index].v.stringV, nodePage->bTreeNode->keys[index-1].v.stringV);

            // Move respective RID
            int ridIndex = index;
            if(!nodePage->isLeaf)
                ridIndex++;
            nodePage->bTreeNode->recordsPtr[ridIndex].page = nodePage->bTreeNode->recordsPtr[ridIndex-1].page;
            nodePage->bTreeNode->recordsPtr[ridIndex].slot = nodePage->bTreeNode->recordsPtr[ridIndex-1].slot;
        }

        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV)+1);
        strcpy(nodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);

        if(!nodePage->isLeaf)
            index++;
        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        return RC_OK;
    }
}

/**
 * Update management information page of BTree in the page file.
 */
RC updateBTreeManagementPage(BTreeHandle *tree) {
    // Initial page data is ready. Put it into the page 0.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    pinPage(tree->mgmtData->bm, ph, 0);

    // Copy required data on the page.
    size_t copyFromByte = 0;
    memmove(ph->data + copyFromByte, &tree->keyType, sizeof(DataType));
    copyFromByte += sizeof(DataType);
    memmove(ph->data + copyFromByte, &tree->mgmtData->rootNodePageNum, sizeof(int));
    copyFromByte += sizeof(int);
    memmove(ph->data + copyFromByte, &tree->mgmtData->maxNumKeys, sizeof(int));
    copyFromByte += sizeof(int);
    memmove(ph->data + copyFromByte, &tree->mgmtData->treeNumNodes, sizeof(int));
    copyFromByte += sizeof(int);
    memmove(ph->data + copyFromByte, &tree->mgmtData->treeNumEntries, sizeof(int));

    // Mark page 0 as dirty page and write page with table info.
    if (markDirty(tree->mgmtData->bm, ph) != RC_OK)
        return RC_CLDNT_WRITE_PAGE;

    // Unpin page 0.
    if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    freeBMPageHandle(ph);
    return RC_OK;
}

/**
 * Free memory allocated for BM_PageHandle
 */
RC freeBMPageHandle(BM_PageHandle *ph) {
    if(ph->data)
        free(ph->data);
    if(ph)
        free(ph);
    return RC_OK;
}

/**
 * Check whether node is full or not.
 */
bool checkNodeFull(int numKeys, int maxNumKeys) {
    if(numKeys == maxNumKeys)
        return true;		// Node is full.
    return false;        // Node is not full.
}

/**
 * Function BTree node operations.
 */
// Create new node and initialize some data in it.
BTreeNodePage *createNodePage(int pageNum, DataType keyType, int n) {

    // Allocate memory for BTree node page.
    BTreeNodePage *nodePage = (BTreeNodePage *) malloc (sizeof(BTreeNodePage));
    nodePage->pageNum = pageNum;
    nodePage->keyType = keyType;
    nodePage->numKeys = 0;        // Initially 0 keys.
    nodePage->maxNumKeys = n;
    nodePage->numPointers = 0;    // Initially 0 pointers.
    nodePage->isLeaf = false;
    nodePage->isRoot = false;

    // Allocate memory for tree node structure.
    nodePage->bTreeNode = (Node *) malloc(sizeof(Node));

    // Allocate memory for keys.
    nodePage->bTreeNode->keys = (Value *) malloc(sizeof(Value) * n);

    // Allocate memory for pointers in the tree.
    nodePage->bTreeNode->recordsPtr = (RID *) malloc(sizeof(RID) * (n+1));
    int i;
    for(i=0; i <= n; i++) {
        nodePage->bTreeNode->recordsPtr[i].page = -1;
        nodePage->bTreeNode->recordsPtr[i].slot = -1;
    }

    return nodePage;
}

// Free memory allocated for nodePage
RC freeBTreeNodePage(BTreeNodePage *nodePage) {

    if(nodePage->bTreeNode->recordsPtr)
        free(nodePage->bTreeNode->recordsPtr);    // Free memory allocated for record pointer.
    if(nodePage->bTreeNode->keys)
        free(nodePage->bTreeNode->keys);        // Free memory allocated for record keys.
    if(nodePage->bTreeNode)
        free(nodePage->bTreeNode);              // Free memory allocated for B-Tree node.
    if(nodePage)
        free(nodePage);                 // Free memory allocated for B-Tree Node Page.

    return RC_OK;
}

// Update node information in the memory to help and put it in the page file.
RC updateBTreeNodePageInBufManager(BTreeHandle *tree, BTreeNodePage *nodePage, BM_PageHandle *ph) {
    // Serialize and write new node data in case of root node (initial case)
    // or old node in other cases.
    if(serializeBTreeNodePage(nodePage, ph->data) != RC_OK)
        return RC_SERIALIZATION_FAILED;

    // make page dirty
    RC retCode = markDirty(tree->mgmtData->bm, ph);
    if (retCode != RC_OK)
        return retCode;

    // Unpin page.
    if (unpinPage(tree->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    return RC_OK;
}



/*
 * Following functions are used to update data in new node and old node,
 * whenever any node is split.
 */

bool updateOldNode(BTreeHandle *tree, Value *key, RID rid,
                   BTreeNodePage *nodePage, int nodeMaxElements, bool newKeyPlaced, int oldIndex, bool fromNonLeaf) {

    int index = nodeMaxElements-1;
    bool copyRid = false;
    for(index; (index >= 0) && (oldIndex >= 0); index--) {
        switch (key->dt) {
            case DT_INT:
                if((nodePage->bTreeNode->keys[oldIndex].v.intV > key->v.intV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.intV = nodePage->bTreeNode->keys[oldIndex].v.intV;
                    if(fromNonLeaf) {
                        nodePage->bTreeNode->recordsPtr[index+1].page = nodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                        nodePage->bTreeNode->recordsPtr[index+1].slot = nodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
                    } else {
                        nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex].page;
                        nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    }
                    copyRid = false;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.intV = key->v.intV;
                    if(fromNonLeaf) {
                        nodePage->bTreeNode->recordsPtr[index+1].page = rid.page;
                        nodePage->bTreeNode->recordsPtr[index+1].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    } else {
                        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
                        nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    }
                    copyRid = true;
                    newKeyPlaced = true;
                }
                break;
            case DT_FLOAT:
                if((nodePage->bTreeNode->keys[oldIndex].v.floatV > key->v.floatV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.floatV = nodePage->bTreeNode->keys[oldIndex].v.floatV;
//                    nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex].page;
  //                  nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;
    //                nodePage->bTreeNode->recordsPtr[index].page = rid.page;
      //              nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_STRING:
                if((strcmp(nodePage->bTreeNode->keys[oldIndex].v.stringV, key->v.stringV) > 0) || (newKeyPlaced)) {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.stringV =
                              (char *) malloc(strlen(nodePage->bTreeNode->keys[oldIndex].v.stringV) + 1);
                    strcpy(nodePage->bTreeNode->keys[index].v.stringV, nodePage->bTreeNode->keys[oldIndex].v.stringV);
        //            nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex].page;
          //          nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV) + 1);
                    strcpy(nodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);
            //        nodePage->bTreeNode->recordsPtr[index].page = rid.page;
              //      nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_BOOL:
                // No indexing for bool => Useless indexing.
                return RC_BOOL_INDEX_NOT_SUPPORTED;
            default:
                return RC_WRONG_DATATYPE;
        }

/*        if(!(newKeyPlaced)) {
            if(fromNonLeaf) {
                nodePage->bTreeNode->recordsPtr[index+1].page = nodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                nodePage->bTreeNode->recordsPtr[index+1].slot = nodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
            } else {
                nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
            }
        } else {
            if(fromNonLeaf) {
                nodePage->bTreeNode->recordsPtr[index+1].page = rid.page;
                nodePage->bTreeNode->recordsPtr[index+1].slot = rid.slot;
            } else {
                nodePage->bTreeNode->recordsPtr[index].page = rid.page;
                nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
            }
            break;
        }*/
    }

    if((!newKeyPlaced) && (oldIndex == -1) && (index == 0)) {
        switch(key->dt) {
            case DT_INT:
                // Update data in the new node.
                nodePage->bTreeNode->keys[index].v.intV = key->v.intV;
            case DT_FLOAT:
                // Update data in the new node.
                nodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;
                break;
            case DT_STRING:
                nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV) + 1);
                strcpy(nodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);
                break;
        }

        if(!fromNonLeaf) {
            nodePage->bTreeNode->recordsPtr[index].page = rid.page;
            nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
        } else {
            nodePage->bTreeNode->recordsPtr[index+1].page = rid.page;
            nodePage->bTreeNode->recordsPtr[index+1].slot = rid.slot;
        }
        newKeyPlaced = true;
    }
    nodePage->numKeys = nodeMaxElements;
    nodePage->numPointers = nodeMaxElements + 1;

    return newKeyPlaced;
}


bool updateNewNode(BTreeHandle *tree, Value *key, RID rid, 
                   BTreeNodePage *oldNodePage, BTreeNodePage *newNodePage,
                   int newNodeMaxElements, bool newKeyPlaced, int *lastIndexVisited, bool fromNonLeaf) {

    int index = newNodeMaxElements-1;        // Index for new node.
    int oldIndex = tree->mgmtData->maxNumKeys-1;        // Indexx for old node.

    for(index; index >= 0; index--) {
        newNodePage->bTreeNode->keys[index].dt = key->dt;    // Update key datatype.
        switch (key->dt) {
            case DT_INT:
                if((oldNodePage->bTreeNode->keys[oldIndex].v.intV > key->v.intV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.intV = oldNodePage->bTreeNode->keys[oldIndex].v.intV;
                    if(fromNonLeaf) {
                        newNodePage->bTreeNode->recordsPtr[index+1].page = oldNodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                        newNodePage->bTreeNode->recordsPtr[index+1].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex+1].slot;
                        oldNodePage->bTreeNode->recordsPtr[oldIndex+1].page = -1;
                        oldNodePage->bTreeNode->recordsPtr[oldIndex+1].slot = -1;
                    } else {
                        newNodePage->bTreeNode->recordsPtr[index].page = oldNodePage->bTreeNode->recordsPtr[oldIndex].page;
                        newNodePage->bTreeNode->recordsPtr[index].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex].slot;
                        oldNodePage->bTreeNode->recordsPtr[oldIndex].page = -1;
                        oldNodePage->bTreeNode->recordsPtr[oldIndex].slot = -1;
                    }
                    // Update the respective data in the old node.
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.intV = key->v.intV;
                    if(fromNonLeaf) {
                        newNodePage->bTreeNode->recordsPtr[index+1].page = rid.page;
                        newNodePage->bTreeNode->recordsPtr[index+1].slot = rid.slot;
                    } else {
                        newNodePage->bTreeNode->recordsPtr[index].page = rid.page;
                        newNodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    }
                    newKeyPlaced = true;
                }
                break;
            case DT_FLOAT:
                if((oldNodePage->bTreeNode->keys[oldIndex].v.floatV > key->v.floatV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.floatV = oldNodePage->bTreeNode->keys[oldIndex].v.floatV;
                    newNodePage->bTreeNode->recordsPtr[index+1].page = oldNodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                    newNodePage->bTreeNode->recordsPtr[index+1].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex+1].slot;

                    // Update the respective data in the old node.
                    oldNodePage->bTreeNode->recordsPtr[oldIndex+1].page = -1;
                    oldNodePage->bTreeNode->recordsPtr[oldIndex+1].slot = -1;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;
                    newNodePage->bTreeNode->recordsPtr[index+1].page = rid.page;
                    newNodePage->bTreeNode->recordsPtr[index+1].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_STRING:
                if((strcmp(oldNodePage->bTreeNode->keys[oldIndex].v.stringV, key->v.stringV) > 0) || (newKeyPlaced)) {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.stringV =
                                 (char *) malloc(strlen(oldNodePage->bTreeNode->keys[oldIndex].v.stringV) + 1);
                    strcpy(newNodePage->bTreeNode->keys[index].v.stringV, oldNodePage->bTreeNode->keys[oldIndex].v.stringV);
                    newNodePage->bTreeNode->recordsPtr[index+1].page = oldNodePage->bTreeNode->recordsPtr[oldIndex+1].page;
                    newNodePage->bTreeNode->recordsPtr[index+1].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex+1].slot;

                    // Update the respective data in the old node.
                    oldNodePage->bTreeNode->recordsPtr[oldIndex+1].page = -1;
                    oldNodePage->bTreeNode->recordsPtr[oldIndex+1].slot = -1;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV) + 1);
                    strcpy(newNodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);
                    newNodePage->bTreeNode->recordsPtr[index+1].page = rid.page;
                    newNodePage->bTreeNode->recordsPtr[index+1].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_BOOL:
                // No indexing for bool => Useless indexing.
                return RC_BOOL_INDEX_NOT_SUPPORTED;
            default:
                return RC_WRONG_DATATYPE;
        }
    }

    newNodePage->numKeys = newNodeMaxElements;
    newNodePage->numPointers = newNodeMaxElements + 1;

    *lastIndexVisited = oldIndex;
    return newKeyPlaced;
}

void printNodeData(BTreeNodePage *nodePage) {
    printf("\n\n ------- Node Page: %d --------", nodePage->pageNum);
//    printf("\n -- Mgmt Info --");
  //  printf("\n nodePage->numKeys: %d", nodePage->numKeys);
    //printf("\n nodePage->numPointers: %d", nodePage->numPointers);

    int i=0;
    printf("\n Node Keys -->");
    for(i; i < nodePage->maxNumKeys; i++)
        printf("\n (%d => %d)", i, nodePage->bTreeNode->keys[i].v.intV);

    printf("\n Node Key Pointers-->");
    for(i=0; i <= nodePage->maxNumKeys; i++)
        printf("\n [%d => (%d, %d)]", i, nodePage->bTreeNode->recordsPtr[i].page, nodePage->bTreeNode->recordsPtr[i].slot);
}

void printTreeInfor(BTreeHandle *tree) {
    printf("\n --- Tree Info ---");
    printf("\n tree)->mgmtData->rootNodePageNum: %d", tree->mgmtData->rootNodePageNum);
    printf("\n tree)->mgmtData->treeNumNodes: %d", tree->mgmtData->treeNumNodes);
    printf("\n tree)->mgmtData->treeNumEntries: %d", tree->mgmtData->treeNumEntries);
}

