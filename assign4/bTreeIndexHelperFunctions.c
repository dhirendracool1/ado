#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"
#include "bTreeIndexHelperFunctions.h"

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

int findKeyFloattLeafNode(Value *nodeKeys, Value *key, int n) {
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
    }
    return i;
}

int findChildPageFloatKey(Value *nodeKeys, Value *key, int n) {
    // Check where our key fits.
    int i=0;
    for(i=0; i < n; i++) {
        if(nodeKeys[i].v.floatV >  key->v.intV)
            return i;
    }
    return i;
}

int findChildPageStringtKey(Value *nodeKeys, Value *key, int n) {
    // Check where our key fits.
    int i=0;
    for(i=0; i < n; i++) {
        if(strcmp(nodeKeys[i].v.stringV, key->v.stringV) > 0)
            return i;
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
        else if(nodePage->bTreeNode->keys[i].v.intV >= key->v.intV) {
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
        else if (nodePage->bTreeNode->keys[i].v.floatV >= key->v.floatV) {
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
        else if(strcmp(nodePage->bTreeNode->keys[i].v.stringV, key->v.stringV) >= 0) {
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
            nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[index-1].page;
            nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[index-1].slot;
        }

        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.intV = key->v.intV;

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
            nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[index-1].page;
            nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[index-1].slot;
        }

        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;

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
            nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[index-1].page;
            nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[index-1].slot;
        }

        nodePage->bTreeNode->keys[index].dt = key->dt;
        nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV)+1);
        strcpy(nodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);

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
    BTreeNodePage *nodePage = (BTreeNodePage *) malloc (sizeof(BTreeNodePage *));
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
    nodePage->bTreeNode->recordsPtr = (RID *) malloc(sizeof(RID) * n);
    int i;
    for(i=0; i < n; i++) {
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
                   BTreeNodePage *nodePage, int nodeMaxElements, bool newKeyPlaced, int oldIndex) {

    int index = nodeMaxElements-1;
    for(index; index >= 0; index--) {
        switch (key->dt) {
            case DT_INT:
                if((nodePage->bTreeNode->keys[oldIndex].v.intV > key->v.intV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.intV = nodePage->bTreeNode->keys[oldIndex].v.intV;
                    nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex].page;
                    nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.intV = key->v.intV;
                    nodePage->bTreeNode->recordsPtr[index].page = rid.page;
                    nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_FLOAT:
                if((nodePage->bTreeNode->keys[oldIndex].v.floatV > key->v.floatV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.floatV = nodePage->bTreeNode->keys[oldIndex].v.floatV;
                    nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex].page;
                    nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;
                    nodePage->bTreeNode->recordsPtr[index].page = rid.page;
                    nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_STRING:
                if((strcmp(nodePage->bTreeNode->keys[oldIndex].v.stringV, key->v.stringV) > 0) || (newKeyPlaced)) {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.stringV =
                              (char *) malloc(strlen(nodePage->bTreeNode->keys[oldIndex].v.stringV) + 1);
                    strcpy(nodePage->bTreeNode->keys[index].v.stringV, nodePage->bTreeNode->keys[oldIndex].v.stringV);
                    nodePage->bTreeNode->recordsPtr[index].page = nodePage->bTreeNode->recordsPtr[oldIndex].page;
                    nodePage->bTreeNode->recordsPtr[index].slot = nodePage->bTreeNode->recordsPtr[oldIndex].slot;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    nodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV) + 1);
                    strcpy(nodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);
                    nodePage->bTreeNode->recordsPtr[index].page = rid.page;
                    nodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
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

    nodePage->numKeys = nodeMaxElements;
    nodePage->numPointers = nodeMaxElements + 1;

    return newKeyPlaced;
}


bool updateNewNode(BTreeHandle *tree, Value *key, RID rid, 
                   BTreeNodePage *oldNodePage, BTreeNodePage *newNodePage,
                   int newNodeMaxElements, bool newKeyPlaced, int *lastIndexVisited) {

    int index = newNodeMaxElements-1;        // Index for new node.
    int oldIndex = tree->mgmtData->maxNumKeys-1;        // Indexx for old node.

    for(index; index >= 0; index--) {
        newNodePage->bTreeNode->keys[index].dt = key->dt;    // Update key datatype.
        switch (key->dt) {
            case DT_INT:
                if((oldNodePage->bTreeNode->keys[oldIndex].v.intV > key->v.intV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.intV = oldNodePage->bTreeNode->keys[oldIndex].v.intV;
                    newNodePage->bTreeNode->recordsPtr[index].page = oldNodePage->bTreeNode->recordsPtr[oldIndex].page;
                    newNodePage->bTreeNode->recordsPtr[index].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex].slot;

                    // Update the respective data in the old node.
                    oldNodePage->bTreeNode->recordsPtr[oldIndex].page = -1;
                    oldNodePage->bTreeNode->recordsPtr[oldIndex].slot = -1;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.intV = key->v.intV;
                    newNodePage->bTreeNode->recordsPtr[index].page = rid.page;
                    newNodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_FLOAT:
                if((oldNodePage->bTreeNode->keys[oldIndex].v.floatV > key->v.floatV) || (newKeyPlaced)) {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.floatV = oldNodePage->bTreeNode->keys[oldIndex].v.floatV;
                    newNodePage->bTreeNode->recordsPtr[index].page = oldNodePage->bTreeNode->recordsPtr[oldIndex].page;
                    newNodePage->bTreeNode->recordsPtr[index].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex].slot;

                    // Update the respective data in the old node.
                    oldNodePage->bTreeNode->recordsPtr[oldIndex].page = -1;
                    oldNodePage->bTreeNode->recordsPtr[oldIndex].slot = -1;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.floatV = key->v.floatV;
                    newNodePage->bTreeNode->recordsPtr[index].page = rid.page;
                    newNodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
                    newKeyPlaced = true;
                }
                break;
            case DT_STRING:
                if((strcmp(oldNodePage->bTreeNode->keys[oldIndex].v.stringV, key->v.stringV) > 0) || (newKeyPlaced)) {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.stringV =
                                 (char *) malloc(strlen(oldNodePage->bTreeNode->keys[oldIndex].v.stringV) + 1);
                    strcpy(newNodePage->bTreeNode->keys[index].v.stringV, oldNodePage->bTreeNode->keys[oldIndex].v.stringV);
                    newNodePage->bTreeNode->recordsPtr[index].page = oldNodePage->bTreeNode->recordsPtr[oldIndex].page;
                    newNodePage->bTreeNode->recordsPtr[index].slot = oldNodePage->bTreeNode->recordsPtr[oldIndex].slot;

                    // Update the respective data in the old node.
                    oldNodePage->bTreeNode->recordsPtr[oldIndex].page = -1;
                    oldNodePage->bTreeNode->recordsPtr[oldIndex].slot = -1;
                    oldIndex--;
                } else {
                    // Update data in the new node.
                    newNodePage->bTreeNode->keys[index].v.stringV = (char *) malloc(strlen(key->v.stringV) + 1);
                    strcpy(newNodePage->bTreeNode->keys[index].v.stringV, key->v.stringV);
                    newNodePage->bTreeNode->recordsPtr[index].page = rid.page;
                    newNodePage->bTreeNode->recordsPtr[index].slot = rid.slot;
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
