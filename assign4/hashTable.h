
#include "buffer_mgr.h"

//void initializePageInfo(PageInfo pageInfo, int pageNumber); // set key for the HashTable
int getKey(PageInfo *pageInfo);  // get the key for the Page frame
int getFixCount(PageInfo *pageInfo);  // get the fix count value of the page
bool getDirtyFlag(PageInfo *pageInfo);  // get the dirty flag value of the page
void setFixCount(BM_HashTable *hTable, int index, int value);  // set the fix count value of the page
void setDirtyFlag(BM_HashTable *hTable, int index, bool value);  // set the dirty flag value of the page

//void initializeHashTable(int size, BM_HashTable *hashTable);  // to initialize the hash table
int hashFunc1(int key, int size);  // First hash function
int hashFunc2(int key);  // Second hash function
extern int findPageFrame(int pageNum, BM_HashTable *hTable);  // insert
extern int find(int key, BM_HashTable *hTable);  // find
PageInfo delete(int key, BM_HashTable *hTable);  // delete



