
#include<stdio.h>
#include<stdlib.h>
#include "hashTable.h"
#include "buffer_mgr.h"

// set values for PageInfo of a given page
/*void initializePageInfo(PageInfo pageInfo, int pageNumber){
	pageInfo.pageHandle.pageNum = pageNumber;
	pageInfo.fixCount = 0;
	pageInfo.dirtyFlag = false;
}*/

// return pageNumber of the page frame
int getKey(PageInfo *pageInfo) {
	return pageInfo->pageHandle->pageNum;
}

int getFixCount(PageInfo *pageInfo) {
	return pageInfo->fixCount;
}

bool getDirtyFlag(PageInfo *pageInfo) {
	return pageInfo->dirtyFlag;
}

void setFixCount(BM_HashTable *hTable, int index, int value) {
	hTable->hashArray[index]->fixCount = value;
}

void setDirtyFlag(BM_HashTable *hTable, int index, bool value) {
	hTable->hashArray[index]->dirtyFlag = value;
}

/*void initializeHashTable(int size, BM_HashTable *hTable){
	//SortedList sortedList;
	hTable->arraySize = size;

	// create array
	hTable->hashArray = (PageInfo *) malloc(sizeof(PageInfo) * hTable->arraySize);
	initializePageInfo(hTable->nonItem,-1);
}*/

// to hash the key
//int hashFunc1(int key, BM_HashTable *hTable) {
int hashFunc1(int key, int size) {
	return key % size;
}

// to set the stepSize
/*
int hashFunc2(int key) {
	// non-zero, less than array size, different from hashFunc1
	// array size must be relatively prime to 5, 4, 3, and 2
	return (5 - key % 5);
}
*/

// find an empty page frame
int findPageFrame(int pageNum, BM_HashTable *hTable){
	// (assumes table not full)
        int counter = 0, i;
        int size = hTable->arraySize;
	int hashVal = hashFunc1(pageNum, size); // hash the key
	int stepSize = 1; // get step size
	// int stepSize = hashFunc2(pageNum); // get step size

	// until empty cell with value -1
	while((counter < hTable->arraySize) && (getKey(hTable->hashArray[hashVal]) != -1))
	{	
		hashVal += stepSize; // add the step
		hashVal %= hTable->arraySize; // for wraparound
                counter++;
	}
	return hashVal;
}

/*

// delete a DataItem
PageInfo delete(int key, BM_HashTable hTable){
	int hashVal = hashFunc1(key, hTable); // hash the key
	int stepSize = hashFunc2(key); // get step size
	// until empty cell
	while(&hTable.hashArray[hashVal] != NULL) {

		// is correct hashVal?
		if(getKey(hTable.hashArray[hashVal]) == key)
		{
			PageInfo temp = hTable.hashArray[hashVal]; // save item
			hTable.hashArray[hashVal] = hTable.nonItem; // delete item
			return temp; // return item
		}
		hashVal += stepSize; // add the step
		hashVal %= hTable.arraySize; // for wraparound
	}
	return NULL; // can’t find item
}
*/


// find item with key
int find(int key, BM_HashTable *hTable){
	int counter = 0, i;
	// (assumes table not full)
        int size = hTable->arraySize;
        int hashVal = hashFunc1(key, size); // hash the key
	int stepSize = 1; // get step size for Linear probing way of hashing
	// int stepSize = hashFunc2(key); // get step size for separate chaining way of hashing
//	while((counter < hTable->arraySize) && (getKey(hTable->hashArray[hashVal]) != -1))  // until empty cell,
        while((counter < hTable->arraySize) && (hTable->hashArray[hashVal]->pageHandle->pageNum != -1))
	{ // is correct hashVal?
		if(getKey(hTable->hashArray[hashVal]) == key) {
			//return hTable.hashArray[hashVal]; // yes, return item
			return hashVal;
                }
		counter ++;
		hashVal += stepSize; // add the step
		hashVal %= hTable->arraySize; // for wraparound
	}
	return -1; // can’t find item
}


