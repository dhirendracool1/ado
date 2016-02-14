#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "record_mgr.h"


// table and manager
RC initRecordManager(void *mgmtData) {
    // TO DO
    return RC_OK;
}

RC shutdownRecordManager() {
    // TO DO
    return RC_OK;
}

RC createTable(char *name, Schema *schema) {

    // Create a file the table.
    if (createPageFile(name) != RC_OK)
        return RC_TABLE_CREATION_FAILED;

    // Initialize bufferpool for the this page file/Table.
    BM_BufferPool *bm = MAKE_POOL();
    if (initBufferPool(bm, name, 2, RS_FIFO, NULL) != RC_OK)
        return RC_BUFFER_INIT_FAILED;

    // Pin the first page and write the schema and table details in the page file in first few pages.
    PageData *pd = (PageData *) malloc(sizeof(PageData));
    pd->pageNum = 0;
    pd->tblID = 1000;
    pd->dbID = 2000;
    pd->pageInfoType = true;

    pd->pageActData.tblMgmtData = (TableMgmtData *) malloc(sizeof(TableMgmtData));
    pd->pageActData.tblMgmtData->schema = schema;
    pd->pageActData.tblMgmtData->numRecords = 0; // Initially no records in relation.
    pd->pageActData.tblMgmtData->totalTableBlocks = 1; // Total blocks allocated for the page.
    pd->pageActData.tblMgmtData->recLength = getRecordSize(schema);

    // Initial page data is ready. Put it into the page 0.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    pinPage(bm, ph, 0);
   // ph->data = serializePageData(pd, schema);
    serializePageData(pd, schema, ph->data);

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
    if(ph->data)
        free(ph->data);
    if(ph)
        free(ph); // Free BM page handle
    free(pd->pageActData.tblMgmtData); // Free schema and page data table info
    free(pd); // Free page data.

    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {

    if (name == NULL)
        return RC_TABLE_NAME_NOT_PROVIDED;

    // Update table name in the data structure
    rel->name = (char *) malloc(sizeof(char) * (strlen(name) + 1));
    strcpy(rel->name, name);

    // Allocate memory of RM_tableMgmtData
    rel->mgmtData = (RM_tableMgmtData *) malloc(sizeof(RM_tableMgmtData));

    // Initialize bufferpool.
    rel->mgmtData->bm = MAKE_POOL();
    if (initBufferPool(rel->mgmtData->bm, name, 10, RS_FIFO, NULL) != RC_OK)
        return RC_BUFFER_INIT_FAILED;

    // Read table schema and update the RM_TableData
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    if (pinPage(rel->mgmtData->bm, ph, 0) != RC_OK) // Page 0 contains schema
        return RC_READ_SCHEMA_PAGE_FAILED;

    // Update schema in the RM_TableData
    PageData *newPd = deSerializePageData(ph->data, true, rel->schema);
    rel->schema = callCreateSchema(newPd->pageActData.tblMgmtData->schema);
    rel->mgmtData->numRecords = newPd->pageActData.tblMgmtData->numRecords;
    rel->mgmtData->recLength = newPd->pageActData.tblMgmtData->recLength;
    rel->mgmtData->totalTableBlocks = newPd->pageActData.tblMgmtData->totalTableBlocks;

    // Unpin page.
    if (unpinPage(rel->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    if(ph->data)
        free(ph->data);
    if(ph)
        free(ph);
    if(newPd->pageActData.tblMgmtData)
        free(newPd->pageActData.tblMgmtData);
    if(newPd)
        free(newPd);
    return RC_OK;
}

RC closeTable(RM_TableData *rel) {

    if (rel == NULL)
        return RC_ARG_NULL;

    // Bufferpool no longer required, shutdown bufferpool
    if (shutdownBufferPool(rel->mgmtData->bm) != RC_OK)
        return RC_BUFFERPOOL_SHUTDOWN_FALED;

    // Free buffer pool manager.
    free(rel->mgmtData->bm);

    // Free memory allocated for schema.
    if (freeSchema(rel->schema) != RC_OK)
        return RC_SCHEMA_MEM_FREE_FAILED;

    // Free memory allocated for relation name.
    free(rel->name);
    free(rel->mgmtData);        // Table mgmt data.

    return RC_OK;
}

RC deleteTable(char *name) {
    if (name == NULL)
        return RC_FILE_NOT_PROVIDED;

    if (destroyPageFile(name) != RC_OK)
        return RC_TABLE_DELETE_FAILED;

    return RC_OK;
}

int getNumTuples(RM_TableData *rel) {
    if (rel == NULL)
        return RC_NO_TBLDATA;

    if (rel->mgmtData == NULL)
        return RC_NO_TBL_MGMT_INFO;

    return rel->mgmtData->numRecords;
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {

    if(rel == NULL)
        return RC_NULL_TABLEDATA;

    if(record == NULL)
        return RC_NULL_RECORD;

    // Create a page handle
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    int slot, j;
    bool find=false;
    int pageID=rel->mgmtData->totalTableBlocks-1;
    RC retCode;

    /* Get the last page from table file using bufferpool associated with the table
       file. (Read page into ph)
       Basically, data to be inserted will be inserted in sequential manner,
       because of this no free slot will be available on the previous pages.
       Lets say, if we delete a record and then that particular slot will be marked
       as TOMBSTONE, so that particular slot will not be used to insert new record. */

    bool isMgmtPage = true;
    PageData *page;
    while(isMgmtPage) {

        if ((retCode = pinPage(rel->mgmtData->bm, ph, pageID)) != RC_OK)
            return retCode;

        // If page info type is false, then use same page for working.
        page = deSerializePageData(ph->data, false, rel->schema);

        if(!page->pageInfoType) {
            // If page contains record information
            isMgmtPage = page->pageInfoType;
            int numRecords= page->pageActData.rmTblRec->maxNumBlockRec;    // Max # blks in a page.

            for (j=0; j < numRecords; j++) {
                // Check block header info, to check which slot is free to insert record in that slot.
                if (page->pageActData.rmTblRec->recordInformation[j] == Rec_Empty) {
                    // If empty slot found use it to insert new record.
                    slot=j;
                    find=true;
                    break;
                }
            }
        } else {
            pageID++;
            if(page)
                freeRecordsPage(page);
            if(pageID == rel->mgmtData->totalTableBlocks)
                break;
        }
    }

    if (find) {
        // If free slot found on the last data page of table file
        record->id.page = pageID;
        record->id.slot = slot;
    } else {
        // If empty space no available on the page. Use new page to insert data
        if(isMgmtPage) {
            record->id.page = pageID;
        } else {
            record->id.page = pageID+1;
            if(page)
                freeRecordsPage(page);
        }

        record->id.slot = 0;

	// Unpin previously considered page.
        if (unpinPage(rel->mgmtData->bm, ph) != RC_OK)
            return RC_UNPIN_FAILED;

        if(ph->data)
            free(ph->data);

        //there was no empty space so we should create next Block	
        if ((retCode = pinPage(rel->mgmtData->bm, ph, record->id.page)) != RC_OK)
            return retCode;

        // As we need to insert data in new page. So initialize the page data struct for this new page.
        page = createRecordPage(pageID, rel->schema);
        rel->mgmtData->totalTableBlocks++;
    }

    
    // Put data in the structure.
    memcpy(page->pageActData.rmTblRec->blkRecords[record->id.slot]->rec, record->data, rel->mgmtData->recLength);
    page->pageActData.rmTblRec->recordInformation[record->id.slot] = Rec_Full;    // Rec slot is full
    rel->mgmtData->numRecords++;    // Increase num of recs in the table.

    // Update BM_PAGEHANDLE
    ph->pageNum = record->id.page;
    serializePageData(page, rel->schema, ph->data);

    // make page dirty
    retCode = markDirty(rel->mgmtData->bm, ph);
    if (retCode != RC_OK)
        return retCode;

    // Unpin respective page.
    if (unpinPage(rel->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    if(ph->data)
        free(ph->data);
    if(ph)
        free(ph);
    if(page)
        freeRecordsPage(page);
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {

        if(rel == NULL)
        return RC_NULL_TABLEDATA;

    if((id.page < 0) || (id.slot < 0))
        return RC_INVALID_RID;

    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    // Pinpage mentioned in RID.
    RC retCode = pinPage(rel->mgmtData->bm, ph, id.page);
    if (retCode != RC_OK)
        return retCode;

    PageData *page = deSerializePageData(ph->data, false, rel->schema);
    //set the RecInfo to a Tombstone    
    page->pageActData.rmTblRec->recordInformation[id.slot] = Rec_Tmbstn;
    //decrease number of records
    rel->mgmtData->numRecords--;

    // Write down the page in the buffer pool.
    serializePageData(page, rel->schema, ph->data);
    // make page dirty
    if ((retCode = markDirty(rel->mgmtData->bm, ph)) != RC_OK)
        return retCode;

    // Unpin page.
    if (unpinPage(rel->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    if(ph->data)
        free(ph->data);
    if(ph)
        free(ph);
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {

    if(rel == NULL)
        return RC_NULL_TABLEDATA;    // If table data information not provided.

    if(record == NULL)
        return RC_NULL_RECORD;        // If record information not provided.

    if((record->id.page < 0) || (record->id.slot < 0))
        return RC_INVALID_RID;        // IF RID given is invalid.

    // Access page requested for the record.
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    RC retCode = pinPage(rel->mgmtData->bm, ph, record->id.page);
    if (retCode != RC_OK)
        return retCode;

    // Read block data into the PageData structure
    PageData *page = deSerializePageData(ph->data, false, rel->schema);
    //update record
    memcpy(page->pageActData.rmTblRec->blkRecords[record->id.slot]->rec, record->data, getRecordSize(rel->schema));
    // make page dirty
    serializePageData(page, rel->schema, ph->data);
    retCode = markDirty(rel->mgmtData->bm, ph);
    if (retCode != RC_OK)
        return retCode;

    // Unpin page.
    if (unpinPage(rel->mgmtData->bm, ph) != RC_OK)
        return RC_UNPIN_FAILED;

    if(ph->data)
        free(ph->data);
    if(ph)
        free(ph);
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {

    if(rel == NULL)
        return RC_NULL_TABLEDATA;    // If table data info not provided.

    if((id.page < 0) || (id.slot < 0))
        return RC_INVALID_RID;        // If invalid i.e. negative RID provided.

    // Access page related to given RID
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    RC retCode = pinPage(rel->mgmtData->bm, ph, id.page);
    if (retCode != RC_OK) {
        if(ph->data)
            free(ph->data);
        if(ph)
            free(ph);
        return retCode;
    }

    PageData *page = deSerializePageData(ph->data, false, rel->schema);

    //check whether records are stored on this page
    if (page->pageInfoType==false) {
        if(page->pageActData.rmTblRec->recordInformation[id.slot] == Rec_Full) {
            record->id= id;        // Set RID
            // Update record data.
            memcpy(record->data, page->pageActData.rmTblRec->blkRecords[id.slot]->rec, getRecordSize(rel->schema));
            // Unpin page.
            if (unpinPage(rel->mgmtData->bm, ph) != RC_OK) {
                if(page)
                    freeRecordsPage(page);
                if(ph->data)
                    free(ph->data);
                if(ph)
                    free(ph);
                return RC_UNPIN_FAILED;
            }
            if(page)
                freeRecordsPage(page);
            if(ph->data)
                free(ph->data);
            if(ph)
                free(ph);
            return RC_OK;
        } else if (page->pageActData.rmTblRec->recordInformation[id.slot] == Rec_Tmbstn) {
            // Unpin page.
            if (unpinPage(rel->mgmtData->bm, ph) != RC_OK) {
                if(page)
                    freeRecordsPage(page);
                if(ph->data)
                    free(ph->data);
                if(ph)
                    free(ph);
                return RC_UNPIN_FAILED;
            }
            if(page)
                freeRecordsPage(page);
            if(ph->data)
                free(ph->data);
            if(ph)
                free(ph);
            return RC_TMBSTONE_RECORD;
        } else {
            // Unpin page.
            if (unpinPage(rel->mgmtData->bm, ph) != RC_OK) {
                if(page)
                    freeRecordsPage(page);
                if(ph->data)
                    free(ph->data);
                if(ph)
                    free(ph); 
                return RC_UNPIN_FAILED;
            }
            if(page)
                freeRecordsPage(page);
            if(ph->data)
                free(ph->data);
            if(ph)
                free(ph);
            return RC_EMPTY_RECORD;
        }
    } else {
        // Unpin page.
        if (unpinPage(rel->mgmtData->bm, ph) != RC_OK) {
            if(page)
                freeRecordsPage(page);
            if(ph->data)
                free(ph->data);
            if(ph)
                free(ph);
            return RC_UNPIN_FAILED;
        }
        if(page)
            freeRecordsPage(page);
        if(ph->data)
            free(ph->data);
        if(ph)
            free(ph);
        return RC_READ_NON_EXISTING_RECORD;
    }
}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

    if(rel == NULL)
        return RC_NULL_TABLEDATA;

    // Allocate memory for RM_tableData in scan
    scan->rel = (RM_TableData *) malloc(sizeof(RM_TableData));
    // Update table name in the scan's RM_tableData
    scan->rel->name = (char *) malloc (sizeof(char) * (strlen(rel->name) + 1));
    strcpy(scan->rel->name, rel->name);

    // Update schema in scan's RM_TableData
    scan->rel->schema = callCreateSchema(rel->schema);

    // Allocate memory for RM_tableMgmtData
    scan->rel->mgmtData = (RM_tableMgmtData *) malloc(sizeof(RM_tableMgmtData));
    scan->rel->mgmtData->bm = rel->mgmtData->bm;    // Bufferpool
    scan->rel->mgmtData->numRecords = rel->mgmtData->numRecords;    // Assign numRecords
    scan->rel->mgmtData->recLength = rel->mgmtData->recLength;    // Assign recLength
    scan->rel->mgmtData->totalTableBlocks = rel->mgmtData->totalTableBlocks;    // # table blocks.

    // Allocate memory for scan related management data
    scan->mgmtData = (RM_scanMgmtData *) malloc(sizeof(RM_scanMgmtData));
    scan->mgmtData->currentRID = (RID *) malloc(sizeof(RID));    // Allocate mem for RID
//    scan->mgmtData->expr = (Expr *) malloc(sizeof(Expr));       // Mem for Expression.
//    memcpy(scan->mgmtData->expr, cond, sizeof(Expr));

    scan->mgmtData->expr = cond;

    // Initialize currentRID for scan
    scan->mgmtData->currentRID->page = 1;
    scan->mgmtData->currentRID->slot = -1;

    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {

    if(scan == NULL)
        return RC_NULL_SCANHANDLE;    // Scanhandle not provided.

    Value *val;
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    // Get valid RID to be scanned.
    RC retCode = getNextRecordRID(scan, ph);
    if(retCode != RC_OK) {
        if(ph->data)
            free(ph->data);
        if(ph)
            free(ph);
        return retCode;
    }

    // Get the page data
    PageData *page = deSerializePageData(ph->data, false, scan->rel->schema);
    // Record is available. Read it.
    RID id;
    id.page = scan->mgmtData->currentRID->page;
    id.slot = scan->mgmtData->currentRID->slot;
    retCode = getRecord(scan->rel, id, record);
    if (retCode != RC_OK)
        return retCode;

    if (scan->mgmtData->expr==NULL) {
        if(page)
            freeRecordsPage(page);
        if(ph->data)
            free(ph->data);
        if(ph)
            free(ph);
        return RC_OK;
    } else {
        retCode = evalExpr(record, scan->rel->schema, scan->mgmtData->expr, &val);
        if(!val->v.boolV) {
            return (next(scan, record));
        }
        if(page)
            freeRecordsPage(page);
        if(ph->data)
            free(ph->data);
        if(ph)
            free(ph);
        return RC_OK;
    }
}

// This function is used get next valid RID to be read.
RC getNextRecordRID(RM_ScanHandle *scan, BM_PageHandle *ph) {
    // Total Blocks for table
    int totalTableBlocks = scan->rel->mgmtData->totalTableBlocks;

    // Total Records in a block
    int memAvailForRecords = PAGE_SIZE - ((sizeof(int) * 3) + sizeof(bool));
        int recSize = getRecordSize(scan->rel->schema);
        int memReqdForOneRec = (sizeof(int) * (scan->rel->schema->numAttr + 2)) + recSize
                        + sizeof(RecInfo);
        int maxNumBlockRecords = memAvailForRecords / memReqdForOneRec;

    // Allocate memory for page handle.
    bool isMgmtPage = true;
    PageData *page;
    bool checkPage = true;
    while(checkPage) {
        /* Infinite loop - breakout only if
           1. No more tuples to read. (RETURN)
           2. If we found a tuple to read. (RETURN)*/
        while(isMgmtPage) {
            /* Loop to get valid record page.
               Breakout of loop iff,
               1. No more tuples available. (RETURN)
               2. Page is record page.
               3. Page pinning failed. (RETURN) */
            if(scan->mgmtData->currentRID->page > totalTableBlocks)
                return RC_RM_NO_MORE_TUPLES;        // No tuples available for table.

            RC retCode = pinPage(scan->rel->mgmtData->bm, ph, scan->mgmtData->currentRID->page);
            if (retCode != RC_OK)
                        return retCode;

            // Get the page data
            page = deSerializePageData(ph->data, false, scan->rel->schema);
            isMgmtPage = page->pageInfoType;        // Check page type, mgmt or record page.
            if(isMgmtPage) {
                // Unpin previous page.
                if (unpinPage(scan->rel->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;
                if(page)
                    freeRecordsPage(page);
                scan->mgmtData->currentRID->page++;
            }
        }

        // We are here means, page that we got is a record page.
        int slot = ++scan->mgmtData->currentRID->slot;
        // Check whether slot is empty or full or tombstoned.
        for(; slot < maxNumBlockRecords; slot++) {
            /* Get record slot, which should be either FULL or empty.
               Loop working cases and breakout -
               1. If Rec_FULL found. (BREAK) 
               2. If Rec_EMPTY, No more tuples available. (RETURN)
               3. If Rec_TMBSTONE, check next slot.
               4. If slot num >= total number of recs in a block.*/
            if(page->pageActData.rmTblRec->recordInformation[slot] == Rec_Full)
                break;                // We have found slot for the record to be read.
            else if(page->pageActData.rmTblRec->recordInformation[slot] == Rec_Empty) {
                if (unpinPage(scan->rel->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;
                if(page)
                    freeRecordsPage(page);
                return RC_RM_NO_MORE_TUPLES;    // This means no more tuples 
            }
                            // can be found after this slot.
        }

        /* We here means, either we have found our valid record slot.
           or for loop condtion evaluated to false, which means we gotta check
           next for the record. */
        if(slot < maxNumBlockRecords) {    // Update current slot with its actual value.
            scan->mgmtData->currentRID->slot = slot;
            if (unpinPage(scan->rel->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;
            if(page)
                freeRecordsPage(page);
            return RC_OK;
        } else {
            /* We are here means, we did not find record in this page,
               get next page check data on that page. */
            scan->mgmtData->currentRID->slot = -1;
            if (unpinPage(scan->rel->mgmtData->bm, ph) != RC_OK)
                    return RC_UNPIN_FAILED;
            if(page)
                freeRecordsPage(page);
            int nextPage = scan->mgmtData->currentRID->page + 1;

            // Check next page will be valid or not.
            if(nextPage >= totalTableBlocks) {
                if(page)
                    freeRecordsPage(page);
                return RC_RM_NO_MORE_TUPLES; 
            } else {
                scan->mgmtData->currentRID->slot = -1;
                scan->mgmtData->currentRID->page++;
            }
        }
    }
}


RC closeScan(RM_ScanHandle *scan) {

    if(scan == NULL)
        return RC_NULL_SCANHANDLE;

    free(scan->mgmtData->currentRID);    // RID in scan's mgmt data
    free(scan->mgmtData);       // Scan's management data
    free(scan->rel->mgmtData);    // Scan's RM_tableData's mgmt data
    free(scan->rel->name);      // Table name in scan
    free(scan->rel);        // SCan's RM_tableData

    return RC_OK;
}

// dealing with schemas
int getRecordSize(Schema *schema) {

    if (schema == NULL)
        return RC_NULL_SCHEMA;

    int recSizeInBytes = 0;
    // Update length of the record in bytes for the table/relation for each attribute
    int i;
    for (i=0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
        case DT_INT:
            recSizeInBytes += sizeof(int);
            break;
        case DT_STRING:
//            recSizeInBytes += (sizeof(char) * schema->typeLength[i]) + 1;
            recSizeInBytes += schema->typeLength[i] + 1;
            break;
        case DT_FLOAT:
            recSizeInBytes += sizeof(float);
            break;
        case DT_BOOL:
            recSizeInBytes += sizeof(bool);
            break;
        }
    }
    return recSizeInBytes;
}




Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
        int *typeLength, int keySize, int *keys) {

    // Allocate memory for schema
    Schema *newSchema = (Schema *) malloc(sizeof(Schema));
    newSchema->numAttr = numAttr; // # of attributes in schema
    newSchema->keySize = keySize; // # keys in schema
    newSchema->attrNames = attrNames;
    newSchema->dataTypes = dataTypes;
    newSchema->typeLength = typeLength;
    newSchema->keyAttrs = keys;

    return newSchema;
}


RC freeSchema(Schema *schema) {

    if (schema == NULL)
        return RC_NULL_SCHEMA;

    free(schema->keyAttrs); // Free keys array memory.
    free(schema->typeLength); // Free data type length array memory.
    free(schema->dataTypes); // Free array of data types array.

    // Free memory allocated for each attribute name pointer
    int i=0;
    for (; i < schema->numAttr; i++)
        free(schema->attrNames[i]);

    free(schema->attrNames); // Free pointer to a array of pointers.
    free(schema); // Free memory allocated for schema.

    return RC_OK;
}


// Allocate memroy for page record.
PageData *createRecordPage(int pageNum, Schema *schema) {

    // Allocate memory for pageData.
    PageData *pd = (PageData *) malloc(sizeof(PageData));
    pd->pageNum = pageNum;
    pd->tblID = 1111;
    pd->dbID = 2222;
    pd->pageInfoType = false;

    // Allocate memory for records present on the page.
    pd->pageActData.rmTblRec = (RM_tableRecords *) malloc(sizeof(RM_tableRecords));
    /* 
     1. Size available for records and its related information.
     Size Available = PAGE_SIZE - ((sizeof(int) * 3) + sizeof(bool)) [ 3 int's and 1 bool ]
     2. Size required for block records info.
     TotalSizeFor1Rec = 2 int's + numAttr * 1 int + getRecordSize(schema) + sizeof(RecInfo)
     */
    int memAvailForRecords = PAGE_SIZE - ((sizeof(int) * 3) + sizeof(bool));
    int recSize = getRecordSize(schema);
    int memReqdForOneRec = (sizeof(int) * (schema->numAttr + 2)) + recSize + sizeof(RecInfo);
    pd->pageActData.rmTblRec->maxNumBlockRec = memAvailForRecords / memReqdForOneRec;

    // Record info, empty space, TOMBSTONE, space occupied by record.
    pd->pageActData.rmTblRec->recordInformation = (RecInfo *) malloc(sizeof(RecInfo) * pd->pageActData.rmTblRec->maxNumBlockRec);

    // Array of records and records header to placed in the block.
    pd->pageActData.rmTblRec->blkRecords = (BlockRecords **) malloc(sizeof(BlockRecords *) * pd->pageActData.rmTblRec->maxNumBlockRec);

    // Allocate memory for block records
    int i, j;
    for (i=0; i < pd->pageActData.rmTblRec->maxNumBlockRec; i++) {
        pd->pageActData.rmTblRec->recordInformation[i] = Rec_Empty;
        pd->pageActData.rmTblRec->blkRecords[i] = (BlockRecords *) malloc(sizeof(BlockRecords));
        pd->pageActData.rmTblRec->blkRecords[i]->othPageNum = -1;
        pd->pageActData.rmTblRec->blkRecords[i]->othSlotNum = -1;
        pd->pageActData.rmTblRec->blkRecords[i]->nullVal = (int *) malloc(sizeof(int) * schema->numAttr);
        for (j=0; j < schema->numAttr; j++)
            pd->pageActData.rmTblRec->blkRecords[i]->nullVal[j] = 0; // Initially all attr slots are NULL.

        pd->pageActData.rmTblRec->blkRecords[i]->rec = (char *) malloc(recSize); // Mem allocated for actual rec data.
    }

    return pd;
}

// Free page data memory.
RC freeRecordsPage(PageData *pd) {

    if (pd == NULL)
        return RC_NULL_PAGEDATA;

    int i;
    if(pd->pageActData.rmTblRec->blkRecords) {
        for (i=0; i < pd->pageActData.rmTblRec->maxNumBlockRec; i++) {
            free(pd->pageActData.rmTblRec->blkRecords[i]->rec); // Free actual data space
            free(pd->pageActData.rmTblRec->blkRecords[i]->nullVal); // Free null val bitmap array
            free(pd->pageActData.rmTblRec->blkRecords[i]); // Free mem allocated for record pointers
        }

        free(pd->pageActData.rmTblRec->blkRecords); // Free mem allocated for array blk recs
        free(pd->pageActData.rmTblRec->recordInformation); // Free mem allocated for array of rec info.
        free(pd->pageActData.rmTblRec); // Free mem allocated for rec and record struct.
    } else if(pd->pageActData.tblMgmtData) {
        freeSchema(pd->pageActData.tblMgmtData->schema);
        free(pd->pageActData.tblMgmtData);
    }
    free(pd); // Free mem allocated for page data
    return RC_OK;
}


// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema) {
    /*
     * Creating a new record allocates enough memory to the data field to
     * hold the binary representations for all attributes of this record as
     * determined by the schema.
     */
    if(schema == NULL)
        return RC_NULL_SCHEMA;
    // malloc for attributes data individually
    // array of values for all attributes in a record
    Value attrArray[schema->numAttr];
    *record = (Record *) malloc(sizeof(Record));
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    (*record)->data = (char *) malloc(getRecordSize(schema));
    strcpy((*record)->data, "\0");

    // Initialize all the records and data.
    int i=0, copyDataFrom = 0;
    for(i=0; i < schema->numAttr; i++) {
        attrArray[i].dt = schema->dataTypes[i];
        switch(schema->dataTypes[i]) {
            case DT_INT:
                attrArray[i].v.intV = (int) NULL;
                memcpy((*record)->data + copyDataFrom, &attrArray[i].v.intV, sizeof(int));
                copyDataFrom += sizeof(int);
                break;
            case DT_STRING:
                attrArray[i].v.stringV = (char *) malloc(schema->typeLength[i] + 1);
                char *null = "\0";
                strcpy(attrArray[i].v.stringV, "\0");
                strcpy((*record)->data + copyDataFrom, attrArray[i].v.stringV);
                memcpy((*record)->data + copyDataFrom, attrArray[i].v.stringV, strlen(attrArray[i].v.stringV));
                copyDataFrom += (schema->typeLength[i]);
                free(attrArray[i].v.stringV);
                break;
            case DT_FLOAT:
                attrArray[i].v.floatV = 0.0;
                memcpy((*record)->data + copyDataFrom, &attrArray[i].v.floatV, sizeof(float));
                copyDataFrom += sizeof(float);
                break;
            case DT_BOOL:
                attrArray[i].v.boolV = false;
                memcpy((*record)->data + copyDataFrom, &attrArray[i].v.boolV, sizeof(bool));
                copyDataFrom += sizeof(bool);
                break;
            default:
                return RC_WRONG_DATATYPE;
        }
    }

    return RC_OK;
}

// Free memory allocated for record.
RC freeRecord(Record *record) {
    if(record == NULL)
        return RC_NULL_RECORD;
    if(record->data)
        free(record->data);
    if(record)
        free(record);
    return RC_OK;
}


// Get the specified attribute value from the relation.
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    if(schema == NULL)
        return RC_NULL_SCHEMA;
    if(record == NULL)
        return RC_NULL_RECORD;

    // check if attrNum passed is valid and exists for the record
    if(attrNum >= schema->numAttr)
        return RC_INVALID_ATTR_NUMBER;

    // Calculate actual position of data for attr in the data.
    int getDataFromByte = 0, i=0;
    for(i=0; i < attrNum; i++) {
        switch(schema->dataTypes[i]) {
            case DT_INT:
                getDataFromByte += sizeof(int);
                break;
            case DT_STRING:
                getDataFromByte += schema->typeLength[i] + 1;
                break;
            case DT_FLOAT:
                getDataFromByte += sizeof(float);
                break;
            case DT_BOOL:
                getDataFromByte += sizeof(bool);
                break;
            default:
                return RC_WRONG_DATATYPE;
        }
    }

    // Calculate how many bytes to be read from data.
    *value = (Value *) malloc(sizeof(Value));
    int readInBytes = 0;
    switch(schema->dataTypes[attrNum]) {
        case DT_INT:
            readInBytes += sizeof(int);
            (*value)->dt = DT_INT;
            memcpy(&(*value)->v.intV, record->data + getDataFromByte, readInBytes);
            break;
        case DT_STRING:
            readInBytes += schema->typeLength[attrNum] + 1;
            (*value)->dt = DT_STRING;
            (*value)->v.stringV = (char *) malloc(readInBytes);
            memcpy((*value)->v.stringV, record->data + getDataFromByte, readInBytes);
            break;
        case DT_FLOAT:
            readInBytes += sizeof(float);
            (*value)->dt = DT_FLOAT;
            memcpy(&(*value)->v.floatV, record->data + getDataFromByte, readInBytes);
            break;
        case DT_BOOL:
            readInBytes += sizeof(bool);
            (*value)->dt = DT_BOOL;
            memcpy(&(*value)->v.boolV, record->data + getDataFromByte, readInBytes);
            break;
        default:
            return RC_WRONG_DATATYPE;
    }

    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    if(schema == NULL)
        return RC_NULL_SCHEMA;
    if(record == NULL)
        return RC_NULL_RECORD;

    // check if attrNum passed is valid and exists for the record
    if(attrNum >= schema->numAttr)
        return RC_INVALID_ATTR_NUMBER;

    // Calculate actual position of data for attr in the data.
    int putDataFromByte = 0, i=0;
    for(i=0; i < attrNum; i++) {
        switch(schema->dataTypes[i]) {
            case DT_INT:
                putDataFromByte += sizeof(int);
                break;
            case DT_STRING:
                putDataFromByte += schema->typeLength[i] + 1;
                break;
            case DT_FLOAT:
                putDataFromByte += sizeof(float);
                break;
            case DT_BOOL:
                putDataFromByte += sizeof(bool);
                break;
            default:
                return RC_WRONG_DATATYPE;
        }
    }

    // Calculate how many bytes to be read from data.
    int putBytes = 0;
    switch(value->dt) {
        case DT_INT:
            putBytes += sizeof(int);
            memcpy(record->data + putDataFromByte, &value->v.intV, putBytes);
            break;
        case DT_STRING:
            putBytes += schema->typeLength[attrNum] + 1;
            memcpy(record->data + putDataFromByte, value->v.stringV, putBytes);
            break;
        case DT_FLOAT:
            putBytes += sizeof(float);
            memcpy(record->data + putDataFromByte, &value->v.floatV, putBytes);
            break;
        case DT_BOOL:
            putBytes += sizeof(bool);
            memcpy(record->data + putDataFromByte, &value->v.boolV, putBytes);
            break;
        default:
            return RC_WRONG_DATATYPE;
    }

    return RC_OK;
}

// Call create schema function
Schema *callCreateSchema(Schema *schema) {
    return (createSchema(schema->numAttr, schema->attrNames, schema->dataTypes,
        schema->typeLength, schema->keySize, schema->keyAttrs));
}
