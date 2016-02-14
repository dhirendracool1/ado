#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"


// mgmt info for scanning
typedef struct RM_scanMgmtData
{
   RID *currentRID;
   Expr *expr;
} RM_scanMgmtData;

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  RM_scanMgmtData *mgmtData;
} RM_ScanHandle;

// Record flags in a block.
typedef enum RecInfo {
    Rec_Empty = 0,	// In free space available.
    Rec_Full = 1,	// Record present in that location.
    Rec_Tmbstn = 2	// Tombstone marker, record space can't be used.
} RecInfo;

// Record information and record data in a block.
typedef struct BlockRecords {
    int othPageNum;	// Optional page number, (For TID - used if record is moved)
    int othSlotNum;	// Optional slot number, (For TID - used if record is moved)
    int *nullVal;	// Null value bit map for a record
//    Record *rec;	// Actual record
    char *rec;
} BlockRecords;

// Records in and block header in the block.
typedef struct RM_tableRecords {
    RecInfo *recordInformation;	// Array of record flags for each record in a block
    BlockRecords **blkRecords;		// Array of records in a block
    int maxNumBlockRec;
} RM_tableRecords;



// Table management data to be stored in a block
typedef struct TableMgmtData {
    Schema *schema;		// Relation schema
    int numRecords;             // Number of records in the table.
    int recLength;              // # total bytes in a tuple.
    int totalTableBlocks;       // # blocks to hold for all tuples.
} TableMgmtData;

// Data to be stored in a page.
typedef struct PageData {
    int pageNum;		// Page Num
    int tblID;			// Table ID
    int dbID;			// Database ID
    bool pageInfoType;  	// False - Records are stored on this page.
                        	// True - Table management information stored on this page.
    union pageActData {		// Page either contains actual record info or rel mgmt info
        TableMgmtData *tblMgmtData;
        RM_tableRecords *rmTblRec;
    } pageActData;
} PageData;



// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

RC getNextRecordRID (RM_ScanHandle *scan, BM_PageHandle *ph);
PageData *createRecordPage(int pageNum, Schema *schema);
RC freeRecordsPage(PageData *pd);
PageData *deSerializePageData(char *data, bool pageType, Schema *schema);
char *serializePageData(PageData *pageData, Schema *schema, char *result);
char *serializeTableSchema(Schema *schema);
Schema *callCreateSchema(Schema *schema);
#endif // RECORD_MGR_H
