#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"
#include "btree_mgr.h"

// dynamic string
typedef struct VarString {
  char *buf;
  int size;
  int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
  do {							\
  var = (VarString *) malloc(sizeof(VarString));	\
  var->size = 0;					\
  var->bufsize = 100;					\
  var->buf = malloc(100);				\
  } while (0)

#define FREE_VARSTRING(var)			\
  do {						\
  free(var->buf);				\
  free(var);					\
  } while (0)

#define GET_STRING(result, var)			\
  do {						\
    result = malloc((var->size) + 1);		\
    memcpy(result, var->buf, var->size);	\
    result[var->size] = '\0';			\
  } while (0)

#define RETURN_STRING(var)			\
  do {						\
    char *resultStr;				\
    GET_STRING(resultStr, var);			\
    FREE_VARSTRING(var);			\
    return resultStr;				\
  } while (0)

#define ENSURE_SIZE(var,newsize)				\
  do {								\
    if (var->bufsize < newsize)					\
    {								\
      int newbufsize = var->bufsize;				\
      while((newbufsize *= 2) < newsize);			\
      var->buf = realloc(var->buf, newbufsize);			\
    }								\
  } while (0)

#define APPEND_STRING(var,string)					\
  do {									\
    ENSURE_SIZE(var, var->size + strlen(string));			\
    memcpy(var->buf + var->size, string, strlen(string));		\
    var->size += strlen(string);					\
  } while(0)

#define APPEND(var, ...)			\
  do {						\
    char *tmp = malloc(10000);			\
    sprintf(tmp, __VA_ARGS__);			\
    APPEND_STRING(var,tmp);			\
    free(tmp);					\
  } while(0)

// prototypes
static RC attrOffset (Schema *schema, int attrNum, int *result);

// implementations
char *
serializeTableInfo(RM_TableData *rel)
{
  VarString *result;
  MAKE_VARSTRING(result);
  
  APPEND(result, "TABLE <%s> with <%i> tuples:\n", rel->name, getNumTuples(rel));
  APPEND_STRING(result, serializeSchema(rel->schema));
  
  RETURN_STRING(result);  
}

char * 
serializeTableContent(RM_TableData *rel)
{
  int i;
  VarString *result;
  RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
  Record *r = (Record *) malloc(sizeof(Record));
  MAKE_VARSTRING(result);

  for(i = 0; i < rel->schema->numAttr; i++)
    APPEND(result, "%s%s", (i != 0) ? ", " : "", rel->schema->attrNames[i]);

  startScan(rel, sc, NULL);
  
  while(next(sc, r) != RC_RM_NO_MORE_TUPLES) 
    {
    APPEND_STRING(result,serializeRecord(r, rel->schema));
    APPEND_STRING(result,"\n");
    }
  closeScan(sc);

  RETURN_STRING(result);
}


char * 
serializeSchema(Schema *schema)
{
  int i;
  VarString *result;
  MAKE_VARSTRING(result);

  APPEND(result, "Schema with <%i> attributes (", schema->numAttr);

  for(i = 0; i < schema->numAttr; i++)
    {
      APPEND(result,"%s%s: ", (i != 0) ? ", ": "", schema->attrNames[i]);
      switch (schema->dataTypes[i])
	{
	case DT_INT:
	  APPEND_STRING(result, "INT");
	  break;
	case DT_FLOAT:
	  APPEND_STRING(result, "FLOAT");
	  break;
	case DT_STRING:
	  APPEND(result,"STRING[%i]", schema->typeLength[i]);
	  break;
	case DT_BOOL:
	  APPEND_STRING(result,"BOOL");
	  break;
	}
    }
  APPEND_STRING(result,")");

  APPEND_STRING(result," with keys: (");

  for(i = 0; i < schema->keySize; i++)
    APPEND(result, "%s%s", ((i != 0) ? ", ": ""), schema->attrNames[schema->keyAttrs[i]]); 

  APPEND_STRING(result,")\n");

  RETURN_STRING(result);
}

char * 
serializeRecord(Record *record, Schema *schema)
{
  VarString *result;
  MAKE_VARSTRING(result);
  int i;
  
  APPEND(result, "[%i-%i] (", record->id.page, record->id.slot);

  for(i = 0; i < schema->numAttr; i++)
    {
      APPEND_STRING(result, serializeAttr (record, schema, i));
      APPEND(result, "%s", (i == 0) ? "" : ",");
    }
  
  APPEND_STRING(result, ")");

  RETURN_STRING(result);
}

char * 
serializeAttr(Record *record, Schema *schema, int attrNum)
{
  int offset;
  char *attrData;
  VarString *result;
  MAKE_VARSTRING(result);
  
  attrOffset(schema, attrNum, &offset);
  attrData = record->data + offset;

  switch(schema->dataTypes[attrNum])
    {
    case DT_INT:
      {
	int val = 0;
	memcpy(&val,attrData, sizeof(int));
	APPEND(result, "%s:%i", schema->attrNames[attrNum], val);
      }
      break;
    case DT_STRING:
      {
	char *buf;
	int len = schema->typeLength[attrNum];
	buf = (char *) malloc(len + 1);
	strncpy(buf, attrData, len);
	buf[len] = '\0';
	
	APPEND(result, "%s:%s", schema->attrNames[attrNum], buf);
	free(buf);
      }
      break;
    case DT_FLOAT:
      {
	float val;
	memcpy(&val,attrData, sizeof(float));
	APPEND(result, "%s:%f", schema->attrNames[attrNum], val);
      }
      break;
    case DT_BOOL:
      {
	bool val;
	memcpy(&val,attrData, sizeof(bool));
	APPEND(result, "%s:%s", schema->attrNames[attrNum], val ? "TRUE" : "FALSE");
      }
      break;
    default:
      return "NO SERIALIZER FOR DATATYPE";
    }

  RETURN_STRING(result);
}

char *
serializeValue(Value *val)
{
  VarString *result;
  MAKE_VARSTRING(result);
  
  switch(val->dt)
    {
    case DT_INT:
      APPEND(result,"%i",val->v.intV);
      break;
    case DT_FLOAT:
      APPEND(result,"%f", val->v.floatV);
      break;
    case DT_STRING:
      APPEND(result,"%s", val->v.stringV);
      break;
    case DT_BOOL:
      APPEND_STRING(result, ((val->v.boolV) ? "true" : "false"));
      break;
    }

  RETURN_STRING(result);
}

Value *
stringToValue(char *val)
{
  Value *result = (Value *) malloc(sizeof(Value));
  
  switch(val[0])
    {
    case 'i':
      result->dt = DT_INT;
      result->v.intV = atoi(val + 1);
      break;
    case 'f':
      result->dt = DT_FLOAT;
      result->v.floatV = atof(val + 1);
      break;
    case 's':
      result->dt = DT_STRING;
      result->v.stringV = malloc(strlen(val));
      strcpy(result->v.stringV, val + 1);
      break;
    case 'b':
      result->dt = DT_BOOL;
      result->v.boolV = (val[1] == 't') ? TRUE : FALSE;
      break;
    default:
      result->dt = DT_INT;
      result->v.intV = -1;
      break;
    }
  
  return result;
}


RC 
attrOffset (Schema *schema, int attrNum, int *result)
{
  int offset = 0;
  int attrPos = 0;
  
  for(attrPos = 0; attrPos < attrNum; attrPos++)
    switch (schema->dataTypes[attrPos])
      {
      case DT_STRING:
	offset += schema->typeLength[attrPos];
	break;
      case DT_INT:
	offset += sizeof(int);
	break;
      case DT_FLOAT:
	offset += sizeof(float);
	break;
      case DT_BOOL:
	offset += sizeof(bool);
	break;
      }
  
  *result = offset;
  return RC_OK;
}

PageData *deSerializePageData(char *data, bool pageType, Schema *schema){

	int i, j;
        size_t leaveSizeBytes = 0;
        PageData *pageData = (PageData *) malloc(sizeof(PageData));

        memcpy(&pageData->pageNum, data + leaveSizeBytes, sizeof(int));   // get pageNum
	leaveSizeBytes += sizeof(int);
        memcpy(&pageData->tblID, data + leaveSizeBytes, sizeof(int));   // get table ID
	leaveSizeBytes += sizeof(int);
        memcpy(&pageData->dbID, data + leaveSizeBytes, sizeof(int));  // get DB ID
        leaveSizeBytes += sizeof(int);
        memcpy(&pageData->pageInfoType, data + leaveSizeBytes, sizeof(bool));   // get pageInfoType
        leaveSizeBytes += sizeof(bool);

        if (pageType == false){
		// If page contains record information.
                pageData->pageActData.rmTblRec = (RM_tableRecords *) malloc(sizeof(RM_tableRecords));
		// Get max records present in the page.
                memcpy(&pageData->pageActData.rmTblRec->maxNumBlockRec, data + leaveSizeBytes, sizeof(int));
	        leaveSizeBytes += sizeof(int);

		// Allocate memory for RecInfo
                pageData->pageActData.rmTblRec->recordInformation = (RecInfo *) malloc(sizeof(RecInfo)
                                * pageData->pageActData.rmTblRec->maxNumBlockRec );
		// Allocate memory for block of record array
		pageData->pageActData.rmTblRec->blkRecords = (BlockRecords **) malloc(sizeof(BlockRecords)
				* pageData->pageActData.rmTblRec->maxNumBlockRec);

		int tr = pageData->pageActData.rmTblRec->maxNumBlockRec;
		// Extract information related to each record.
                for(i=0; i < tr; i++) {
			// Extract rec info 
	                memcpy(&pageData->pageActData.rmTblRec->recordInformation[i], data + leaveSizeBytes, sizeof(RecInfo));
			leaveSizeBytes += sizeof(RecInfo);
		}

		size_t recSize = (size_t)getRecordSize(schema);

		for(i=0; i < tr; i++) {
			// Allocate menory for block record pointer.
	                pageData->pageActData.rmTblRec->blkRecords[i] = (BlockRecords *) malloc(sizeof(BlockRecords));
			// Copy page num (FOR TID case)
        		memcpy(&pageData->pageActData.rmTblRec->blkRecords[i]->othPageNum, data + leaveSizeBytes, sizeof(int));
			leaveSizeBytes += sizeof(int);

			// Copy slot num
			memcpy(&pageData->pageActData.rmTblRec->blkRecords[i]->othSlotNum, data + leaveSizeBytes, sizeof(int));
			leaveSizeBytes += sizeof(int);

			// Get Null value bit map.
			pageData->pageActData.rmTblRec->blkRecords[i]->nullVal = (int *) malloc(sizeof(int) * schema->numAttr);
			for(j=0; j < schema->numAttr; j++) {
				memcpy(&pageData->pageActData.rmTblRec->blkRecords[i]->nullVal[j], data + leaveSizeBytes, sizeof(int));
				leaveSizeBytes += sizeof(int);
			}

			// Get record information.
			pageData->pageActData.rmTblRec->blkRecords[i]->rec = (char *) malloc(recSize);
			memcpy(pageData->pageActData.rmTblRec->blkRecords[i]->rec, data + leaveSizeBytes, recSize);
			leaveSizeBytes += recSize;
		}

        } else if(pageType){
                pageData->pageActData.tblMgmtData = (TableMgmtData *) malloc(sizeof(TableMgmtData));
		// Get Num Records.
		memcpy(&pageData->pageActData.tblMgmtData->numRecords, data + leaveSizeBytes, sizeof(int));
		leaveSizeBytes += sizeof(int);

		// Get RecLength.
		memcpy(&pageData->pageActData.tblMgmtData->recLength, data + leaveSizeBytes, sizeof(int));
                leaveSizeBytes += sizeof(int);

                // Get total table blocks.
		memcpy(&pageData->pageActData.tblMgmtData->totalTableBlocks, data + leaveSizeBytes, sizeof(int));
                leaveSizeBytes += sizeof(int);

		// Get schema information.
		// Allocate memory for schema.
		pageData->pageActData.tblMgmtData->schema =  (Schema *) malloc(sizeof(Schema));
		memcpy(&pageData->pageActData.tblMgmtData->schema->numAttr, data + leaveSizeBytes, sizeof(int));
                leaveSizeBytes += sizeof(int);
		int numAttr = pageData->pageActData.tblMgmtData->schema->numAttr;

		// Get keySize.
		memcpy(&pageData->pageActData.tblMgmtData->schema->keySize, data + leaveSizeBytes, sizeof(int));
                leaveSizeBytes += sizeof(int);

		pageData->pageActData.tblMgmtData->schema->attrNames = (char **) malloc(sizeof(char *) * numAttr);
		pageData->pageActData.tblMgmtData->schema->dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
		pageData->pageActData.tblMgmtData->schema->typeLength = (int *) malloc(sizeof(int) * numAttr);
		
		for(i=0; i < numAttr; i++) {
			// Copy Attr names.
			int attrNameLen;
			memcpy(&attrNameLen, data + leaveSizeBytes, sizeof(int));
			leaveSizeBytes += sizeof(int);
			// Allocate memory for attribute name.
			pageData->pageActData.tblMgmtData->schema->attrNames[i] = (char *) malloc(sizeof(char));// * attrNameLen);
        	        memcpy(pageData->pageActData.tblMgmtData->schema->attrNames[i], data + leaveSizeBytes, sizeof(char));// * attrNameLen);
			leaveSizeBytes += (sizeof(char));// * attrNameLen);

			// Get datatype info.
			memcpy(&pageData->pageActData.tblMgmtData->schema->dataTypes[i], data + leaveSizeBytes, sizeof(DataType));
			leaveSizeBytes += sizeof(DataType);

			// Get typelength  info.
			memcpy(&pageData->pageActData.tblMgmtData->schema->typeLength[i], data + leaveSizeBytes, sizeof(int));
                        leaveSizeBytes += sizeof(int);
		}

		pageData->pageActData.tblMgmtData->schema->keyAttrs = (int *) malloc(sizeof(int) * pageData->pageActData.tblMgmtData->schema->keySize);
		for(i=0; i < pageData->pageActData.tblMgmtData->schema->keySize; i++) {
			memcpy(&pageData->pageActData.tblMgmtData->schema->keyAttrs[i], data + leaveSizeBytes, sizeof(int));
                        leaveSizeBytes += sizeof(int);
		}
        }

        return pageData;
}


char *serializePageData(PageData *pageData, Schema *schema, char *result){
	int i, j;
        size_t copyFromByte = 0;
//        char *result = (char *) malloc(PAGE_SIZE);

	// Copy page number
	memmove(result + copyFromByte, &pageData->pageNum, sizeof(int)) ;
	copyFromByte += sizeof(int);

	// Copy tblID, dbID and page info type.
	memmove(result + copyFromByte, &pageData->tblID, sizeof(int)) ;
        copyFromByte += sizeof(int);
	memmove(result + copyFromByte, &pageData->dbID, sizeof(int)) ;
        copyFromByte += sizeof(int);
	memmove(result + copyFromByte, &pageData->pageInfoType, sizeof(bool)) ;
        copyFromByte += sizeof(bool);

        // Copy union i.e. serialize it.
        if(pageData->pageInfoType == false){
                // Records are stored on this page. Hence serialize RM_tableRecords.
		memmove(result + copyFromByte, &pageData->pageActData.rmTblRec->maxNumBlockRec, sizeof(int)) ;
	        copyFromByte += sizeof(int);

		// Serialize record information
		int tr = pageData->pageActData.rmTblRec->maxNumBlockRec;
                // Rec Info
                for(i=0; i < tr; i++) {
			memmove(result + copyFromByte, &pageData->pageActData.rmTblRec->recordInformation[i], sizeof(RecInfo));
			copyFromByte += sizeof(RecInfo);
                }

                size_t recSize = (size_t)getRecordSize(schema);

                for(i=0; i < tr; i++) {
                        // Copy page num (FOR TID case)
                        memmove(result + copyFromByte, &pageData->pageActData.rmTblRec->blkRecords[i]->othPageNum, sizeof(int));
                        copyFromByte += sizeof(int);

                        // Copy slot num
                        memmove(result + copyFromByte, &pageData->pageActData.rmTblRec->blkRecords[i]->othSlotNum, sizeof(int));
                        copyFromByte += sizeof(int);

                        // Get Null value bit map.
                        for(j=0; j < schema->numAttr; j++) {
                                memmove(result + copyFromByte, &pageData->pageActData.rmTblRec->blkRecords[i]->nullVal[j], sizeof(int));
                                copyFromByte += sizeof(int);
                        }

                        // Get record information.
                        memmove(result + copyFromByte, pageData->pageActData.rmTblRec->blkRecords[i]->rec, recSize);
                        copyFromByte += recSize;
                }

        } else if (pageData->pageInfoType){
                // Table management information stored on this page. Hence serialize TableMgmtData.
		memmove(result + copyFromByte, &pageData->pageActData.tblMgmtData->numRecords, sizeof(int));
                copyFromByte += sizeof(int);
		memmove(result + copyFromByte, &pageData->pageActData.tblMgmtData->recLength, sizeof(int));
                copyFromByte += sizeof(int);
                memmove(result + copyFromByte, &pageData->pageActData.tblMgmtData->totalTableBlocks, sizeof(int));
                copyFromByte += sizeof(int);

		memmove(result+copyFromByte, &(pageData->pageActData.tblMgmtData->schema->numAttr), sizeof(int)) ;  // copy numAttr
	        copyFromByte += sizeof(int);
	        memmove(result+copyFromByte, &(pageData->pageActData.tblMgmtData->schema->keySize), sizeof(int)) ; //copy keySize
        	copyFromByte += sizeof(int);

	        // for each attribute
        	for(i=0; i<pageData->pageActData.tblMgmtData->schema->numAttr; i++) {
                	int attributeSize = strlen(pageData->pageActData.tblMgmtData->schema->attrNames[i]) + 1;
	                memmove(result+copyFromByte, &attributeSize, sizeof(int)) ; //copy attribute size
        	        copyFromByte += sizeof(int);

                	memmove(result+copyFromByte, pageData->pageActData.tblMgmtData->schema->attrNames[i], (strlen(pageData->pageActData.tblMgmtData->schema->attrNames[i]) )) ; //copy attrName
	                copyFromByte += (strlen(pageData->pageActData.tblMgmtData->schema->attrNames[i]));
        	        memmove(result+copyFromByte, &(pageData->pageActData.tblMgmtData->schema->dataTypes[i]), sizeof(DataType)) ;  // copy dataType
                	copyFromByte += sizeof(DataType);
	                memmove(result+copyFromByte, &(pageData->pageActData.tblMgmtData->schema->typeLength[i]), sizeof(int)) ;  // copy typeLength
        	        copyFromByte += sizeof(int);
	        }
        	// copy key Attributes
	        for(i=0; i<pageData->pageActData.tblMgmtData->schema->keySize; i++){
        	        memmove(result+copyFromByte, &(pageData->pageActData.tblMgmtData->schema->keyAttrs[i]), sizeof(int)) ;
                	copyFromByte += sizeof(int);
	        }
/*
		char *schemaChar = serializeTableSchema(pageData->pageActData.tblMgmtData->schema);
		memmove(result + copyFromByte, schemaChar, (sizeof(char)*strlen(schemaChar)));
		copyFromByte += (sizeof(char)*strlen(schemaChar)); */
        }

        return result;
}

char *serializeTableSchema(Schema *schema) {
	int i;
	char *result = (char *) malloc(PAGE_SIZE);
	size_t leaveSizeBytes = 0;

 	memmove(result+leaveSizeBytes, &(schema->numAttr), sizeof(int)) ;  // copy numAttr
  	leaveSizeBytes += sizeof(int);
  	memmove(result+leaveSizeBytes, &(schema->keySize), sizeof(int)) ; //copy keySize
  	leaveSizeBytes += sizeof(int);
  	// for each attribute
  	for(i=0; i<schema->numAttr; i++) {
		int attributeSize = strlen(schema->attrNames[i]);
                memmove(result+leaveSizeBytes, &attributeSize, sizeof(int)) ; //copy attribute size
                leaveSizeBytes += sizeof(int);
 		memmove(result+leaveSizeBytes, schema->attrNames[i], strlen(schema->attrNames[i])) ; //copy attrName
 		leaveSizeBytes += strlen(schema->attrNames[i]);
 		memmove(result+leaveSizeBytes, &(schema->dataTypes[i]), sizeof(DataType)) ;  // copy dataType
 		leaveSizeBytes += sizeof(DataType);
 		memmove(result+leaveSizeBytes, &(schema->typeLength[i]), sizeof(int)) ;  // copy typeLength
 		leaveSizeBytes += sizeof(int);
  	}
  	// copy key Attributes
  	for(i=0; i<schema->keySize; i++){
 		memmove(result+leaveSizeBytes, &(schema->keyAttrs[i]), sizeof(int)) ;
 		leaveSizeBytes += sizeof(int);
  	}
	char *tempString = (char *) malloc(leaveSizeBytes);
	memcpy(tempString, result, leaveSizeBytes);
	free(result);
  	return tempString;
}

RC serializeBTreeNodePage(BTreeNodePage *nodePage, char *pageInfo) {

    size_t copyFromByte = 0;

    // Copy page num, key type, num of keys, num of pointers, max num of keys, node info.
    memmove(pageInfo + copyFromByte, &nodePage->pageNum, sizeof(int));
    copyFromByte += sizeof(int);

    memmove(pageInfo + copyFromByte, &nodePage->keyType, sizeof(DataType));
    copyFromByte += sizeof(DataType);

    memmove(pageInfo + copyFromByte, &nodePage->numKeys, sizeof(int));
    copyFromByte += sizeof(int);

    memmove(pageInfo + copyFromByte, &nodePage->numPointers, sizeof(int));
    copyFromByte += sizeof(int);

    memmove(pageInfo + copyFromByte, &nodePage->maxNumKeys, sizeof(int));
    copyFromByte += sizeof(int);

    memmove(pageInfo + copyFromByte, &nodePage->isLeaf, sizeof(bool));
    copyFromByte += sizeof(bool);

    memmove(pageInfo + copyFromByte, &nodePage->isRoot, sizeof(bool));
    copyFromByte += sizeof(bool);

    int i=0;
    for(i=0; i < nodePage->numKeys; i++) {
        memmove(pageInfo + copyFromByte, &nodePage->bTreeNode->keys[i], sizeof(Value));
        copyFromByte += sizeof(Value);
    }

    for(i=0; i <= nodePage->maxNumKeys; i++) {
        memmove(pageInfo + copyFromByte, &nodePage->bTreeNode->recordsPtr[i].page, sizeof(int));
        copyFromByte += sizeof(int);

        memmove(pageInfo + copyFromByte, &nodePage->bTreeNode->recordsPtr[i].slot, sizeof(int));
        copyFromByte += sizeof(int);
    }

    return RC_OK;
}

BTreeNodePage *deSerializeBTreeNodePage(char *pageInfo) {

    BTreeNodePage *nodePage = (BTreeNodePage *) malloc(sizeof(BTreeNodePage));
    size_t leaveSizeBytes = 0;

    memcpy(&nodePage->pageNum, pageInfo + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);

    memcpy(&nodePage->keyType, pageInfo + leaveSizeBytes, sizeof(DataType));
    leaveSizeBytes += sizeof(DataType);

    memcpy(&nodePage->numKeys, pageInfo + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);

    memcpy(&nodePage->numPointers, pageInfo + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);

    memcpy(&nodePage->maxNumKeys, pageInfo + leaveSizeBytes, sizeof(int));
    leaveSizeBytes += sizeof(int);

    memcpy(&nodePage->isLeaf, pageInfo + leaveSizeBytes, sizeof(bool));
    leaveSizeBytes += sizeof(bool);

    memcpy(&nodePage->isRoot, pageInfo + leaveSizeBytes, sizeof(bool));
    leaveSizeBytes += sizeof(bool);

    // Allocate memory for tree node structure.
    nodePage->bTreeNode = (Node *) malloc(sizeof(Node));

    // Allocate memory for keys.
    nodePage->bTreeNode->keys = (Value *) malloc(sizeof(Value) * nodePage->maxNumKeys);

    // Allocate memory for pointers in the tree.
    nodePage->bTreeNode->recordsPtr = (RID *) malloc(sizeof(RID) * (nodePage->maxNumKeys+1));

    int i=0;
    for(i=0; i < nodePage->numKeys; i++) {
        memcpy(&nodePage->bTreeNode->keys[i], pageInfo + leaveSizeBytes, sizeof(Value));
        leaveSizeBytes += sizeof(Value);
    }

    for(i=0; i <= nodePage->maxNumKeys; i++) {
        memcpy(&nodePage->bTreeNode->recordsPtr[i].page, pageInfo + leaveSizeBytes, sizeof(int));
        leaveSizeBytes += sizeof(int);

        memcpy(&nodePage->bTreeNode->recordsPtr[i].slot, pageInfo + leaveSizeBytes, sizeof(int));
        leaveSizeBytes += sizeof(int);
    }

    return nodePage;
}
