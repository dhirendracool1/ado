#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096
#define FILE_HEADER_SIZE 64

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_INVALID_PAGE_NUMBER 5
#define RC_READ_SEEK_FAILED 6
#define RC_READ_ERROR 7
#define RC_EOF 8
#define RC_FILE_POINTER_NOT_AVAILABLE 15
#define RC_FILE_CRT_FAILED 9
#define RC_CHUNK_NOT_FULL_WRITTEN 10
#define RC_FILE_NOT_CLOSED 11
#define RC_FILE_DEL_FAILED 12
#define RC_FILE_NOT_OPENED 16
#define RC_PAGE_HANDLE_NOT_INIT 13
#define RC_WRITE_SEEK_FAILED 14

//***** Assignment 2 Error codes.
#define RC_FORCE_FLUSH_FAILED 18
#define RC_POOL_IN_USE 19
#define RC_BUFFER_MGR_NOT_INIT 20
#define RC_PAGE_NOT_FOUND 21
#define RC_FORCE_FAILED 22
#define RC_INVALID_PAGENUMBER 23
#define RC_NULL_LIST 24
#define RC_NO_SUCH_LINK 25
#define RC_NO_FRAME_AVAILABLE 26
#define RC_INVALID_REPLACEMENT_STRATEGY 27
#define RC_WRONG_NUMBER_OF_FRAMES 28
#define RC_FILE_NAME_NOT_PROVIDED 29
#define RC_PAGE_REPLACE_FAIL -2
#define RC_CANNOT_REPLACE_PAGE -3
#define RC_NO_FRAME_WAIT 30
#define RC_BUFFERPOOL_SHUTDOWN_FAILED 311

//***** Assignment 3 Error codes.
#define RC_TABLE_CREATION_FAILED 31
#define RC_TABLE_NAME_NOT_PROVIDED 32
#define RC_READ_SCHEMA_PAGE_FAILED 33
#define RC_SCHEMA_MEM_FREE_FAILED 34
#define RC_FILE_NOT_PROVIDED 35
#define RC_NO_TBL_MGMT_INFO 36
#define RC_NULL_SCHEMA 37
#define RC_NULL_RECORD 38
#define RC_INVALID_ATTR_NUMBER 39
#define RC_NULL_PAGEDATA 40
#define RC_NO_TBLDATA 41
#define RC_TABLE_DELETE_FAILED 42
#define RC_BUFFER_INIT_FAILED 43
#define RC_BUFFERPOOL_SHUTDOWN_FALED 44
#define RC_ARG_NULL 45
#define RC_WRITE_PAGE_FAILED 46
#define RC_UNPIN_PAGE_FAILED 47
#define RC_NULL_TABLEDATA 48
#define RC_INVALID_RID 49
#define RC_TMBSTONE_RECORD 50
#define RC_READ_NON_EXISTING_RECORD 51
#define RC_EMPTY_RECORD 52
#define RC_CLDNT_WRITE_PAGE 53
#define RC_UNPIN_FAILED 54
#define RC_NULL_SCANHANDLE 55
#define RC_SCHEMAMEM_FREE_FAILED 56
#define RC_WRONG_DATATYPE 57
#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

/* Assignment 4 */
#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303
#define RC_FILENAME_NOT_GIVEN 304
#define RC_FILENAME_NOT_GIVEN 304
#define RC_READ_BTREE_INFO_FAILED 305
#define RC_INDEX_NOT_DELETED 306
#define RC_TREEHANDLE_NOT_INITIALIZED 307
#define RC_NULL_KEY 308
#define RC_BOOL_INDEX_NOT_SUPPORTED 309
#define RC_SERIALIZATION_FAILED 310
#define RC_READ_NODE_PAGE_FAILED 312

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
