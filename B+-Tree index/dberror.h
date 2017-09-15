#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

#define RC_SEEK_FILE_POSITION_FAILED 100 // seek the current position of the file failed
#define RC_SEEK_FILE_TAIL_ERROR 101 // seek file tail position failed
#define RC_CLOSE_FILE_FAILED 102 // close file failed
#define RC_REMOVE_FILE_FAILED 103 // remove file failed
#define RC_ENOUGH_PAGES 104 // enough pages, no need to append new pages
#define RC_READ_FILE_FAILED 110 // read file failed

//More return code for buffer mananger
#define RC_NO_REMOVABLE_PAGE 120 // no removable page for replacement
#define RC_PAGELIST_NOT_INITIALIZED 121 // PageList is not initialized
#define RC_PAGE_NOT_FOUND 122 // page not found when searching the requested page in the PageList
#define RC_INVALID_NUMPAGES 123 // numPages of the Buffer Pool is invalid
#define RC_PAGE_FOUND 124 // search the requested page and found it in the BUffer Pool
#define RC_FLUSH_POOL_ERROR 125 // force flush pool meets error (some pages are in use)
#define RC_RS_NOT_IMPLEMENTED 126 // the requested replacement strategy is not implemented

//More return code for record Manager
#define RC_INVALID_TARGET_VALUE 130 // invalid expected value
#define RC_SCHEMA_NOT_CREATED 131 // schema not created
#define RC_OPEN_TABLE_FAILED 132 // open table failed
#define RC_WRONG_NEW_RESERVED_PAGE_NUM 133 // wrong new reserved page number
#define RC_GET_FIRST_FREE_PAGE_ERROR 134 // get first free page error
#define RC_TABLE_FILE_NO_ACCESS 135 // Can't access to tables
#define RC_GET_IS_OCCUPIED_ERROR 136 // get isOccupied error
#define RC_CHECK_RECORD_LENGTH_ERROR 137 // check record length error
#define RC_PAGE_FULL_ERROR 138 // page is already full
#define RC_SET_TOTAL_PAGES_ERROR 139 //Error when setting pages
#define RC_RID_OUT_OF_BOUND 140 //Too many RIDs 
#define RC_RID_IS_RESERVED_PAGE 141 //Trying to return RID for a reserved page
#define RC_SLOT_EMPTY 142 // Empty Slot 
#define RC_SLOT_ERROR 143 // Invalid Slot 
#define RC_MEMORY_COPY_ERROR 144 //Memory copy error 
#define RC_WRONG_SCHEMA 145 // Encountered a wrong schema 
#define RC_WRONG_DATATYPE 146 // The wrong datatype 
#define RC_TABLE_EXISTS 147 //There exists a table 
#define RC_SET_TOTAL_RECORDS_ERROR 148 //Set total records error 
#define RC_NO_RID 149 //invalid RIDs 
#define RC_NOT_EQUALS_EXPR 150 // Expressions are not equal 
#define RC_LEFT_NOT_ATTR 151 // Left part is not an attribute
#define RC_RIGHT_NOT_CONS 152 // right part is not a constant 
#define RC_INVALID_ATTRREF 153 // Invalid attribute references
#define RC_INVALID_EXPR_CONS_DATATYPE 154 // invalid expression constant 

//Buffer_Mgr error message
#define RC_TRAVERSE_TO_THE_END 401
#define RC_INVALID_STRATEGY 402
#define RC_INVALID_PARAMETER 403
#define RC_LOCATE_PAGE_FAILED 404


#define RC_ERROR 400 // Added a new definiton for ERROR
#define RC_PINNED_PAGES_IN_BUFFER 500 // Added a new definition for Buffer Manager

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

// Added new definitions for Record Manager
#define RC_RM_NO_TUPLE_WITH_GIVEN_RID 600
#define RC_SCAN_CONDITION_NOT_FOUND 601

// Added new definition for B-Tree
#define RC_EXCEED_MAX_RANK 701
#define RC_INSERT_ERROR 702
#define RC_NO_RECORDS_TO_SCAN 703

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
