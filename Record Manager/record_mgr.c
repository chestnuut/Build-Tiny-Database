#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "buffer_mgr.h"
#include "dberror.h"
#include "dt.h"
#include "expr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "tables.h"
/*
	Created by Zinuo Li
*/
#define FIRSTPAGE_RESERVED 96
#define PAGE_CAPACITY sizeof(char) * 8 * (PAGE_SIZE - FIRSTPAGE_RESERVED)
#define PAGE_TABLEINFO 0
#define PAGE_RESERVEDFIRST 1
//call bufferpool
BM_BufferPool *BM;

Schema *SCHEMA = NULL;
int PAGE_MAX_RECORDS = -1;
char *SCHEMA_STR = NULL;
int RECORD_LENGTH = -1;
//call record management
RM_TableData *REL = NULL;

// prototypes
//table and manager
RC initRecordManager(void *mgmtData);
RC shutdownRecordManager();
RC makePage_TableInfo();
RC createTable(char *name, Schema *schema);
RC openTable(RM_TableData *rel, char *name);
RC closeTable(RM_TableData *rel);
RC deleteTable(char *name);
int getNumTuples(RM_TableData *rel);

//handling records in a table
RC insertRecord(RM_TableData *rel, Record *record);
RC deleteRecord(RM_TableData *rel, RID id);
RC updateRecord(RM_TableData *rel, Record *record);
RC getRecord(RM_TableData *rel, RID id, Record *record);

//scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
RC next(RM_ScanHandle *scan, Record *record);
RC closeScan(RM_ScanHandle *scan);

//dealing with scemas
int getRecordSize(Schema *schema);
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
		int *typeLength, int keySize, int *keys);
RC freeSchema(Schema *schema);

//dealing with records and attribute values
RC createRecord(Record **record, Schema *schema);
RC freeRecord(Record *record);
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value);
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value);

//Condition update using scans
RC condUpdateRecord(RM_ScanHandle *scan, Expr *setCond);

//Auxiliary Functions
int getLengthOfRecord(RM_TableData *rel);
int TotalLengthofRecords(RM_TableData *rel);
RC setLengthofRecords(RM_TableData *rel, int value);
int getTotalPages(RM_TableData *rel);
RC setTotalPages(RM_TableData *rel, int value);
int MaxRecordsInPage(RM_TableData *rel);

RC ValidRID(RM_TableData *rel, RID id);
PageNumber UsedPageNum(PageNumber pageNum);
bool isFreePage(RM_TableData *rel, PageNumber reservedPageNum,
		PageNumber pageNum);
bool getBit(char *data, int CoresBits);
RC setBit(char *data, int CoresBits, bool value);

RC makeUsedPage(RM_TableData *rel, PageNumber reservedPageNum);
RC WriteUsedPage(RM_TableData *rel, PageNumber reservedPageNum, PageNumber pageNum, bool value);

PageNumber FirstFreePage(RM_TableData *rel, PageNumber reservedPageNum);
RC setFirstFreePage(RM_TableData *rel, PageNumber reservedPageNum, int value);
PageNumber getSecondUsedPage(RM_TableData *rel, PageNumber reservedPageNum);

RC setSecondUsedPage(RM_TableData *rel, PageNumber reservedPageNum, int value);
PageNumber getPrevUsedPage(RM_TableData *rel, PageNumber reservedPageNum);
RC setPrevUsedPage(RM_TableData *rel, PageNumber reservedPageNum, int value);

PageNumber searchFirstFreePage(RM_TableData *rel);
bool isPageInitialized(RM_TableData *rel, PageNumber pageNum);
RC setPageInitialized(RM_TableData *rel, PageNumber pageNum, bool value);
PageNumber getCurrentRecords(RM_TableData *rel, PageNumber pageNum);
RC setCurrentRecords(RM_TableData *rel, PageNumber pageNum, int value);

bool isSlotOccupied(RM_TableData *rel, PageNumber pageNum, int slotId);
RC setSlotOccupied(RM_TableData *rel, PageNumber pageNum, int slotId, bool value);
RC checkRecordLength(RM_TableData *rel, char *recordData);

RC initRecordPage(RM_TableData *rel, PageNumber pageNum);
int getFreeSlotId(RM_TableData *rel, PageNumber pageNum);


char *schemaToString(Schema * schema);


RC initRecordManager(void *mgmtData) {
	printf("BEGIN\n");
	printf("Record Manager Initializing...\n");
	RC result;
	int numPages = 10;
	// init BUffer Manager
	// init parameters for Buffer Manager
	if (BM == NULL) {
		BM = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
	}
	//Using LRU to deal with buffer
	ReplacementStrategy strategy = RS_LRU;
	// Call initBufferPool() to init Buffer Manager
	
	result = -127;
	result = initBufferPool(BM, "", numPages, strategy, NULL);
	//if not being intialized, raise an error
	if (result != RC_OK) {
		return result;
	}
	//set memory of schema
	if (SCHEMA == NULL) {
		SCHEMA = (Schema *) malloc(sizeof(Schema));

		SCHEMA->numAttr = -1;
		SCHEMA->attrNames = NULL;
		SCHEMA->dataTypes = NULL;
		SCHEMA->typeLength = NULL;
		SCHEMA->keyAttrs = NULL;
		SCHEMA->keySize = -1;
	}
	//allocate string in schema
	if (SCHEMA_STR == NULL) {
		SCHEMA_STR = (char *) calloc(PAGE_SIZE, sizeof(char));
	}
	//allocate record manager
	if (REL == NULL) {
		REL = (RM_TableData *) malloc(sizeof(RM_TableData));
		REL->name = NULL;
		REL->schema = SCHEMA;
		REL->mgmtData = BM;
	}
	printf("END-initialize\n\n");
	return RC_OK;
} 

RC makePage_TableInfo() {
	printf("Beginning-makePage_TableInfo\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;
	// Pin to page PAGE_TABLEINFO[0], page that stores the table info
	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);
	//if the page not found, return error
	if (result != RC_OK) {
		return result;
	}
	//if it's done, mark it dirty
	result = -127;
	result = markDirty(BM, page);
	//if the page not found, return an error
	if (result != RC_OK) {
		return result;
	}

	// Set totalRecords=0, totalPages=0
	int totalRecords = 0;
	int totalPages = 1;

	// Write the following type of info into the page:
	// RECORD_LENGTH: int
	// totalRecords: int
	// totalPages: int
	// PAGE_MAX_RECORDS: int
	// SCHEMA_STR: char * (seperated by '|')
	// create a pointer with type int * to store int data
	int *intp = (int *) page->data; 
	intp[0] = RECORD_LENGTH;
	intp[1] = totalRecords;
	intp[2] = totalPages;
	intp[3] = PAGE_MAX_RECORDS;
	// Create a pointer with char * to store string data
	char *charp = (char *) (intp + 4); 
	memcpy(charp, SCHEMA_STR, strlen(SCHEMA_STR));

	// Unpin the table info page since we already done with it
	result = -127;
	result = unpinPage(BM, page);
	//raise an error if not found
	if (result != RC_OK) {
		return result;
	}
	//free the memory
	free(page);

	printf("END-makePageTableINfo\n\n");
	return RC_OK;
} 

RC createTable(char *name, Schema *schema) {
	printf("Beginning-createTable\n");
	RC result;

	//return an error code if the transformed SCHEMA_STR is empty 
	if (SCHEMA == NULL || strlen(SCHEMA_STR) == 0) {
		return RC_SCHEMA_NOT_CREATED;
	}

	// Check if the file exists
	result = -127;
	result = access(name, F_OK);

	// if the file does not exist (result = -1), create it by calling createPageFile(),
	// else if the file exists (result = 0), then return error code to report it
	// else, some error happens, return result
	if (result == -1) {
		result = -127;
		result = createPageFile(name);

		if (result != RC_OK) {
			return result;
		}
	} else if (result == 0) {
		return RC_TABLE_EXISTS;
	} else {
		return result;
	}

	// Set the name of the file to the Buffer Manager
	BM->pageFile = name;

	// initialize the table info page
	result = -127;
	result = makePage_TableInfo();

	if (result != RC_OK) {
		return result;
	}

	// Then create the first reserved page, which is page PAGE_RESERVEDFIRST[1]
	result = -127;
	result = makeUsedPage(NULL, PAGE_RESERVEDFIRST);

	printf("END-CreateTable\n\n");
	return result;
} 

RC deleteTable(char *name) {
	printf("Beginning-deleteTable\n");
	RC result;
	//delete the table, if not works return an error
	result = destroyPageFile(name);

	if (result != RC_OK) {
		return result;
	}
	printf("END-deletetable\n\n");
	return RC_OK;
} 

int getNumTuples(RM_TableData *rel) {
	printf("Beginning-getNumTuples\n");
	RC result = TotalLengthofRecords(rel);

	printf("END-getNumTuples\n\n");
	return result;
} 

RC openTable(RM_TableData *rel, char *name) {
	printf("Beginning-openTable\n");
	// We need Set all the info needed

	// Check if the global variable SCHEMA_STR is empty

	// if the file has no access,return an error
	if (access(name, R_OK) != 0 || access(name, W_OK) != 0) {
		return RC_TABLE_FILE_NO_ACCESS;
	}
	// Set the name of the file to the Buffer Manager
	BM->pageFile = name;
	// Set the table structure
	rel->name = name;
	rel->schema = SCHEMA;
	// Store the entry of the Buffer Manager here in the table structure
	rel->mgmtData = BM;

	printf("END-openTable\n\n");
	return RC_OK;
} 

RC closeTable(RM_TableData *rel) {
	printf("Beginning-closeTable\n");
	RC result;

	// Write back all stuff back
	result = -127;
	result = forceFlushPool(BM);

	if (result != RC_OK) {
		return result;
	}
	// set the file name in BM to null 
	BM->pageFile = NULL;

	//set the table info to NULL in RM
	rel->name = NULL;
	rel->schema = NULL;
	rel->mgmtData = NULL;

	printf("END-closeTable\n\n");
	return RC_OK;
} 

RC shutdownRecordManager() {
	printf("Beginning-shutdownRecordManager\n");
	RC result;
	//free the memory of string in schema
	free(SCHEMA_STR);
	SCHEMA_STR = NULL;
	//then free the schema
	free(SCHEMA);
	SCHEMA = NULL;

	//Shutdown bufferpool to avoid memory leaks
	result = -127;
	result = shutdownBufferPool(BM);

	if (result != RC_OK) {
		return result;
	}
	// free the allocated memory for buffer amanager
	free(BM);
	BM = NULL;
	// free the allocated memory for Record manager
	free(REL);
	REL = NULL;

	printf("END-shutdownRecordManager\n\n");
	return RC_OK;
}

int getLengthOfRecord(RM_TableData *rel) {
	printf("Beginning-getLengthOfRecord\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;
	//pin to page[0] where stores those information
	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data; 
	int recordLength = intp[0];
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}
	//unleash the memory
	free(page);

	printf("END-getLengthOfRecord\n\n");
	return recordLength;
} 

RC setLengthofRecords(RM_TableData *rel, int value) {
	printf("Beginning-setLengthofRecords\n");

	if (value < 0) {
		return RC_SET_TOTAL_RECORDS_ERROR;
	}

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}
	//change the data frame pointer to int pointer
	int *intp = (int *) page->data;

	intp[1] = value;

	result = -127;
	result = unpinPage(BM, page);
	//if not the case, raise an error
	if (result != RC_OK) {
		return result;
	}
	//free the memory allocated
	free(page);

	printf("END-SettotalRecords\n\n");
	return RC_OK;
}

int TotalLengthofRecords(RM_TableData *rel) {
	printf("Beginning-TotalLengthofRecords\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);

	//if pinPage not successful
	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data;
	int totalRecords = intp[1];

	result = -127;
	result = unpinPage(BM, page);
	// if unpin page not successful
	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-getTotalRecods\n\n");
	return totalRecords;
} 

RC setTotalPages(RM_TableData *rel, int value) {
	printf("Beginning-setTotalPages\n");
	if (value < 1) {
		return RC_SET_TOTAL_PAGES_ERROR;
	}

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);

	if (result != RC_OK) {
		return result;
	}
	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}
	int *intp = (int *) page->data;
	intp[2] = value;

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}
	//free memory allocated
	free(page);

	printf("END-setTotalPages\n\n");
	return RC_OK;
}

RC WriteUsedPage(RM_TableData *rel, PageNumber reservedPageNum, PageNumber pageNum, bool value) {
	printf("Beginning-WriteUsedPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// find the bit corresponding to the page
	int CoresBits = FIRSTPAGE_RESERVED * sizeof(char) * 8 + pageNum - reservedPageNum;

	// pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}
	result = -127;
	result = setBit(page->data, CoresBits, value);

	PageNumber firstFreePageNum = FirstFreePage(rel, reservedPageNum);

	if (firstFreePageNum < 0) {
		return firstFreePageNum;
	}

	// if the value to be set is FALSE (page becomes full) and the pageNum <= firstFreePage,
	// find the next first free page's number (start from the pageNum's bit) and reset the firstFreePage
	if (value == FALSE && pageNum <= firstFreePageNum) {
		// Find the next first free page
		// Set the pointer to the first bit after the pageNum's bit
		if (firstFreePageNum != (reservedPageNum + PAGE_CAPACITY)) {
			CoresBits++;
		}
		// Set the firstFreePage
		int maxCoresBits = PAGE_SIZE * sizeof(char) * 8;

		while (CoresBits < maxCoresBits && getBit(page->data, CoresBits) == 0) {
			CoresBits++;
		}
		//if CoresBits == maxCoresBits and getBit(page->data, CoresBits) == 0, thwn set first free page.
		if (CoresBits == maxCoresBits && getBit(page->data, CoresBits) == 0) {
			int totalPages = getTotalPages(rel);

			if (totalPages < 0) {
				return totalPages;
			}

			result = -127;
			result = makeUsedPage(rel, totalPages);

			if (result != RC_OK) {
				return result;
			}

			result = -127;
			result = setTotalPages(rel, ++totalPages);

			if (result != RC_OK) {
				return result;
			}

			result = -127;
			result = setFirstFreePage(rel, reservedPageNum, 0);

			if (result != RC_OK) {
				return result;
			}
		} else {
			PageNumber nextFreePageNum = CoresBits
					- FIRSTPAGE_RESERVED * sizeof(char) * 8 + reservedPageNum;
			result = -127;
			result = setFirstFreePage(rel, reservedPageNum, nextFreePageNum);

			if (result != RC_OK) {
				return result;
			}
		}
	}

	// unpin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-WriteUsedPage\n\n");
	return RC_OK;
} 

int getTotalPages(RM_TableData *rel) {
	printf("Beginning-getTotalPages\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data; 
	int totalPages = intp[2];

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-getTotalPages\n\n");
	return totalPages;
}

int MaxRecordsInPage(RM_TableData *rel) {
	printf("Beginning-MaxRecordsInPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, PAGE_TABLEINFO);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data; 
	int pageMaxRecords = intp[3];

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-MaxRecordsInPage\n\n");
	return pageMaxRecords;
} 

RC ValidRID(RM_TableData *rel, RID id) {
	printf("Beginning-ValidRID\n");
	int totalPages = getTotalPages(rel);

	if (totalPages < 0) {
		return totalPages;
	}

	int pageMaxRecords = MaxRecordsInPage(rel);

	if (pageMaxRecords < 0) {
		return pageMaxRecords;
	}

	if (id.page < 0 || id.page > totalPages - 1 || id.slot < 0
			|| id.slot > pageMaxRecords - 1) {
		return RC_RID_OUT_OF_BOUND;
	}

	int dataPages = PAGE_CAPACITY + PAGE_RESERVEDFIRST;

	if ((id.page % dataPages) == PAGE_RESERVEDFIRST) {
		return RC_RID_IS_RESERVED_PAGE;
	}

	printf("END-ValidRID\n\n");
	return RC_OK;
} 

bool isFreePage(RM_TableData *rel, PageNumber reservedPageNum, PageNumber pageNum) {
	printf("Beginning-isFreePage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	int CoresBits = FIRSTPAGE_RESERVED * sizeof(char) * 8 + pageNum
			- reservedPageNum;

	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	bool isFree = getBit(page->data, CoresBits);

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END\n\n");
	return isFree;
} 

RC makeUsedPage(RM_TableData *rel, PageNumber reservedPageNum) {
	printf("Beginning-makeUsedPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	memset(page->data, 0xFF, PAGE_SIZE);

	int *intp = (int *) page->data;

	intp[0] = reservedPageNum + 1;
	intp[1] = 0;

	if (reservedPageNum == PAGE_RESERVEDFIRST) {
		intp[2] = 0;
	} else {
		intp[2] = reservedPageNum
				- (PAGE_CAPACITY + PAGE_RESERVEDFIRST);

		result = -127;
		result = setSecondUsedPage(rel, intp[2], reservedPageNum);

		if (result != RC_OK) {
			return result;
		}
	}

	char *cp = page->data + sizeof(int) * 3;
	memset(cp, '#', FIRSTPAGE_RESERVED - sizeof(int) * 3);

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	// increment totalPages by one
	int totalPages = getTotalPages(rel);

	if (totalPages < 0) {
		return totalPages;
	}

	result = -127;
	result = setTotalPages(rel, ++totalPages);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-makeUsedPage\n\n");
	return RC_OK;
}

PageNumber FirstFreePage(RM_TableData *rel, PageNumber reservedPageNum) {
	printf("Beginning-FirstFreePage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data;
	int firstFreePage = intp[0];

	// unpin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-FirstFreePage\n\n");
	return firstFreePage;
} 

bool getBit(char *data, int CoresBits) {
	printf("Beginning-getBit\n");
	int byteOffset;
	int bitPosition;
	bool bitFlag;

	// Calculate CoresBits / (sizeof(char) * 8) and get quotient and remainder
	div_t divResult = div(CoresBits, sizeof(char) * 8);

	// Set offsets
	if (divResult.rem == 0) {
		byteOffset = divResult.quot - 1;
		bitPosition = divResult.rem;
	} else {
		byteOffset = divResult.quot;
		bitPosition = sizeof(char) * 8 - divResult.rem;
	}

	char byteValue = data[byteOffset];
	char bitValue = (byteValue >> bitPosition) & 1;

	if (bitValue == 0x00) {
		bitFlag = FALSE;
	} else if (bitValue == 0x01) {
		bitFlag = TRUE;
	} else {
		return -1;
	}

	printf("END-getBit\n\n");
	return bitFlag;
} 


RC setBit(char *data, int CoresBits, bool value) {
	printf("Beginning-setBit\n");
	int byteOffset;
	int bitPosition;
	char target;

	div_t divResult = div(CoresBits, sizeof(char) * 8);

	if (divResult.rem == 0) {
		byteOffset = divResult.quot - 1;
		bitPosition = divResult.rem;
	} else {
		byteOffset = divResult.quot;
		bitPosition = sizeof(char) * 8 - divResult.rem;
	}

	char byteValue = data[byteOffset];

	// Set target value
	if (value == TRUE) {
		target = 0x01;
	} else if (value == FALSE) {
		target = 0x00;
	} else {
		return RC_INVALID_TARGET_VALUE;
	}

	char resultValue = (byteValue & ~(1 << (bitPosition)))
			| (target << (bitPosition));

	data[byteOffset] = resultValue;

	printf("END-setbit\n\n");
	return RC_OK;
} 


PageNumber UsedPageNum(PageNumber pageNum) {
	printf("Beginning-UsedPageNum\n");
	PageNumber reservedPageNum;

	int dataPages = PAGE_CAPACITY + PAGE_RESERVEDFIRST;

	div_t d = div(pageNum, dataPages);

	if (d.rem == 0) {
		reservedPageNum = (d.quot - 1) * dataPages + PAGE_RESERVEDFIRST;
	} else {
		reservedPageNum = d.quot * dataPages + PAGE_RESERVEDFIRST;
	}

	printf("END-UsedPageNum\n\n");
	return reservedPageNum;
} 

RC setFirstFreePage(RM_TableData *rel, PageNumber reservedPageNum, int value) {
	printf("Beginning-setFirstFreePage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	int *intp = (int *) page->data;
	intp[0] = value;

	// unpin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-SetFirstFreePage\n\n");
	return RC_OK;
} 

RC setSecondUsedPage(RM_TableData *rel, PageNumber reservedPageNum, int value) {
	printf("Beginning-setSecondUsedPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	int *intp = (int *) page->data;
	intp[1] = value;

	// unpin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-setSecondUsedPage\n\n");
	return RC_OK;
} 

PageNumber getSecondUsedPage(RM_TableData *rel, PageNumber reservedPageNum) {
	printf("Beginning-getSecondUsedPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data;
	int nextReservedPage = intp[1];

	// unpin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-getSecondUsedPage\n\n");
	return nextReservedPage;
}  


PageNumber searchFirstFreePage(RM_TableData *rel) {
	printf("Beginning-searchFirstFreePage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	PageNumber firstFreePage;
	PageNumber reservedPageNum = PAGE_RESERVEDFIRST;

	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data;

	while (intp[0] == 0 && intp[1] != 0) {
		result = -127;
		result = unpinPage(BM, page);

		if (result != RC_OK) {
			return result * (-1);
		}

		reservedPageNum += PAGE_CAPACITY + PAGE_RESERVEDFIRST;

		result = -127;
		result = pinPage(BM, page, reservedPageNum);

		if (result != RC_OK) {
			return result * (-1);
		}

		intp = (int *) page->data;
	}

	if (intp[0] == 0 && intp[1] == 0) {
		// Set firstFreePage to -1 as error code
		firstFreePage = -1;
	} else {
		firstFreePage = intp[0];
	}

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-searchFirstFreePage\n\n");
	return firstFreePage;
}

RC setPrevUsedPage(RM_TableData *rel, PageNumber reservedPageNum, int value) {
	printf("Beginning-setPrevUsedPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// Pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	int *intp = (int *) page->data;
	intp[2] = value;

	// Unpin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-setPrevUsedPage\n\n");
	return RC_OK;
} 

PageNumber getPrevUsedPage(RM_TableData *rel, PageNumber reservedPageNum) {
	printf("Beginning-getPrevUsedPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin the reserved page
	result = -127;
	result = pinPage(BM, page, reservedPageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) page->data;
	int prevReservedPage = intp[2];

	// pin the reserved page
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-getPrevUsedPage\n\n");
	return prevReservedPage;
} 

RC setPageInitialized(RM_TableData *rel, PageNumber pageNum, bool value) {
	printf("Beginning-setPageInitialized\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// Get target value
	char target;

	if (value == TRUE) {
		target = '1';
	} else if (value == FALSE) {
		target = 0;
	} else {
		return RC_INVALID_TARGET_VALUE;
	}

	// pin page 0 which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	*page->data = target;

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-setPageInitialized\n\n");
	return RC_OK;
} 

bool isPageInitialized(RM_TableData *rel, PageNumber pageNum) {
	printf("Beginning-isPageInitialized\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin page 0 which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	char value = *page->data;

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	bool isInited;

	if (value == '1') {
		isInited = TRUE;
	} else if (value == 0) {
		isInited = FALSE;
	} else {
		return -1;
	}

	free(page);

	printf("END-isPageInitialized\n\n");
	return isInited;
} 

RC setCurrentRecords(RM_TableData *rel, PageNumber pageNum, int value) {
	printf("Beginning-setCurrentRecords\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin page 0 which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	int *intp = (int *) (page->data + 1);
	intp[0] = value;

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-setCurrentRecords\n\n");
	return RC_OK;
} 

int getCurrentRecords(RM_TableData *rel, PageNumber pageNum) {
	printf("Beginning-getCurrentRecords\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	int *intp = (int *) (page->data + 1);
	int currentRecords = intp[0];

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-getCurrentRecords\n\n");
	return currentRecords;
} 

bool isSlotOccupied(RM_TableData *rel, PageNumber pageNum, int slotId) {
	printf("Beginning-isSlotOccupied\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// pin page 0 which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	char *cp = page->data + sizeof(char) + sizeof(int) + sizeof(char) * slotId;
	char value = *cp;

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	bool isOccupied;

	if (value == '1') {
		isOccupied = TRUE;
	} else if (value == 0) {
		isOccupied = FALSE;
	} else {
		return -1;
	}

	free(page);

	printf("END-isSlotOccupied\n\n");
	return isOccupied;
}

RC setSlotOccupied(RM_TableData *rel, PageNumber pageNum, int slotId, bool value) {
	printf("Beginning-setSlotOccupied\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// Get target value
	char target;

	if (value == TRUE) {
		target = '1';
	} else if (value == FALSE) {
		target = 0;
	} else {
		return RC_INVALID_TARGET_VALUE;
	}

	// pin page 0 which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	char *cp = page->data + sizeof(char) + sizeof(int) + sizeof(char) * slotId;
	*cp = target;

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-setSlotOccupied\n\n");
	return RC_OK;
}

int getFreeSlotId(RM_TableData *rel, PageNumber pageNum) {
	printf("Beginning-getFreeSlotId\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	int pageMaxRecords = MaxRecordsInPage(rel);

	if (pageMaxRecords < 0) {
		return pageMaxRecords;
	}

	// pin page with pageNum which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	char *cp = page->data + sizeof(char) + sizeof(int);

	int i = 0;
	while (i < pageMaxRecords && *(cp + i * sizeof(char)) != 0) {
		i++;
	}

	int slotId;

	if (i < pageMaxRecords && *(cp + i * sizeof(char)) == 0) {
		slotId = i;
	} else if (i == pageMaxRecords) {
		return RC_PAGE_FULL_ERROR * (-1);
	}

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-getFreeSlotId\n\n");
	return slotId;
}

RC checkRecordLength(RM_TableData *rel, char *recordData) {
	printf("Beginning-checkRecordLength\n");
	int recordLength = getLengthOfRecord(rel);

	if (recordLength < 0) {
		return recordLength;
	}

	if (recordLength != strlen(recordData)) {
		return RC_CHECK_RECORD_LENGTH_ERROR;
	}

	printf("END-checkRecordLength\n\n");
	return RC_OK;
}


RC initRecordPage(RM_TableData *rel, PageNumber pageNum) {
	printf("Beginning-initRecordPage\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	// Set isPageInitialized to TRUE
	result = -127;
	result = setPageInitialized(rel, pageNum, TRUE);

	if (result != RC_OK) {
		return result;
	}

	// Set currentRecords to 0
	result = -127;
	result = setCurrentRecords(rel, pageNum, 0);

	if (result != RC_OK) {
		return result;
	}

	int pageMaxRecords = MaxRecordsInPage(rel);

	// pin page with pageNum which stores the information of input table
	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result * (-1);
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	char *cp = page->data + sizeof(char) + sizeof(int);

	memset(cp, 0, pageMaxRecords);

	// release the authority of handling page 0
	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result * (-1);
	}

	free(page);

	printf("END-initRecordPage\n\n");
	return RC_OK;
} 


RC insertRecord(RM_TableData *rel, Record *record) {
	printf("Beginning-insertRecord\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result; // return code

	int slotId;
	int spaceOffset;

	int recordLength = getLengthOfRecord(rel);

	if (recordLength < 0) {
		return recordLength;
	}

	int pageMaxRecords = MaxRecordsInPage(rel);

	if (pageMaxRecords < 0) {
		return pageMaxRecords;
	}

	int totalRecords = TotalLengthofRecords(rel);

	if (totalRecords < 0) {
		return totalRecords;
	}

	// Search the first page with free space
	PageNumber pageNum = searchFirstFreePage(rel);

	// The record page with this pageNum must not be full,
	// that is, if this record page is initialized, then currentRecords < pageMaxRecords
	// else if this page is not, then initialize it and set currentRecords to 0
	if (pageNum < 0) {
		return pageNum;
	}

	// Check if this record page is initialized
	bool isInited = isPageInitialized(rel, pageNum);

	// if is is not initialized, then initialize it and set slotId to 0
	// else if it is, then simply get slotId
	// else, some error happens, return result
	if (isInited < 0) {
		return isInited;
	} else if (isInited == FALSE) {
		result = -127;
		result = initRecordPage(rel, pageNum);

		if (result != RC_OK) {
			return result;
		}

		slotId = 0;

		// increment totalPages by one
		int totalPages = getTotalPages(rel);

		if (totalPages < 0) {
			return totalPages;
		}

		result = -127;
		result = setTotalPages(rel, ++totalPages);

		if (result != RC_OK) {
			return result;
		}

	} else if (isInited == TRUE) {
		slotId = getFreeSlotId(rel, pageNum);

		if (slotId < 0) {
			return slotId;
		}
	}

	// Get spaceOffset by slotId
	spaceOffset = PAGE_SIZE - (slotId + 1) * recordLength * sizeof(char);

	result = -127;
	result = pinPage(BM, page, pageNum);

	if (result != RC_OK) {
		return result;
	}

	// Get the free space position and insert record
	char *cp = page->data + spaceOffset;

	memcpy(cp, record->data, recordLength);

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	// Get currentRecords
	int currentRecords = getCurrentRecords(rel, pageNum);

	if (currentRecords < 0) {
		return currentRecords;
	}

	// Increment currentRecords by one and then set it
	result = -127;
	result = setCurrentRecords(rel, pageNum, ++currentRecords);

	if (result != RC_OK) {
		return result;
	}

	// Set the isOccupied flag of the slot to TRUE
	result = -127;
	result = setSlotOccupied(rel, pageNum, slotId, TRUE);

	if (result != RC_OK) {
		return result;
	}

	// increment totalRecords in the table info page by one
	result = -127;
	result = setLengthofRecords(rel, ++totalRecords);

	if (result != RC_OK) {
		return result;
	}

	// No need to check the return value
	PageNumber reservedPageNum = UsedPageNum(pageNum);

	// if now the currentRecords = pageMaxRecords, then the page is full, set the isFree flag in the reserved page to 0
	if (currentRecords == pageMaxRecords) {
		result = -127;
		result = WriteUsedPage(rel, reservedPageNum, pageNum, FALSE);

		if (result != RC_OK) {
			return result;
		}
	}

	// Set the record's RID
	record->id.page = pageNum;
	record->id.slot = slotId;

	free(page);

	printf("END-insertRecord\n\n");
	return RC_OK;
}


RC deleteRecord(RM_TableData *rel, RID id) {
	printf("Beginning-deleteRecord\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = ValidRID(rel, id);

	if (result != RC_OK) {
		return result;
	}

	// The RID is valid. Now remove the content stored in RID

	// Get recordLength
	int recordLength = getLengthOfRecord(rel);

	if (recordLength < 0) {
		return recordLength;
	}

	result = -127;
	result = pinPage(BM, page, id.page);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	// Set recordOffset
	int spaceOffset = PAGE_SIZE - sizeof(char) * recordLength * (id.slot + 1);

	// Delete the record by set all bytes to 0
	char *cp = page->data + spaceOffset;
	memset(cp, 0, recordLength);

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	// Decrement currentRecords stored in id.page
	int currentRecords = getCurrentRecords(rel, id.page);

	if (currentRecords < 0) {
		return currentRecords;
	}

	result = -127;
	result = setCurrentRecords(rel, id.page, --currentRecords);

	if (result != RC_OK) {
		return result;
	}

	// Now the currentRecords is the new value

	// Set isOccupied to FALSE
	result = -127;
	result = setSlotOccupied(rel, id.page, id.slot, FALSE);

	if (result != RC_OK) {
		return result;
	}

	// Decrement the totalRecords stored in page 0
	int totalRecords = TotalLengthofRecords(rel);

	if (totalRecords < 0) {
		return totalRecords;
	}

	result = -127;
	result = setLengthofRecords(rel, --totalRecords);

	if (result != RC_OK) {
		return result;
	}

	// Now the totalRecords is the new value

	// No need to check the return value for this function
	PageNumber reservedPageNum = UsedPageNum(id.page);

	// Get the pageMaxRecords
	int pageMaxRecords = MaxRecordsInPage(rel);

	if (pageMaxRecords < 0) {
		return pageMaxRecords;
	}

	// if now the currentRecords = pageMaxRecords - 1, then the page becomes free
	// update the isFree flag in the reserved page to TRUE
	if (currentRecords == pageMaxRecords - 1) {
		result = -127;
		result = WriteUsedPage(rel, reservedPageNum, id.page, TRUE);

		if (result != RC_OK) {
			return result;
		}
	}

	free(page);

	printf("END- deleteRecord\n\n");
	return RC_OK;
}


RC updateRecord(RM_TableData *rel, Record *record) {
	printf("Beginning-updateRecord\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	bool isOccupied = isSlotOccupied(rel, record->id.page, record->id.slot);

	if (isOccupied < 0) {
		return isOccupied;
	} else if (isOccupied == FALSE) {
		return RC_SLOT_EMPTY;
	} else if (isOccupied != TRUE) {
		return RC_SLOT_ERROR;
	}

	result = -127;
	result = pinPage(BM, page, record->id.page);

	if (result != RC_OK) {
		return result;
	}

	result = -127;
	result = markDirty(BM, page);

	if (result != RC_OK) {
		return result;
	}

	int recordLength = getLengthOfRecord(rel);

	if (recordLength < 0) {
		return recordLength;
	}

	// Set the offset
	int spaceOffset = PAGE_SIZE
			- (record->id.slot + 1) * recordLength * sizeof(char);

	char *cp = page->data + spaceOffset;

	// write the data into page
	memcpy(cp, record->data, recordLength);

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	free(page);

	printf("END-updateRecord\n\n");
	return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
	printf("Beginning-getRecord\n");
	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RC result;

	result = -127;
	result = ValidRID(rel, id);

	if (result != RC_OK) {
		return result;
	}

	// check if the slot is occupied or not
	bool isOccupied = isSlotOccupied(rel, id.page, id.slot);

	// if not occupied, return RC_NON_EXISTING_RECORD

	if (isOccupied < 0) {
		return isOccupied;
	} else if (isOccupied == FALSE) {
		return RC_SLOT_EMPTY;
	} else if (isOccupied != TRUE) {
		return RC_SLOT_ERROR;
	}

	// declare integers to store RECORD_LENGTH, PAGE_MAX_RECORDS, TOTAL_PAGES
	int recordLength = getLengthOfRecord(rel);

	if (recordLength < 0) {
		return recordLength;
	}

	// start place of the record we need
	int spaceOffset = PAGE_SIZE - (id.slot + 1) * recordLength * sizeof(char);

	result = -127;
	result = pinPage(BM, page, id.page);

	if (result != RC_OK) {
		return result;
	}

	char *cp = page->data + spaceOffset;

	// get this record at the slot
	// notice, we record our records from the bottom of a page to the top
	memcpy(record->data, cp, recordLength);

	// add a \0 at the end of cp
	record->data[recordLength] = '\0';

	result = -127;
	result = unpinPage(BM, page);

	if (result != RC_OK) {
		return result;
	}

	//if there is a record, let record's id = id passed in
	record->id.page = id.page;
	record->id.slot = id.slot;

	free(page);

	printf("END-getRecord\n\n");
	return RC_OK;
} 

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	printf("Beginning-startScan\n");

	// allocate memory for RM_ScanCondition
	RM_ScanCondition *sc = (RM_ScanCondition *) malloc(
			sizeof(RM_ScanCondition));
	sc->id = (RID *) malloc(sizeof(RID));
	sc->cond = cond;
	sc->id->page = PAGE_RESERVEDFIRST + 1;
	sc->id->slot = 0;

	scan->rel = rel;
	scan->mgmtData = sc;

	printf("END-startScan\n\n");
	return RC_OK;
} 

RC next(RM_ScanHandle *scan, Record *record) {
	printf("Beginning-next\n");
	RC result;

	RM_ScanCondition *sc = (RM_ScanCondition *) scan->mgmtData;

	if (sc->id == NULL) {
		return RC_NO_RID;
	}

	int totalPages = getTotalPages(scan->rel);

	if (totalPages < 0) {
		return totalPages;
	}

	int totalRecords = TotalLengthofRecords(scan->rel);

	if (totalRecords < 0) {
		return totalRecords;
	}

	int pageMaxRecords = MaxRecordsInPage(scan->rel);

	if (pageMaxRecords < 0) {
		return pageMaxRecords;
	}

	int dataPages = PAGE_CAPACITY + PAGE_RESERVEDFIRST;

	// if to the end ,return
	div_t d = div(sc->id->page, dataPages);

	int curRecordPos = d.quot
			* (PAGE_CAPACITY + d.rem - PAGE_RESERVEDFIRST - 1)
			* pageMaxRecords + (sc->id->slot + 1);

	if (curRecordPos == totalRecords) {
		// set RECORD back to the first record position
		sc->id->page = PAGE_RESERVEDFIRST + 1;
		sc->id->slot = -1;

		return RC_RM_NO_MORE_TUPLES;
	}

	if (sc->id->slot == (pageMaxRecords - 1)) {
		if (sc->id->page == totalPages - 1) {
			// set RECORD back to the first record position
			sc->id->page = PAGE_RESERVEDFIRST + 1;
			sc->id->slot = -1;

			return RC_RM_NO_MORE_TUPLES;
		}

		curRecordPos++;

		// set to the first slot in the next page
		sc->id->page++;
		sc->id->slot = 0;
	} else {
		curRecordPos++;

		// set to the next slot in this page
		sc->id->slot++;
	}

	// declare an array of Value to store the expression evaluated oc
	// the first Value in the array is a pointer to a boolean Value
	Value *isFound = (Value *) malloc(sizeof(Value));
	isFound->v.boolV = FALSE;
	isFound->dt = DT_BOOL;

	Value **oc = (Value **) malloc(sizeof(Value *));
	*oc = isFound;

	// a nest while loop, that starts from rid till the end of file
	// or the wanted record is found
	// the outer loop is for pages
	while (curRecordPos < (totalRecords + 1) && sc->id->page < totalPages
			&& (*oc)->v.boolV == FALSE) {
		// search this page only if it's not a reserved page
		if ((sc->id->page % dataPages) != PAGE_RESERVEDFIRST
				&& sc->id->slot != (pageMaxRecords - 1)) {
			// inner loop for slots on one page
			while (curRecordPos < (totalRecords + 1)
					&& sc->id->slot < pageMaxRecords
					&& (*oc)->v.boolV == FALSE) {
				// check is the current slot is occupied
				// we check if the record in this slot only if it's occupied
				bool isOccupied = isSlotOccupied(scan->rel, sc->id->page,
						sc->id->slot);

				if (isOccupied < 0) {
					return isOccupied;
				} else if (isOccupied == TRUE) {
					// get the record at rid
					result = -127;
					result = getRecord(scan->rel, *(sc->id), record);

					if (result != RC_OK) {
						return result;
					}

					// evaluate this record with the given schema, and update isFound
					// if something wrong happened, return result, set isFound = FALSE
					result = -127;
					result = evalExpr(record, scan->rel->schema, sc->cond, oc);

					if (result != RC_OK) {
						return result;
					}

					// check isFound still a boolean, if not, set it back to boolean and FALSE again
					// if ((*oc)->dt != DT_BOOL) {
					if ((*oc)->v.boolV == FALSE) {
						free(*oc);
						*oc = isFound;
					}
				} // end of if "is it occupied"

				// increase slot number
				if ((*oc)->v.boolV == FALSE) {
					curRecordPos++;
					sc->id->slot++;
				}

			} 
		}

		//increase page number and reset slot number
		if ((*oc)->v.boolV == FALSE) {
			curRecordPos++;
			sc->id->page++;
			sc->id->slot = 0;
		}
	} 

	if ((*oc)->v.boolV == TRUE) {
		result = RC_OK;
	} else if (curRecordPos == totalRecords || sc->id->page == totalPages) {
		sc->id->page = record->id.page;
		sc->id->slot = record->id.slot;
		result = RC_RM_NO_MORE_TUPLES;
	}

	free(isFound);
	free(oc);

	printf("END- next\n\n");
	return result;
} 

RC closeScan(RM_ScanHandle *scan) {
	printf("Beginning-closeScan\n");
	RM_ScanCondition *sc = (RM_ScanCondition *) scan->mgmtData;
	free(sc->id);
	free(sc);

	scan->mgmtData = NULL;
	scan->rel = NULL;

	printf("END-closeScan\n\n");
	return RC_OK;
}

// dealing with schemas

int getRecordSize(Schema *schema) {
	printf("Beginning-getRecordSize\n");
	int recordSize = 0;

	// loop to calculate the record size
	int i;
	for (i = 0; i < schema->numAttr; i++) {
		if (schema->dataTypes[i] == DT_INT) {
			schema->typeLength[i] = sizeof(int);
		} else if (schema->dataTypes[i] == DT_FLOAT) {
			schema->typeLength[i] = sizeof(float);
		} else if (schema->dataTypes[i] == DT_BOOL) {
			schema->typeLength[i] = sizeof(bool);
		} else if (schema->dataTypes[i] != DT_STRING) {
			return RC_WRONG_DATATYPE * (-1);
		}

		recordSize += schema->typeLength[i];
	}

	printf("END-getRecordSize\n\n");
	return recordSize;
} 

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength,int keySize, int *keys) {
	printf("Beginning-createSchema\n");

	// if the global variables are not allocated to memory, allocate first
	if (SCHEMA == NULL) {
		SCHEMA = (Schema *) malloc(sizeof(Schema));
	}

	// write values into the global variable SCHEMA
	SCHEMA->numAttr = numAttr;
	SCHEMA->attrNames = attrNames;
	SCHEMA->dataTypes = dataTypes;
	SCHEMA->typeLength = typeLength;
	SCHEMA->keyAttrs = keys;
	SCHEMA->keySize = keySize;

	// function schemaToString() will allocate memory for the return string
	// We should free
	char *oc = NULL;
	oc = schemaToString(SCHEMA);

	// calculate the length of schema_to_write
	int schemaLength = strlen(oc);

	// check if the length is small enough. if too long, it can't be written into
	// one page, unset SCHEMA, then return NULL.
	// In table info page, there are RECORD_LENGTH, TOTAL_RECORDS,
	// TOTAL_PAGES, PAGE_MAX_RECORDS and SCHEMA_STR.
	if (schemaLength > (PAGE_SIZE - sizeof(int) * 4)) {
		memset(SCHEMA_STR, 0, PAGE_SIZE);

		SCHEMA->numAttr = -1;
		SCHEMA->attrNames = NULL;
		SCHEMA->dataTypes = NULL;
		SCHEMA->typeLength = NULL;
		SCHEMA->keyAttrs = NULL;
		SCHEMA->keySize = -1;

		free(oc);

		return NULL;
	} else if (SCHEMA_STR == NULL) {
		SCHEMA_STR = (char *) calloc(PAGE_SIZE, sizeof(char));
	}

	memcpy(SCHEMA_STR, oc, schemaLength);

	free(oc);

	//calculate the length of a record of SCHEMA
	RECORD_LENGTH = getRecordSize(SCHEMA);

	// calculate the PAGE_MAX_RECORDS
	// On each record page, there are the following in the head
	// 1. isInitialized (1 char)
	// 2. current record number (1 int)
	// 3. isOccupied (1 char for each record)
	PAGE_MAX_RECORDS = (PAGE_SIZE - sizeof(char) - sizeof(int))
			/ (sizeof(char) + RECORD_LENGTH);

	printf("END- createSchema\n\n");
	return SCHEMA;
}

RC freeSchema(Schema *schema) {
	printf("Beginning-freeSchema\n");

	schema->numAttr = -1;
	schema->attrNames = NULL;
	schema->dataTypes = NULL;
	schema->typeLength = NULL;
	schema->keyAttrs = NULL;
	schema->keySize = -1;

	printf("END-freeSchema\n\n");
	return RC_OK;
} 

char *schemaToString(Schema * schema) {
	printf("Beginning-schemaToString\n");
	VarString *schemaStr; // a VarString variable schema_str to record the Schema

	// I will use the VarString structure and its functions
	// "|" between variables of this Schema, "," between attributes for each variable
	MAKE_VARSTRING(schemaStr); 	// make it;
	APPEND(schemaStr, "%d%c", schema->numAttr, '|'); // add numAttr and a '|' between variables

	int i;
	// use a for loop to add attrName
	for (i = 0; i < schema->numAttr; i++) {
		// ',' between attributes and '|' at last
		APPEND(schemaStr, "%s%c", schema->attrNames[i],
				(i != schema->numAttr - 1) ? ',' : '|');
	}

	// similar for dataTypes and typeLength
	for (i = 0; i < schema->numAttr; i++) {
		// ',' between attributes and '|' at last
		APPEND(schemaStr, "%d%c", schema->dataTypes[i],
				(i != schema->numAttr - 1) ? ',' : '|');
	}

	for (i = 0; i < schema->numAttr; i++) {
		// ',' between attributes and '|' at last
		APPEND(schemaStr, "%d%c", schema->typeLength[i],
				(i != schema->numAttr - 1) ? ',' : '|');
	}

	// we use a for loop to add key attributes
	for (i = 0; i < schema->keySize; i++) {
		APPEND(schemaStr, "%d%c", schema->keyAttrs[i],
				(i != schema->keySize - 1) ? ',' : '|');
	}

	// add keySize and a '|', and \0 that indicates end
	APPEND(schemaStr, "%d%c%c", schema->keySize, '|', '\0');

	printf("END- schemaToString\n\n");
	RETURN_STRING(schemaStr); // return
} 

// dealing with records and attribute values

RC createRecord(Record **record, Schema *schema) {
	printf("Beginning-createRecord\n");

	Record *r = (Record *) malloc(sizeof(Record));
	char *rData = (char *) calloc(RECORD_LENGTH + 1, sizeof(char));

	r->id.page = -1;
	r->id.slot = -1;
	r->data = rData;
	record[0] = r;

	printf("END- createRecord\n\n");
	return RC_OK;
} 


RC freeRecord(Record *record) {
	printf("Beginning-freeRecord\n");
	// what should this function do?

	free(record->data);
	record->data = NULL;

	free(record);
	record = NULL;

	printf("END-freeRecord\n\n");
	return RC_OK;
} 


RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
	printf("Beginning-getAttr\n");
	// calculate the offset
	int attrOffset = 0;

	// Allocate memory for attrValue
	// Free it outside this function

	Value *attrValue = (Value *) malloc(sizeof(Value));

	// save the attrNumth attribute's DataType to ATTR_VALUE
	attrValue->dt = schema->dataTypes[attrNum];

	// loop to add the length of each attribute before the target into offset
	int i;
	for (i = 0; i < attrNum; i++) {
		attrOffset += schema->typeLength[i];
	}

	// save the attrNumth attribute to ATTR_VALUE using memcpy
	if (schema->dataTypes[attrNum] == DT_STRING) {
		int tl = schema->typeLength[attrNum];

		attrValue->v.stringV = (char *) calloc(tl + 1, sizeof(char));

		memcpy(attrValue->v.stringV, record->data + attrOffset,
				schema->typeLength[attrNum]);
		attrValue->v.stringV[tl + 1] = '\0';
	} else if (schema->dataTypes[attrNum] == DT_INT) {
		memcpy(&(attrValue->v.intV), record->data + attrOffset,
				schema->typeLength[attrNum]);
	} else if (schema->dataTypes[attrNum] == DT_FLOAT) {
		memcpy(&(attrValue->v.floatV), record->data + attrOffset,
				schema->typeLength[attrNum]);
	} else if (schema->dataTypes[attrNum] == DT_BOOL) {
		memcpy(&(attrValue->v.boolV), record->data + attrOffset,
				schema->typeLength[attrNum]);
	}

	// let value point to the global variable
	*value = attrValue;

	printf("END-getAttr\n\n");
	return RC_OK;
} 


RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
	printf("Beginning-setAttr\n");
	// calculate the offset
	int attrOffset = 0;

	// check if passed in value's dt is the same with the attribute of schema
	// if dataType not the same, return RC_WRONG_DATATYPE
	if (value->dt != schema->dataTypes[attrNum]) {
		return RC_WRONG_DATATYPE;
	}

	// loop to add the length of each attribute before the target into offset
	int i;
	for (i = 0; i < attrNum; i++) {
		attrOffset += schema->typeLength[i];
	}

	// save value[attrNum] to the attrNumth attribute using memcpy
	if (value->dt == DT_STRING) {
		memcpy(record->data + attrOffset, value->v.stringV,
				schema->typeLength[attrNum]);
	} else if (value->dt == DT_BOOL) {
		memcpy(record->data + attrOffset, &(value->v.boolV),
				schema->typeLength[attrNum]);
	} else if (value->dt == DT_INT) {
		memcpy(record->data + attrOffset, &(value->v.floatV),
				schema->typeLength[attrNum]);
	} else if (value->dt == DT_FLOAT) {
		memcpy(record->data + attrOffset, &(value->v.intV),
				schema->typeLength[attrNum]);
	}

	printf("END-setAttr\n\n");
	return RC_OK;
}

/*Condition Updates*/
RC isEqualExpr(Expr *cond) {
	RC result = -127;

	//if ExprType is not operater return error message
	if (cond->type != EXPR_OP) {
		return RC_NOT_EQUALS_EXPR;
	}

	//then check if the Operator type is equals
	//if not, return error message
	if (cond->expr.op->type != OP_COMP_EQUAL) {
		return RC_NOT_EQUALS_EXPR;
	}

	//check if the left part of the operator is attribute
	//if not, return RC_LEFT_NOT_ATTR
	if (cond->expr.op->args[0]->type != EXPR_ATTRREF) {
		return RC_LEFT_NOT_ATTR;
	}

	//then check if the right part of the operator is a constant
	//if not, return RC_RIGHT_NOT_CONS
	if (cond->expr.op->args[1]->type != EXPR_CONST) {
		return RC_RIGHT_NOT_CONS;
	}

	return RC_OK;
}

RC condUpdateRecord(RM_ScanHandle *scan, Expr *setCond) {
	printf("Beginning-condUpdateRecord\n");
	RC result;

	// check if the setCond is "attr = cons"
	result = -127;
	result = isEqualExpr(setCond);

	if (result != RC_OK) {
		return result;
	}

	//check if the setCond's attrRef is a valid number
	int numAttr = scan->rel->schema->numAttr;
	int attrRef = setCond->expr.op->args[0]->expr.attrRef;

	if (attrRef < 0 || attrRef >= numAttr) {
		return RC_INVALID_ATTRREF;
	}
	//check if the attribute type is the valid
	DataType dtAttr = scan->rel->schema->dataTypes[attrRef];
	DataType dtExpr = setCond->expr.op->args[1]->expr.cons->dt;

	if (dtAttr != dtExpr) {
		return RC_INVALID_EXPR_CONS_DATATYPE;
	}

	// search for the record
	// declare a Record and a Value variable
	Record *record;
	result = createRecord(&record, scan->rel->schema);
	if (result != RC_OK) {
		return result;
	}

	//oc equals to the constant in the set condition
	Value *oc = (Value *) malloc(sizeof(Value));
	oc = setCond->expr.op->args[1]->expr.cons;

	//use while loop to find each eligible record, pass in the record we just declared
	while ((result = next(scan, record)) == RC_OK) {
		//after next, record should point to the record we want to find
		//set this record with the given expression
		result = setAttr(record, scan->rel->schema, attrRef, oc);
		if (result != RC_OK) {
			return result;
		}

		//update the slot with this set record
		result = updateRecord(scan->rel, record);
		if (result != RC_OK) {
			return result;
		}

	}

	if (result != RC_RM_NO_MORE_TUPLES) {
		return result;
	}

	//free record and oc;
	free(oc);

	printf("END-conditionalUpdateRecord\n\n");
	return freeRecord(record);
} 

