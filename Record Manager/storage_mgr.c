#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "storage_mgr.h"
#include "dberror.h"


void initStorageManager (void){
	printf("Initializing StorageManager...\n");
}

//Page functions
/*Create a Page file and write \0 bytes as content*/
RC createPageFile (char *fileName){
	//set up a file pointer *pagefile, open a file with mode "w+"
	//"w+" open a text file for update, If same name file exists, erased all its content
	//or create the file if it doesn't exist 
	FILE *pagefile = fopen(fileName, "w+");
	//check pagefile is exists
	if (pagefile==NULL){
		return RC_FILE_NOT_FOUND;
	}
	//allocate two pages, the first contains the total number of pages
	//which is easily to use in openPageFile()
	//the second is the empty page
	char *numPages_str = (char *) calloc (PAGE_SIZE, sizeof(char));
	SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
	//initialize the numPages_str = 1
	strcat(numPages_str, "1\n");
	//write to the file
	fwrite(numPages_str, sizeof(char), PAGE_SIZE, pagefile);
	fwrite(emptyPage, sizeof(char), PAGE_SIZE, pagefile);
	//deallocate memory
	free(numPages_str);
	free(emptyPage);
	//close file
	fclose(pagefile);
	return RC_OK;
}

//Open the created Page file and store relative info to fHandle
RC openPageFile(char *fileName, SM_FileHandle *fHandle){
	//open for reading & writing
	FILE *pagefile = fopen(fileName, "r+");
	if(pagefile==NULL){
		return RC_FILE_NOT_FOUND;
	}
	//allocate a block for total number of pages where we can get from the "first" page
	char *str = (char *) calloc(PAGE_SIZE, sizeof(char));
	//get the number of pages into str
	fgets(str, PAGE_SIZE, pagefile); 
	//remove newline char
	str = strtok(str, "\n"); 
	//assign the file value to fHandle
	fHandle->fileName = fileName;
	fHandle->totalNumPages = atoi(str);//change the str to integer
	fHandle->curPagePos=0;
	fHandle->mgmtInfo=pagefile;
	//deallocate memory
	free(str);
	return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle){
	int isclose=fclose(fHandle->mgmtInfo);
	if(isclose==0){
		return RC_OK;	
	}
	else{
		return RC_FILE_NOT_FOUND;
	}
}

RC destroyPageFile(char *fileName){
	int isremove = remove(fileName);
	if(isremove==0){
		return RC_OK;		
	}
	else{
		return RC_FILE_NOT_FOUND;
	}
}

//read functions
//the method reads the pageNum-th block from a file and stores its content 
//in memPage
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	/*check if file is open*/
	if (fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;	
	}
	/*check valid pageNum.*/
	if (pageNum > fHandle->totalNumPages || pageNum < 0){
		return RC_NON_EXISTING_PAGE;
	}
	//calculate offset from the file head
	int offset = PAGE_SIZE*sizeof(char)*(pageNum+1);
	//set the file position to the pageNumth block
	int isfound = fseek(fHandle->mgmtInfo, offset, SEEK_SET); 
	if(isfound == 0){
		fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
		
		//set current page to the given pageNum
		fHandle->curPagePos = pageNum;
		return RC_OK;	
	}
	else{
		return RC_NON_EXISTING_PAGE;
	}
}

int getBlockPos(SM_FileHandle *fHandle){
	return fHandle->curPagePos;//return the current page position 
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	return readBlock(0, fHandle, memPage);//set pageNum=1 get the first page's position
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	return readBlock(fHandle->totalNumPages, fHandle, memPage);//set pageNum=totalnumpages to get the previous page position
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	return readBlock(fHandle->curPagePos-1, fHandle, memPage);//set pageNum=curPagePos-1 to get the previous page position
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	return readBlock(fHandle->curPagePos, fHandle, memPage);//set pageNum=curPagePos to get the previous page position
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	return readBlock(fHandle->curPagePos+1, fHandle, memPage);//set pageNum=curPagePos-1 to get the previous page position
}

//write functions
//this method write one page to disk using absolute position
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	/*check if file is open*/
	if (fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;	
	}
	/*check valid pageNum.*/
	if(pageNum > fHandle->totalNumPages || pageNum<0){
		return RC_WRITE_FILE_FAILED;
	}
	int offset = PAGE_SIZE*sizeof(char)*(pageNum+1);
	/*set the file position to the pageNumth block*/
	int isfound = fseek(fHandle->mgmtInfo, offset, SEEK_SET);
	if(isfound==0){
		fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
		
		//set current position to pageNum
		fHandle->curPagePos = pageNum;
		return RC_OK;	
	}
	else{
		return RC_WRITE_FILE_FAILED;
	}
}
//write one page to the file of current position
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}
//Increase the totalNumPages by 1 and the new last page contains all zero
RC appendEmptyBlock(SM_FileHandle *fHandle){
	//check the file is open
	FILE *pagefile = fHandle->mgmtInfo;
	if(pagefile==NULL){
		return RC_FILE_NOT_FOUND;
	}
	/*initial page allocation*/
	SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
	//point to the last page
	int isfound = fseek(fHandle->mgmtInfo, 0L, SEEK_END);
	if (isfound == 0){
		//writes data after the last page
		fwrite(emptyPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
		
		fHandle->curPagePos = fHandle->totalNumPages++;
		//set the position back to start
		rewind(fHandle->mgmtInfo);
		//increment the first page's total number of pages' string
		fprintf(fHandle->mgmtInfo, "%d\n", fHandle->totalNumPages);
		//move pointer back to the last page
		fseek(fHandle->mgmtInfo, 0L, SEEK_END); 
		//deallocate memory
		free(emptyPage);
		return RC_OK;
	}
	else{
		free(emptyPage);
		return RC_WRITE_FILE_FAILED;
	}
}

//If the file has less than numberOfPages then increase the size to numberOfPages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle){
	if (fHandle->totalNumPages < numberOfPages){
		int numPages = numberOfPages-fHandle->totalNumPages;
		for(int i = 0; i<numPages; i++){
			appendEmptyBlock(fHandle);
		}
	}
	return RC_OK;
}







