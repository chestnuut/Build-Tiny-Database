#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#define MAX_PAGES 20000
#define MAX_FRAMES 200
#define MAX_K 10
/*Frame list operations*/

/* Auxiliary function */
/*
	Create a new pageframe node in frame list		
*/
pageFrame *createNewFrame(){
    //allocate memory for the pageFrame entry
    pageFrame *pf = malloc(sizeof(pageFrame));
    //initialize the BM_PageHandle content
    pf->page = MAKE_PAGE_HANDLE();
    pf->page->data = calloc(PAGE_SIZE, sizeof(SM_PageHandle));
    pf->page->pageNum = NO_PAGE;
    //initialize pageFrame's attribute
    pf->next = NULL;
    pf->previous = NULL;
    pf->frameNum = 0;
    pf->dirtyFlag = FALSE;
    pf->fixCount = 0;
    pf->rf = 0;
	pf->pageFrequency = 0;
    //return this node
    return pf;
}

/* makeHead() is to make the given pf the head of the list.*/
void makeHead(pageList **plist, pageFrame *NodeToMake){
    //connect the list head with the pageFrame's head node
    pageFrame *headFrame = (*plist)->head;
    //if node already in head or head is null or the node is null
    //Simply return.
    if(headFrame == NULL || NodeToMake == NULL || 
    	NodeToMake == (*plist)->head){return;}
    //else if this node is tail node
    //exchange the head and the tail
    else if(NodeToMake == (*plist)->tail){
        pageFrame *temp = ((*plist)->tail)->previous;
        temp->next = NULL;
        (*plist)->tail = temp;
    }
    //if code runs here, this node is in the middle of the list
    //remove its link and change its next node's previous pointer to 
    //its previous node and its previous node's next pointer to its next node 
    else{
        NodeToMake->previous->next = NodeToMake->next;
        NodeToMake->next->previous = NodeToMake->previous;
    }
    //append this node to the head frame
    NodeToMake->next = headFrame;
    headFrame->previous = NodeToMake;
    NodeToMake->previous = NULL;
    //change the head pointer point to the new node
    (*plist)->head = NodeToMake;
    (*plist)->head->previous = NULL;
    return;
}

/* searchByPageNum() search the pageList and 
	return a pointer to the pagefame given the pageNum */
pageFrame *searchByPageNum(pageList *plist, const PageNumber pageNum){
	//define a current pointer first pointing to head,
	//if isfound, return this pageframe
	//if not, return NULL
    for(pageFrame *current = plist->head; 
    	current != NULL; current = current->next){
    	if(current->page->pageNum == pageNum){ return current;}
    }
    return NULL;
}
/* inPageList() can tell whether a page in the list
	If yes, return a pointer to it and increase fixCount
	If no, return a NULL pointer 
*/
pageFrame *inPageList(BM_BufferPool *const bm, 
	BM_PageHandle *const page,const PageNumber pageNum){

    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    pageFrame *pftoFind = searchByPageNum(attribute->plists, pageNum);
    
    if((attribute->frameContent)[pageNum] != NO_PAGE){
        if((pftoFind) != NULL){
        	//give the client data of that page
        	page->pageNum = pageNum;
        	page->data = pftoFind->page->data;
        	//pinned, increase fixCount
        	pftoFind->fixCount++;
        	pftoFind->rf = 1;
        return pftoFind;       	
        }
        return NULL; 
    }
    return NULL;
}
/* exchangePageFrame() check pageframe's dirtyFlag, If is true, write back to the file
	and then increment writeNum
	then read the bm_pagehandle into memory, store in the pageFrame
	update all relative attribute */
RC exchangePageFrame(BM_BufferPool *const bm, BM_PageHandle *const page, 
	const PageNumber pageNum, pageFrame *toUpdate){
    
    SM_FileHandle fhandle;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    RC rc;
    //open pagefile to write the changes
    if ((rc = openPageFile ((char *)(bm->pageFile), &fhandle)) != RC_OK){
        return rc;
    }
    
    //If the frame to be replaced is dirty, write it back to the disk.
    if(toUpdate->dirtyFlag == TRUE){
    	//first, append enough pages to write
        if((rc = ensureCapacity(toUpdate->page->pageNum, 
        	&fhandle)) != RC_OK){return rc;}
        //write pages back to the file
        if((rc = writeBlock(toUpdate->page->pageNum,&fhandle, 
        	toUpdate->page->data)) != RC_OK){return rc;}
        attribute->numWrite ++;
    }
    
    // Update the frameContent array, set the replaceable page's pageNum to NO_PAGE.
    (attribute->frameContent)[toUpdate->page->pageNum] = NO_PAGE;
    // Read the page's data into toupdated pageframe
    if((rc = ensureCapacity(pageNum, &fhandle)) != RC_OK){return rc;}
    if((rc = readBlock(pageNum, &fhandle, 
    	toUpdate->page->data)) != RC_OK){return rc;}
    
    //set the page handler with new pageNum and data
    page->pageNum = pageNum;
    page->data = toUpdate->page->data;
    //increment total readNum
    (attribute->numRead)++;
    
    //update all the parameters of the new frame
    toUpdate->dirtyFlag = FALSE;
    toUpdate->fixCount = 1;
    toUpdate->page->pageNum = pageNum;
    toUpdate->rf = 1;
    //update the pagetoFrame[] and frametoPage[]
    (attribute->frameContent)[toUpdate->page->pageNum] = toUpdate->frameNum;
    (attribute->pageContent)[toUpdate->frameNum] = toUpdate->page->pageNum;
    //close the pagefile
    closePageFile(&fhandle);
    
    return RC_OK;
    
}
/* Page replacement strategies.*/
/* FIFO is First in First out Strategy
	1. call inPageList() to check whether the page is in the memory
			if True, return RC_OK
	2. Check if the List is full,
			if not, update the frame into the empty frame and update it as head
			if full, traverse the whole list to point to the oldest frame 
				with fixCount = 0, update isfound frame as head
	3. call exchangePageFrame()  */
RC FIFO (BM_BufferPool *const bm, BM_PageHandle *const page, 
	const PageNumber pageNum){
    pageFrame *isfound;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    printf("searching page: %d\n", pageNum);
    // Check if page already in memory. 
    isfound = inPageList(bm, page, pageNum);
    if(isfound != NULL){
    	printf("Page: %d found\n", pageNum);
    	return RC_OK;
    }
    printf("Page: %d not found\n", pageNum);
    // If not isfound, need to update the page into memory
    
    // If the pagelists in the memory are less than the 
    // total available pagelists, locate the first empty frame from the head.
    if(attribute->plists->length < bm->numPages){
    	printf("Ready to append Page: %d\n", pageNum);
        isfound = attribute->plists->head;
        for(int i = 0; i < attribute->plists->length; i++){
        	isfound = isfound->next;
        }
        //add pages to the pageLists, increment the size
        (attribute->plists->length)++;
        printf("Page: %d has append\n", pageNum);
        makeHead(&(attribute->plists), isfound);
    }
    else{
        //For new page, if the pagelists are filled, 
        //locate the oldest frame has fixCount = 0
        printf("Replace Page: %d\n", pageNum);
        isfound = attribute->plists->tail;
        while(isfound != NULL && isfound->fixCount != 0){
            isfound = isfound->previous;
        }
        
        if (isfound == NULL){
            return RC_TRAVERSE_TO_THE_END;
        }
        printf("Page: %d replaced\n", pageNum);
        makeHead(&(attribute->plists), isfound);
    }
 	//initialize return code
    RC rc;
    //exchange isfound frame with page requested
    if((rc = 
    	exchangePageFrame(bm, page, pageNum, isfound)) != RC_OK){return rc;}
    return RC_OK;
}
/*LRU strategy:
	1. call inPageList() to check whether the page is in memory
		if yes, return RC_OK;
	2.  check if the list is full:
			if not, update the page into empty page and make it as head of the list
			if fulll, traverse from tail to look for a frame with fixCount = 0
	3. Since the new comming frame always considered as head pageframe, 
		then the head pageframe is latest used pageframe. The tail should be the least 
		recently used pageframe. */ 
RC LRU (BM_BufferPool *const bm, BM_PageHandle *const page, 
	const PageNumber pageNum){
    printf("You are in LRU test\n");
    pageFrame *isfound;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    printf("searching page: %d\n", pageNum);
    //Check if page already in memory. 
    if((isfound = inPageList(bm, page, pageNum)) != NULL){
     	//When found, make this new pageframe as head since tthis is the latest
     	printf("Page: %d found\n", pageNum);
        makeHead(&(attribute->plists), isfound);
        return RC_OK;
    }
    printf("Page: %d not found\n", pageNum);
    // If the pagelists in the memory is less than the total available pagelists, 
    // locate the first empty frame from the head.
    if((attribute->plists->length) < bm->numPages){
    	printf("Ready to append Page: %d\n", pageNum);
        isfound = attribute->plists->head;
        for(int i=0; i< attribute->plists->length; i++){
        	isfound = isfound->next;
        }
        printf("Page: %d has append\n", pageNum);
        //add pages to the pageLists, increment the size
        attribute->plists->length ++;
    }
    else{
        //For new page, if the pagelists are filled, 
        //locate the oldest frame has fixCount = 0
        isfound = attribute->plists->tail;
        printf("Replace Page: %d\n", pageNum);
        while(isfound != NULL && isfound->fixCount != 0){
            isfound = isfound->previous;
        }
        printf("Page: %d replaced\n", pageNum);
        // Check whether traverse the whole list,
        // if True, then no pages available to update
        if (isfound == NULL){
            return RC_TRAVERSE_TO_THE_END;
        }
    }
    // Initialize return code
    RC rc;
    if((rc = 
    	exchangePageFrame(bm, page, pageNum, isfound)) != RC_OK){return rc;}
    
    //make the new pageframe as head 
    makeHead(&(attribute->plists), isfound);
    return RC_OK;
}
/* LRU_K() replacement strategy :
	1. same as FIFO and LRU, check whether the requested pageframe in memory
		or not, if not, then check if the list is full, If not full, 
		find empty pageframe and update.
	2. everytime a page has been pinned, store the pinCount into khist array.
	3. If the requested pageframe not in memory, traverse from the head pointer,
		find pageframe with fixCount = 0, 
	    and calculate the distance between current entry in khist and k-th 
	    entry in khist, replace the longest one.
	4. If no page is called k times, worked just as LRU.  	
*/
RC LRU_K (BM_BufferPool *const bm, BM_PageHandle *const page, 
	const PageNumber pageNum){
    pageFrame *isfound;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;   
    //initialize distance and maxdistance 
    int distance, maxDistance = -1;  
    int K = (uintptr_t)(attribute->stratData);
    attribute->countPinning++;
    
    // Check if page already in memory.
    if((isfound = inPageList(bm, page, pageNum)) != NULL){
        for(int i = K-1; i > 0; i--){
            attribute->khist[isfound->page->pageNum][i] = 
            	attribute->khist[isfound->page->pageNum][i-1];
        }
        //store the pinCount in khist array
        attribute->khist[isfound->page->pageNum][0] = attribute->countPinning;
        return RC_OK;
    }
    
    // If the pagelists in the memory are less than the total available pagelists, 
    // find out the first empty pageframe from the head.
    if((attribute->plists->length) < bm->numPages){
        isfound = attribute->plists->head;
        for(int i=0; i< attribute->plists->length; i++){
        	isfound = isfound->next;
        }
        //add pages to the pageLists, increment the size
        (attribute->plists->length)++;
    }
    else{    
        //traverse from the head
        pageFrame *current = attribute->plists->head;
        //check fixCount !=0 and exist distance
        while(current != NULL){
            if(current->fixCount == 0 && 
            	attribute->khist[current->page->pageNum][K] != -1){               
                distance = attribute->countPinning - 
            		attribute->khist[current->page->pageNum][K];
                //update maxDistance and current pageframe
                if(distance > maxDistance){
                    maxDistance = distance;
                    isfound = current;
                }
            }
            current = current->next;
        }        
        // Check whether traverse the whole list,
        // if there isn't exist distance, apply LRU
        if(maxDistance == -1){
            current = attribute->plists->head;
            //checks for the least recently used node
            while(current->fixCount != 0 && current != NULL){
                distance = attribute->countPinning - 
                	attribute->khist[current->page->pageNum][0];
                if(distance > maxDistance){
                    maxDistance = distance;
                    isfound = current;
                }
                current = current->next;
            }            
            // Check whether traverse the whole list,
        	// if True, then no pages available to update
            if (maxDistance == -1){
                return RC_TRAVERSE_TO_THE_END;
            }
        }
    }
    //initialize return code
    RC rc;
    
    if((rc = 
    	exchangePageFrame(bm, page, pageNum, isfound)) != RC_OK){return rc;}
    //update the khist array
    for(int i = K-1; i>0; i--){
        attribute->khist[isfound->page->pageNum][i] = 
        	attribute->khist[isfound->page->pageNum][i-1];
    }
    attribute->khist[isfound->page->pageNum][0] = attribute->countPinning;
    return RC_OK;
}
/*
	LFU() replacement strategy :
	1. same as FIFO and LRU, check whether the requested pageframe in memory
		or not, if not, then check if the list is full, If not full, 
		find empty pageframe and update.
	2. if full, traverse from the tail node to locate minimum page frequency, 
		locate this pageframe as head
*/
RC LFU (BM_BufferPool *const bm, BM_PageHandle *const page, 
	const PageNumber pageNum){
    pageFrame *isfound;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    page->data = (SM_PageHandle) malloc(PAGE_SIZE);

     // Check if page already in memory.
 	if((attribute->frameContent)[pageNum] != -1){
        isfound = searchByPageNum(attribute->plists, pageNum);
        //give the client the detail of the page
        page->pageNum = pageNum;
        page->data = isfound->page->data;
        //pinned, increase fixCount and pageFrequency
        isfound->fixCount++;
        isfound->pageFrequency++;
        isfound->rf = 1;
        return RC_OK;
    }

    // If the pagelists in the memory are less than the total available pagelists, 
    // find out the first empty pageframe from the head.
    if((attribute->plists->length) < bm->numPages){
        isfound = attribute->plists->head;
        for(int i=0; i< attribute->plists->length; i++){
        	isfound = isfound->next;
        }
        //add pages to the pageLists, increment the size
        attribute->plists->length ++;
        makeHead(&(attribute->plists), isfound);
    }
    //If the pageLists is full
 	else{
      	//traverse from the tail
      	isfound = attribute->plists->tail;
      	//if current page frequency < previous page frequency, 
      	//locate the previous pageframe as current
      	while(isfound != NULL && 
      		isfound->previous->pageFrequency < isfound->pageFrequency) {                
      			isfound = isfound->previous;
      	}
      	//Check fixCount = 0
      	while(isfound->fixCount != 0 && isfound != NULL){
      	              isfound = isfound->previous;
      	}
		//doesn't exist fixCount = 0, return Error
      	if (isfound == NULL){
      	  return RC_TRAVERSE_TO_THE_END;
      	}
		//Make new pageframe as head
      	makeHead(&(attribute->plists), isfound);
    }

  	SM_FileHandle fhandle;
  	//initialize return code
    RC rc;
    //open page file
    if ((rc = openPageFile (bm->pageFile, &fhandle)) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    // Write it back if requested pageFrame is dirty
    if(isfound->dirtyFlag == TRUE){
        writeBlock(isfound->page->pageNum,&fhandle, isfound->page->data);
        attribute->numWrite ++;
    }
    //Update frameContent[] array, set this pageframe pageNum = NO_PAGE
    (attribute->frameContent)[isfound->page->pageNum] = NO_PAGE;
    // Read the data into new frame.
    isfound->page->data = (SM_PageHandle) malloc(PAGE_SIZE);
    if((rc = ensureCapacity(pageNum, &fhandle)) != RC_OK){return rc;}
    if((rc = readBlock(pageNum, &fhandle, 
    	isfound->page->data)) != RC_OK){return rc;}
    
    //give the client the detail of the page
    page->pageNum = pageNum;
    page->data = isfound->page->data;

    attribute->numRead++;
    //set attribute of the bufferpool
    isfound->dirtyFlag = FALSE;
    isfound->fixCount = 1;
    isfound->page->pageNum = pageNum;
    isfound->rf = 1;
    isfound->pageFrequency = 1;
    //update frameContent and>pageContent
    (attribute->frameContent)[isfound->page->pageNum] = isfound->frameNum;
    (attribute->pageContent)[isfound->frameNum] = isfound->page->pageNum;
	return RC_OK;
}
/*
	CLOCK() replacement strategy:
	1. same as FIFO and LRU, check whether the requested pageframe in memory
		or not
	2. Instead of check full, instantly check the first pageframe with a reference
		bit = 0 from head. 
	3. set reference bit = 0 when traversing.
*/
RC CLOCK (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    pageFrame *isfound;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;   
    //Check if pageframe in pageLists
    if((isfound = inPageList(bm, page, pageNum)) != NULL){return RC_OK;}
    //traverse from head
    else{
        pageFrame *current = attribute->plists->head;
        //locate the pageframe with reference bit = 0
        while (current != NULL && current->rf == 1){
            current->rf = 0;
            current = current->next;
        }
        //If all pageframe's reference bit = 1, return an Error       
        if (current == NULL){
            return RC_TRAVERSE_TO_THE_END;
        }
        isfound = current;
    }
    //Initialize return code
    RC rc;
    // call exchangePageFrame with the new value of isfound
    if((rc = 
    	exchangePageFrame(bm, page, pageNum, isfound)) != RC_OK){return rc;}    
    return RC_OK;
}


/*Buffer Pool Functions*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){
	//initialize storagemanager
    SM_FileHandle fhandle;
    //Check valid PageNum
    if(!bm || numPages <= 0){
        return RC_INVALID_PARAMETER;
    }
    //open the pageFile
    if (openPageFile ((char *)pageFileName, &fhandle) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    //Initialize Buffer manager mgmtInfo
    BMAttri *attribute = malloc(sizeof(BMAttri));   
    
    attribute->numRead = 0;
    attribute->numWrite = 0;
    attribute->stratData = stratData;
    attribute->countPinning = 0;
    
    // Initialize the lookup arrays with 0 values.
    memset(attribute->pageContent,NO_PAGE,MAX_FRAMES*sizeof(int));
    memset(attribute->frameContent,NO_PAGE,MAX_PAGES*sizeof(int));
    memset(attribute->dirtyFlagContent,NO_PAGE,MAX_FRAMES*sizeof(bool));
    memset(attribute->fixCountContent,NO_PAGE,MAX_FRAMES*sizeof(int));
    memset(attribute->khist, -1, sizeof(&(attribute->khist)));
	memset(attribute->FrequencyContent,0,MAX_PAGES*sizeof(int));
    
    // Creating the linked list of empty plists.
    attribute->plists = malloc(sizeof(pageList));
    
    attribute->plists->head = attribute->plists->tail = createNewFrame();
    attribute->plists->length = 0;
    //Create the pageLists
    for(int i = 1; i<numPages; ++i){
        attribute->plists->tail->next = createNewFrame();
        attribute->plists->tail->next->previous = attribute->plists->tail;
        attribute->plists->tail = attribute->plists->tail->next;
        attribute->plists->tail->frameNum = i;
    }
    //set bm attribute
    bm->numPages = numPages;
    bm->pageFile = (char*) pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = attribute;
    //close the pagefile
    closePageFile(&fhandle);
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
	//Initialize return code
	RC rc;
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    
    pageFrame *current = attribute->plists->head;    
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_PARAMETER;
    }
    //call forceFlushPool() 
    if((rc = forceFlushPool(bm)) != RC_OK){return rc;}
    //traverse from head
    while(current != NULL){
        current = current->next;
        //deallocate memory
        free(attribute->plists->head->page->data);
        free(attribute->plists->head);
        attribute->plists->head = current;
    }
    //deallocate head and tail pointer
    attribute->plists->head = attribute->plists->tail = NULL;
    free(attribute->plists);
    free(attribute);
    bm->numPages = 0;
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
	//check valid Parameter
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_PARAMETER;
    }   
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    SM_FileHandle fhandle;
    pageFrame *current = attribute->plists->head;   
    //open a file   
    if (openPageFile ((char *)(bm->pageFile), &fhandle) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    //If a file is dirty, write back to the file
    while(current != NULL){
        if(current->dirtyFlag == TRUE){
            if(writeBlock(current->page->pageNum, 
            	&fhandle, current->page->data) != RC_OK){
                return RC_WRITE_FAILED;
            }
            current->dirtyFlag = FALSE;
            (attribute->numWrite)++;
        }
        current = current->next;
    }    
    closePageFile(&fhandle);    
    return RC_OK;
}

/*Page Management Functions*/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_PARAMETER;
    }
    if(pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    //Cases to implement replacement strategy
    switch (bm->strategy)
    {
        case RS_FIFO:
            return FIFO(bm,page,pageNum);
            break;
        case RS_LRU:
            return LRU(bm,page,pageNum);
            break;
        case RS_CLOCK:
            return CLOCK(bm,page,pageNum);
            break;
        case RS_LFU:
            return LFU(bm,page,pageNum);
            break;
        case RS_LRU_K:
            return LRU_K(bm,page,pageNum);
            break;
        default:
            return RC_INVALID_STRATEGY;
            break;
    }
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	//check invalid parameter
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_PARAMETER;
    }  
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    pageFrame *toUnpinned;
    
    //Set the pointer to the target pageframe which need to unpinned
    if((toUnpinned = 
    	searchByPageNum(attribute->plists, page->pageNum)) == NULL){
        return RC_LOCATE_PAGE_FAILED;
    }
    //When finished unpin, decrement fixCount by 1
    if(toUnpinned->fixCount > 0){
        toUnpinned->fixCount--;
    }
    else{
        return RC_LOCATE_PAGE_FAILED;
    }   
    return RC_OK;
}


RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	//check invalid parameter
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_PARAMETER;
    }
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    pageFrame *toMarkDirty;   
    //Set the pointer to the target pageframe which need to markDirty
    if((toMarkDirty = 
    	searchByPageNum(attribute->plists, page->pageNum)) == NULL){
        return RC_LOCATE_PAGE_FAILED;
    }    
    //Update the pageframe's dirtyFlag = True
    toMarkDirty->dirtyFlag = TRUE;    
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	//Check invalid parameter
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_PARAMETER;
    }
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    SM_FileHandle fhandle;
    pageFrame *toWriteBack;  
    //open the pageFile 
    if (openPageFile ((char *)(bm->pageFile), &fhandle) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    //Set the pointer to the target pageframe which need to writeback
    if((toWriteBack = 
    	searchByPageNum(attribute->plists, page->pageNum)) == NULL){
        closePageFile(&fhandle);
        return RC_LOCATE_PAGE_FAILED;        
    }
    
    //Write the pointing pageframe's content banck to pageFile
    if(writeBlock(toWriteBack->page->pageNum, 
    	&fhandle, toWriteBack->page->data) != RC_OK){
        closePageFile(&fhandle);
        return RC_WRITE_FAILED;
    }    
    attribute->numWrite ++;
    closePageFile(&fhandle);
    return  RC_OK;
}

/*Statistics Functions*/

/*return the value of>pageContent*/
PageNumber *getFrameContents (BM_BufferPool *const bm){   
	BMAttri *attribute = (BMAttri *)bm->mgmtData;
    return attribute->pageContent;
}

/*Traverse through the pagelists from head then 
  updates the entry of dirtyFlagContent accordingly*/
bool *getDirtyFlags (BM_BufferPool *const bm){    
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    pageFrame *current = attribute->plists->head;
    
    while (current != NULL){
        (attribute->dirtyFlagContent)[current->frameNum] = current->dirtyFlag;
        current = current->next;
    }
    
    return attribute->dirtyFlagContent;
}
/*Traverse through the pagelists from head then 
  updates the entry of fixCountContent accordingly*/
int *getFixCounts (BM_BufferPool *const bm){
    BMAttri *attribute = (BMAttri *)bm->mgmtData;
    pageFrame *current = attribute->plists->head;   
    while (current != NULL){
        (attribute->fixCountContent)[current->frameNum] = current->fixCount;
        current = current->next;
    }    
    return attribute->fixCountContent;
}
/*returns the value of numRead*/
int getNumReadIO (BM_BufferPool *const bm){
	BMAttri *attribute = (BMAttri *)bm->mgmtData;   
    return attribute->numRead;
}

/*returns the value of numWrite*/
int getNumWriteIO (BM_BufferPool *const bm){
    BMAttri *attribute = (BMAttri *)bm->mgmtData; 
    return attribute->numWrite;
}
