#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1
#define MAX_PAGES 20000
#define MAX_FRAMES 200
#define MAX_K 10
typedef struct BM_BufferPool {
  char *pageFile;
  int numPages;
  ReplacementStrategy strategy;
  void *mgmtData; // use this one to store the bookkeeping info your buffer 
                  // manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
  PageNumber pageNum;
  char *data;
} BM_PageHandle;

//add DATA STRUCTURE
//pageframe data structure: one PageFrame represent one page/frame in a buffer pool
/*
    struct BM_PageHandle *page: th page handler in bm to store pageNum and data
    double-linked list has next and previous:
        struct pageFrame *next: point to the next pageFrame
        struct pageFrame *previous: point to the previous pageFrame
    int frameNum: frame number in this list
    bool dirtyFlag: When a page is modified, set dirtyFlag = TRUE
    int fixCount: when a page is in using, fixCount=0
    int rf: reference bit works for  CLOCK strategy
    int pageFrequency: the apperance frequency of a pageframe, used in LFU
*/
/*printf("searching page: %d\n", pageNum);
printf("Page: %d found\n", pageNum);
printf("Page: %d not found\n", pageNum);
printf("Ready to append Page: %d\n", pageNum);
printf("Page: %d has append\n", pageNum);
printf("Replace Page: %d\n", pageNum);
printf("Page: %d replaced\n", pageNum);*/

typedef struct pageFrame
{
  struct BM_PageHandle *page;
  struct pageFrame *next;
  struct pageFrame *previous;      
  int frameNum;           
  bool dirtyFlag;              
  int fixCount;           
  int rf;                 
  int pageFrequency; 
}pageFrame;

//the list of pageFrame
/*
    pageFrame *head: point to the head of the list
    pageFrame *tail: point to the tail of the list
    int length: length of the lists
*/
typedef struct pageList{
  pageFrame *head;
  pageFrame *tail;
  int length;
}pageList;

//Attribute of bufferpool. Store these information for function use
/*
    pageList *plists: a pointer to the frame list of current buffer pool
    void *stratData: store the strategy data for the buffer pool
    int numRead: total number of read done on one buffer pool 
    int numWrite: total number of write done on one buffer pool
    int countPinning: total number of pinning done for the buffer pool
    int frameContent[MAX_PAGES]: size is the total pageFile size. each entry store the corresponding frame number
    int pageContent[MAX_FRAMES]:  size is the total frame size. each entry store the corresponding page number
    int FrequencyContent[MAX_PAGES]: size is the total pageFile size. each entry store the presence of each pages
    bool dirtyFlagContent[MAX_FRAMES]: dirtyflags of all the frames
    int fixCountContent[MAX_FRAMES]: fixed count of all the frames.
    int khist[MAX_PAGES][MAX_K]: 2-d array to store the history of reference of each page in memory
*/
typedef struct BMAttri{
  pageList *plists;
  void *stratData;   
  int numRead;            
  int numWrite;
  int countPinning;
  int frameContent[MAX_PAGES];         
  int pageContent[MAX_FRAMES];        
  int FrequencyContent[MAX_PAGES];     
  bool dirtyFlagContent[MAX_FRAMES];        
  int fixCountContent[MAX_FRAMES];        
  int khist[MAX_PAGES][MAX_K];     
  }BMAttri;

// convenience macros
#define MAKE_POOL()					\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
