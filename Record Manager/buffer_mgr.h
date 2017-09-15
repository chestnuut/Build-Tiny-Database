#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H
#include "dberror.h"
#include "dt.h"

typedef struct PageFrame {
    struct BM_PageHandle *page;
    struct PageFrame *prev;
    struct PageFrame *nxt;
    int readIONum;
    int writeIONum;
    int fNum;
    bool dFlag;
    bool cFlag;
    int fCount;
} PageFrame;
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
    RS_LRU = 1,
    RS_CLOCK = 2,
    RS_LFU = 3,
    RS_LRU_K = 4
} ReplacementStrategy;
typedef int PageNumber;
#define NO_PAGE -1
typedef struct BM_BufferPool {
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *mgmtData;
} BM_BufferPool;
typedef struct PageList {
    struct PageFrame *curr;
    struct PageFrame *h;
    struct PageFrame *c;
    struct PageFrame *t;
    int size;
} PageList;
typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

#define MAKE_POOL()					\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName,
		const int numPages, ReplacementStrategy strategy, void *stratData);
RC shutdownBufferPool(BM_BufferPool * const bm);
RC forceFlushPool(BM_BufferPool * const bm);
RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page);
RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page);
RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page);
RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum);
PageNumber *getFrameContents(BM_BufferPool * const bm);
bool *getDirtyFlags(BM_BufferPool * const bm);
int *getFixCounts(BM_BufferPool * const bm);
int getNumReadIO(BM_BufferPool * const bm);
int getNumWriteIO(BM_BufferPool * const bm);

#endif
