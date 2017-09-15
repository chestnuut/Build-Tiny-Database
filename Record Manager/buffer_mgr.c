#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
//#include <pthread.h>
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dt.h"
// Changed Typo conditions things

bool *DIRTY_FLAG; 
int READ_IO_NUM; 
int WRITE_IO_NUM; 
//pthread_rwlock_t rwlock; 
int *FIX_COUNT; 
PageNumber *PAGE_NUM;  
SM_FileHandle *FILE_HANDLE;

int getNumWriteIO(BM_BufferPool * const bm) {
	return WRITE_IO_NUM;
}

int getNumReadIO(BM_BufferPool * const bm) {
	return READ_IO_NUM;
} 

// Find Page 
RC findPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum) {
	PageList *queue = (PageList *) bm->mgmtData;
	if (queue->size < 0) {
		return RC_PAGELIST_NOT_INITIALIZED;
	} else if (queue->size == 0) {
		return RC_PAGE_NOT_FOUND;
	}

	queue->curr = queue->h;

	while (queue->curr != queue->t
			&& queue->curr->page->pageNum != pageNum) {

		queue->curr = queue->curr->nxt;
	}
	if (queue->curr == queue->t
			&& queue->curr->page->pageNum != pageNum) {
		return RC_PAGE_NOT_FOUND;
	}
	page->data = queue->curr->page->data;
	page->pageNum = queue->curr->page->pageNum;

	return RC_PAGE_FOUND;
} 
//Add Page
RC addPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result;
	//pthread_rwlock_init(&rwlock, NULL);
	//pthread_rwlock_wrlock(&rwlock);
	result = -127;
	result = openPageFile(bm->pageFile, FILE_HANDLE);

	if (result != RC_OK) {
		return result;
	}
	if (queue->size == 0) {
		queue->h->fCount = 1;
		queue->h->writeIONum = 1;
		if (FILE_HANDLE->totalNumPages < pageNum + 1) {
			int totalPages = FILE_HANDLE->totalNumPages;
			result = -127;
			result = ensureCapacity(pageNum + 1, FILE_HANDLE);
			WRITE_IO_NUM += pageNum + 1 - totalPages;
			if (result != RC_OK) {
				result = -127;
				result = closePageFile(FILE_HANDLE);
				if (result != RC_OK) {
					return result;
				}
				//pthread_rwlock_unlock(&rwlock);
				//pthread_rwlock_destroy(&rwlock);
				return result;
			}
		}
		queue->h->writeIONum = 0;
		queue->h->readIONum++;
		result = -127;
		result = readBlock(pageNum, FILE_HANDLE, queue->h->page->data);
		READ_IO_NUM++;
		queue->h->readIONum--;

		if (result != RC_OK) {
			result = -127;
			result = closePageFile(FILE_HANDLE);

			if (result != RC_OK) {
				return result;
			}
			//pthread_rwlock_unlock(&rwlock);
			//pthread_rwlock_destroy(&rwlock);

			return result;
		}

		queue->h->page->pageNum = pageNum;
		queue->h->dFlag = FALSE;
		queue->h->cFlag = FALSE;
	} else {
		queue->t = queue->t->nxt;
		queue->t->fCount = 1;
		queue->t->writeIONum = 1;
		if (FILE_HANDLE->totalNumPages < pageNum + 1) {
			int totalPages = FILE_HANDLE->totalNumPages;
			result = -127;
			result = ensureCapacity(pageNum + 1, FILE_HANDLE);
			WRITE_IO_NUM += pageNum + 1 - totalPages;
			if (result != RC_OK) {
				result = -127;
				result = closePageFile(FILE_HANDLE);
				if (result != RC_OK) {
					return result;
				}
				//pthread_rwlock_unlock(&rwlock);
				//pthread_rwlock_destroy(&rwlock);
				return result;
			}
		}
		queue->t->writeIONum = 0;
		queue->t->readIONum++;
		result = -127;
		result = readBlock(pageNum, FILE_HANDLE, queue->t->page->data);
		READ_IO_NUM++;
		queue->t->readIONum--;
		if (result != RC_OK) {
			result = -127;
			result = closePageFile(FILE_HANDLE);

			if (result != RC_OK) {
				return result;
			}
			//pthread_rwlock_unlock(&rwlock);
			//pthread_rwlock_destroy(&rwlock);

			return result;
		}
		queue->t->page->pageNum = pageNum;
		queue->t->dFlag = FALSE;
		queue->t->cFlag = FALSE;
		queue->curr = queue->t;
	}
	queue->size++;
	page->data = queue->curr->page->data;
	page->pageNum = queue->curr->page->pageNum;
	result = -127;
	result = closePageFile(FILE_HANDLE);

	if (result != RC_OK) {
		return result;
	}
	//pthread_rwlock_unlock(&rwlock);
	//pthread_rwlock_destroy(&rwlock);

	return RC_OK;
} 

// Replace the page with given pageNUm
RC changePage(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result; 

	//pthread_rwlock_init(&rwlock, NULL);
	//pthread_rwlock_wrlock(&rwlock);
	result = -127;
	result = openPageFile(bm->pageFile, FILE_HANDLE);

	if (result != RC_OK) {
		return result;
	}
	queue->curr->fCount = 1;
	queue->curr->writeIONum = 1;
	if (queue->curr->dFlag == TRUE) {
		result = -127;
		result = writeBlock(queue->curr->page->pageNum, FILE_HANDLE,
				queue->curr->page->data);
		WRITE_IO_NUM++;

		if (result != RC_OK) {
			result = -127;
			result = closePageFile(FILE_HANDLE);

			if (result != RC_OK) {
				return result;
			}
			//pthread_rwlock_unlock(&rwlock);
			//pthread_rwlock_destroy(&rwlock);

			return result;
		}
		queue->curr->dFlag = FALSE;
	}
	if (FILE_HANDLE->totalNumPages < pageNum + 1) {
		int totalPages = FILE_HANDLE->totalNumPages;
		result = -127;
		result = ensureCapacity(pageNum + 1, FILE_HANDLE);
		WRITE_IO_NUM += pageNum + 1 - totalPages;

		if (result != RC_OK) {
			result = -127;
			result = closePageFile(FILE_HANDLE);

			if (result != RC_OK) {
				return result;
			}
			//pthread_rwlock_unlock(&rwlock);
			//pthread_rwlock_destroy(&rwlock);

			return result;
		}
	}
	queue->curr->writeIONum = 0;
	queue->curr->readIONum++;
	result = -127;
	result = readBlock(pageNum, FILE_HANDLE, queue->curr->page->data);
	READ_IO_NUM++;
	queue->curr->readIONum--;

	if (result != RC_OK) {
		result = -127;
		result = closePageFile(FILE_HANDLE);

		if (result != RC_OK) {
			return result;
		}

		//pthread_rwlock_unlock(&rwlock);
		//pthread_rwlock_destroy(&rwlock);

		return result;
	}
	queue->curr->page->pageNum = pageNum;
	queue->curr->cFlag = FALSE;
	page->data = queue->curr->page->data;
	page->pageNum = queue->curr->page->pageNum;
	result = -127;
	result = closePageFile(FILE_HANDLE);

	if (result != RC_OK) {
		return result;
	}
	//pthread_rwlock_unlock(&rwlock);
	//pthread_rwlock_destroy(&rwlock);

	return RC_OK;
} 
RC FIFO(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result; 
	printf("findPage: Page-%d\n", pageNum);
	result = -127;
	result = findPage(bm, page, pageNum);
	if (result == RC_PAGE_FOUND) {
		queue->curr->fCount++;
		printf("Page-%d found\n", pageNum);

		return result;
	} else if (result != RC_PAGE_NOT_FOUND) {
		return result;
	}
	printf("Page-%d not found\n", pageNum);
	if (queue->size < bm->numPages) {
		printf("addPage: Page-%d\n", pageNum);
		result = -127; 
		result = addPage(bm, page, pageNum);

		if (result == RC_OK) {
			printf("Page-%d added\n", pageNum);
		}

		return result;
	}
	queue->curr = queue->h;
	while (queue->curr != queue->t
			&& (queue->curr->fCount != 0 || queue->curr->readIONum != 0
					|| queue->curr->writeIONum != 0)) {
		queue->curr = queue->curr->nxt;
	}
	if (queue->curr == queue->t
			&& (queue->curr->fCount != 0 || queue->curr->readIONum != 0
					|| queue->curr->writeIONum != 0)) {
		return RC_NO_REMOVABLE_PAGE;
	}
	printf("changedPage: Page-%d\n", pageNum);
	result = -127; 
	result = changePage(bm, page, pageNum);
	if (result == RC_OK && queue->curr != queue->t) {
		if (queue->curr == queue->h) {
			queue->h = queue->h->nxt;
			queue->curr->nxt->prev = NULL;
		} else {
			queue->curr->prev->nxt = queue->curr->nxt;
			queue->curr->nxt->prev = queue->curr->prev;
		}
		queue->curr->prev = queue->t;
		queue->t->nxt = queue->curr;
		queue->curr->nxt = NULL;
		queue->t = queue->t->nxt;
		printf("Page-%d changed\n", pageNum);
	}
	return result;
} 
RC LRU(BM_BufferPool * const bm, BM_PageHandle * const page, PageNumber pageNum) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result; 
	result = -127;
	result = FIFO(bm, page, pageNum);
	if (result != RC_OK && result != RC_PAGE_FOUND) {
		return result;
	} else if (result == RC_PAGE_FOUND && queue->curr != queue->t) {
		if (queue->curr == queue->h) {
			queue->h = queue->h->nxt;
			queue->curr->nxt->prev = NULL;
		} else {
			queue->curr->prev->nxt = queue->curr->nxt;
			queue->curr->nxt->prev = queue->curr->prev;
		}
		if (queue->size < bm->numPages) {
			queue->curr->nxt = queue->t->nxt;
			queue->t->nxt->prev = queue->curr;
		} else {
			queue->curr->nxt = NULL;
		}
		queue->curr->prev = queue->t;
		queue->t->nxt = queue->curr;
		queue->t = queue->t->nxt;
		queue->curr = queue->h;
	}
	return RC_OK;
} 

//Clock Strategy Method
RC CLOCK(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result; 
	printf("findPage: Page-%d\n", pageNum);
	result = -127;
	result = findPage(bm, page, pageNum);
	if (result == RC_PAGE_FOUND) {
		queue->curr->fCount++;
		queue->curr->cFlag = TRUE;
		printf("Page-%d found\n", pageNum);

		return result;
	} else if (result != RC_PAGE_NOT_FOUND) {
		return result;
	}
	printf("Page-%d not found\n", pageNum);
	if (queue->size < bm->numPages) {
		printf("addPage: Page-%d\n", pageNum);
		result = -127; 
		result = addPage(bm, page, pageNum);
		if (result == RC_OK) {
			if (queue->size < bm->numPages) {
				queue->c = queue->curr->nxt;
			} else if (queue->size == bm->numPages) {
				queue->c = queue->h;
			}

			printf("Page-%d added\n", pageNum);
		}

		return result;
	}
	while (queue->c->cFlag == TRUE || queue->c->fCount != 0
			|| queue->c->readIONum != 0 || queue->c->writeIONum != 0) {
		queue->c->cFlag = FALSE;
		if (queue->c == queue->t) {
			queue->c = queue->h;
		} else {
			queue->c = queue->c->nxt;
		}
	}
	queue->curr = queue->c;
	printf("changePage: Page-%d\n", pageNum);
	result = -127;
	result = changePage(bm, page, pageNum);
	if (result == RC_OK) {
		queue->c->cFlag = TRUE;
		if (queue->c == queue->t) {
			queue->c = queue->h;
		} else {
			queue->c = queue->c->nxt;
		}

		printf("Page-%d changed\n", pageNum);
	}

	return result;
}
void initPageList(BM_BufferPool * const bm) {
	PageList *queue = (PageList *) bm->mgmtData;
	PageFrame *pf[bm->numPages];
	int i;
	for (i = 0; i < bm->numPages; i++) {
		pf[i] = (PageFrame *) malloc(sizeof(PageFrame));
		pf[i]->page = MAKE_PAGE_HANDLE();
		pf[i]->page->data = (char *) malloc(PAGE_SIZE * sizeof(char));
		pf[i]->page->pageNum = NO_PAGE;
		pf[i]->fNum = i;
		pf[i]->readIONum = 0;
		pf[i]->writeIONum = 0;
		pf[i]->fCount = 0;
		pf[i]->dFlag = FALSE;
		pf[i]->cFlag = FALSE;
		if (i == 0) {
			pf[i]->prev = NULL;
			pf[i]->nxt = NULL;
		} else {
			pf[i - 1]->nxt = pf[i];
			pf[i]->prev = pf[i - 1];
			pf[i]->nxt = NULL;
		}
	}
	queue->h = pf[0];
	queue->t = queue->h;
	queue->curr = queue->h;
	queue->c = queue->h;
	queue->size = 0;
	return;
}
RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName,
		const int numPages, ReplacementStrategy strategy, void *stratData) {
	printf("Initializing Buffer Manager\n");
	if (numPages <= 0) {
		return RC_INVALID_NUMPAGES;
	}
	initStorageManager();
	FILE_HANDLE = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
	PAGE_NUM = (PageNumber *) malloc(bm->numPages * sizeof(PageNumber)); 
	DIRTY_FLAG = (bool *) malloc(bm->numPages * sizeof(bool)); 
	FIX_COUNT = (int *) malloc(bm->numPages * sizeof(int)); 
	READ_IO_NUM = 0;
	WRITE_IO_NUM = 0;
	bm->pageFile = (char *) pageFileName;
	bm->numPages = numPages; 
	bm->strategy = strategy; 
	PageList *queue = (PageList *) malloc(sizeof(PageList));
	bm->mgmtData = queue;

	initPageList(bm);

	return RC_OK;
} 
RC shutdownBufferPool(BM_BufferPool * const bm) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result; 
	result = -127;
	result = forceFlushPool(bm);

	if (result != RC_OK) {
		return result;
	}
	queue->curr = queue->t;
	if (bm->numPages == 1) {
		free(queue->h->page->data);
		free(queue->h->page);
	} else {
		while (queue->curr != queue->h) {
			queue->curr = queue->curr->prev;
			free(queue->curr->nxt->page->data);
			free(queue->curr->nxt->page);
		}
		free(queue->h->page->data);
		free(queue->h->page);
	}
	free(queue);
	free(FILE_HANDLE);
	free(PAGE_NUM);
	free(DIRTY_FLAG);
	free(FIX_COUNT);

	return RC_OK;
} 
RC forceFlushPool(BM_BufferPool * const bm) {
	PageList *queue = (PageList *) bm->mgmtData;
	int unwritableCount = 0;
	queue->curr = queue->h;

	while (queue->curr != queue->t) {
		if (queue->curr->dFlag == TRUE && queue->curr->fCount > 0) {
			unwritableCount++;
		} else if (queue->curr->dFlag == TRUE
				&& queue->curr->fCount == 0) {
			forcePage(bm, queue->curr->page);
		}

		queue->curr = queue->curr->nxt;
	}
	if (queue->curr == queue->t) {
		if (queue->curr->dFlag == TRUE && queue->curr->fCount > 0) {
			unwritableCount++;
		} else if (queue->curr->dFlag == TRUE
				&& queue->curr->fCount == 0) {
			forcePage(bm, queue->curr->page);
		}
	}
	if (unwritableCount != 0) {
		return RC_FLUSH_POOL_ERROR;
	}
	return RC_OK;
} 

RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result = -127;

	//pthread_rwlock_init(&rwlock, NULL);
	//pthread_rwlock_wrlock(&rwlock);
	result = -127;
	result = openPageFile(bm->pageFile, FILE_HANDLE);

	if (result != RC_OK) {
		return result;
	}

	queue->curr->writeIONum = 1;
	result = -127;
	result = writeBlock(page->pageNum, FILE_HANDLE, page->data);
	WRITE_IO_NUM++;

	if (result != RC_OK) {
		result = -127;
		result = closePageFile(FILE_HANDLE);

		if (result != RC_OK) {
			return result;
		}

		//pthread_rwlock_unlock(&rwlock);
		//pthread_rwlock_destroy(&rwlock);

		return result;
	}
	queue->curr->dFlag = FALSE;

	queue->curr->writeIONum = 0;

	result = -127;
	result = closePageFile(FILE_HANDLE);

	if (result != RC_OK) {
		return result;
	}

	//pthread_rwlock_unlock(&rwlock);
	//pthread_rwlock_destroy(&rwlock);

	return RC_OK;
} 

PageNumber *getFrameContents(BM_BufferPool * const bm) {
	PageList *queue = (PageList *) bm->mgmtData;

	queue->curr = queue->h;

	int pos = 0;
	while (queue->curr != queue->t) {
		PAGE_NUM[pos] = queue->curr->page->pageNum;

		queue->curr = queue->curr->nxt;
		pos++;
	}
	PAGE_NUM[pos++] = queue->curr->page->pageNum;
	if (pos < bm->numPages) {
		int i;
		for (i = pos; i < bm->numPages; i++) {
			PAGE_NUM[i] = NO_PAGE;
		}
	}
	queue->curr = queue->t;
	return PAGE_NUM;
} 

RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	RC result; 
	if (bm->strategy == RS_FIFO) {
		result = -127; 
		result = FIFO(bm, page, pageNum);

		if (result == RC_PAGE_FOUND) {
			result = RC_OK;
		}
	} else if (bm->strategy == RS_LRU) {
		result = LRU(bm, page, pageNum);
	} else if (bm->strategy == RS_CLOCK) {
		result = CLOCK(bm, page, pageNum);
	} else if (bm->strategy == RS_LFU) {
		return RC_RS_NOT_IMPLEMENTED;
	} else if (bm->strategy == RS_LRU_K) {
		return RC_RS_NOT_IMPLEMENTED;
	}

	return result;
} 

RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result;
	result = -127;
	result = findPage(bm, page, page->pageNum);
	if (result != RC_PAGE_FOUND) {
		return result;
	}
	queue->curr->fCount--;

	return RC_OK;
} 

RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {
	PageList *queue = (PageList *) bm->mgmtData;
	RC result; 
	result = -127;
	result = findPage(bm, page, page->pageNum);
	if (result != RC_PAGE_FOUND) {
		return result;
	}

	queue->curr->dFlag = TRUE;

	return RC_OK;
}
bool *getDirtyFlags(BM_BufferPool * const bm) {
	PageList *queue = (PageList *) bm->mgmtData;
	queue->curr = queue->h; 
	int pos = 0; 
	while (queue->curr != queue->t) {
		DIRTY_FLAG[pos] = queue->curr->dFlag;
		queue->curr = queue->curr->nxt;
		pos++; 
	}
	DIRTY_FLAG[pos++] = queue->curr->dFlag;
	if (pos < bm->numPages) {
		int i;
		for (i = pos; i < bm->numPages; i++) {
			DIRTY_FLAG[i] = FALSE;
		}
	}
	queue->curr = queue->t;
	return DIRTY_FLAG;
} 
int *getFixCounts(BM_BufferPool * const bm) {
	PageList *queue = (PageList *) bm->mgmtData;
	queue->curr = queue->h; 
	int pos = 0; 
	while (queue->curr != queue->t) {
		FIX_COUNT[pos] = queue->curr->fCount;
		queue->curr = queue->curr->nxt;
		pos++; 
	}
	FIX_COUNT[pos++] = queue->curr->fCount;
	if (pos < bm->numPages) {
		int i;
		for (i = pos; i < bm->numPages; i++) {
			FIX_COUNT[i] = 0;
		}
	}
	queue->curr = queue->t;
	return FIX_COUNT;
} 
 