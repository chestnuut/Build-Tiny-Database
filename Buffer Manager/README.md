# How to Run:

To Run the two test cases, follow the next three steps:

> **make clean** /\*to clean the generated file*/

> **make assign2_1** /\* compile the source code which test for LRU, FIFO*/

> **make assign2_2** /\*compile the second test case which test for LRU_K, CLOCK, LFU*/

> **make run** /\*to run the test file and the result will be store in assign2_1.log and assign2_2.log*/

See the result in assign2_1.log and assign2_2.log
		
# Data Structure

I have implemented the pageframes' data structures as double linked list, which means there is a head and a tail pointer pointing to the relative head and tail pointer. Moreover, a next pointer pointing to the next and a previous pointer pointing to the previous pageframe. The length of the pageframe list in the structure is also stored.

Each node in the list is called pageFrame and it contains the following attributes :

1. `frameNum`          - the number of the frame in frame list 
2. `dirtyflag`         - the dirty bit of the frame( TRUE = dirty, FALSE =not dirty)
3. `fixcount`          - the fixcount is based on pinning/ unpinning request of the page
4. `rf`                - the reference bit that works for clock strategy
5. `pageFrequency`     - the appearance frequeny of the pageframe

There are some more attributes that are required at the Bufferpool level. The following 
are the attributes implemented :

1. `pageNum`           - the number of page in the pageFile. 
2. `data`              - the actual data pointed.
3. `*plists`           - the pointer to the frame list of the current buffer pool. 
4. `*stratData`        - the pointer to the strategy data for the buffer pool.
5. `numRead`           - the total number of read done on one buffer pool. 
6. `numWrite`          - the total number of write done on one buffer pool. 
7. `countPinning`      - the total number of pinning done for the buffer pool.
8. `frameContent[]`    - the attribute stores corresponding frame number of each entry.
9. `pageContent[]`     - the attribute stores corresponding page number of each entry.
10.`FrequencyContent[]`-the attribute stores the presence of each pages for each entry.
11.`dirtyFlagContent[]`-the attribute stores the dirty flags of all the frames.
12.`fixCountContent[]` -the attribute stores the fixed count of all the frames.
13.`khist[][]`         -the history of reference of each page in memory is stored here. 

All structure can be found in buffer_mgr.h

# Functions:

`initBufferPool`:

1. A BM_BufferPool instance is provided as an argument to the initBufferPool function.
2. The function then validates the parameters provided and if the provided argument is 
invalid, it returns an error message.
3. The attributes of the BM_BufferPool are then initialised by the function.
4. At the end the frame list with empty fram is initialised.

`shutdownBufferPool`:

1. A BM_BufferPool instance is provided as an argument to the shutdownBufferPool 
function.
2. The function then validates the parameters provided and if the provided argument is 
invalid, it returns an error message.
3. The frames of frame list are emptied and deallocation of the each node is performed 
by the function.
4. The function finally deallocates the memory of the BM_Bufferpool->mgmtData.

`forceFlushPool`:

1. A BM_BufferPool instance is provided as an argument to the forceFlushPool function.
2. The function then validates the parameters provided and if the provided argument is 
invalid, it returns an error message.
3. The function then iterates through the frame list and checks for a dirty bit. If a 
frame with dirty bit is found it is set as 1. Then, the functions writes the data back 
to the file on disk, sets the dirty bit flag as 0 and increments the value of numWrite 
by 1.
4. The function returns RC_OK when all the frames are checked and no errors are 
encountered. 

`pinPage`:

1. The pinPage funtion checks whether the page is in the bufferpool.
2. The Buffer manager adopts different strategies to locate the page that is requested 
and for providing the details of the page to client.

`unpinPage`:

1. The unpinPage function iterates through the pages that are available in the frame to 
find the page to be unpinned.
2. The function returns RC_NON_EXISTING_PAGE_IN_FRAME if the page is not found.
3. The function returns RC_OK if the page is found and decreases the value of fixcount 
by 1.

`markDirty`:

1. The makeDirty function iterates through the pages that are available in the frame to 
find the page marked as dirty.
2. The function returns RC_OK if the page is found and sets the dirty bit of the page 
node as 1.

`forcePage`:

1. The forcePage function iterates through the pages that are available in the frame to 
find the page that is to be forced to the disk.
2. The function opens the file and writes the current content of the page to the page 
file on the disk if the page is found.
3. The function returns RC_OK if the above operation is successful otherwise it returns 
RC_NON_EXISTING_PAGE_IN_FRAME.

`getFrameContents`:

1. getFrameContents returns an array of PageNumbers where the ith element was the page 
stored in the ith page frame. This array is updated whenever a new frame is added in the 
function exchangePageFrame.

`getDirtyFlags`:

1. getDirtyFlags returns an array of booleans where the ith element is true if the ith 
page frame is dirty. The dirtyFlagContent gets bigger which iterating through the list 
of frames to check whether the frames are marked as dirty

`getFixCounts`:

1. getFixCounts returns an array of ints where the ith element is the fix count of the 
page stored in the ith page frame. It returns 0 for every empty page.

`getNumReadIO`:

1. getNumReadIO returns the number of pages that have been read from disk since a buffer pool was initialized.

`getNumWriteIO`:

1. This function returns the number of pages that have been written to the page file 
since the buffer pool was initialized.

`makeHead`:

1. This function takes a pageList and a pageFrame as parameters, and makes the node the 
head of given list.
2. This function is used by different page replacement strategies in order to keep the 
logical order of the frames in the frame list.

`searchByPageNum`:

1. This function takes a pageList and a PageNumber as parameters, and searches for the 
page in the framelist.
2. If the page is found, it returns the pageFrame; otherwise returns NULL.
3. This function is used by different page replacement strategies in order to find the 
required frame from the frame list.

`inPageList`:

1. This function takes a BM_BufferPool, BM_PageHandle and a pageNumber as parameters, 
and searches for the page in the pageList.
2. If the page is found :
   a. It sets the BM_PageHandle to refer to this page in memory.
   b. It increases the fixcount of the page.
3. This function is used by different page replacement strategies in case when the page 
is already available in the pageList.

`exchangePageFrame`:

1. This function takes a BM_BufferPool, BM_PageHandle, the destination pageFrame and a 
PageNumber as parameters.
2. If the destination frame has a dirty page, it writes the page back to the disk and 
updates related attributes.
3. It then reads the destination page from the disk into memory, and store in the given 
frame.
4. It updates the pageNum, numRead, dirtyflag, fixcount and rf attributes of the 
destination frame.
5. This function is used by different page replacement strategies in case when the page 
is not in the memory. It calls the function with the frame to be replaced, and the new 
page to be loaded.

## Page Replacement Method

`FIFO()`:

1. Firstly the function checks whether the page is in memory. It calls the inPageList 
function if the page is present and then returns RC_OK.
2. The first free frame is searched for starting from the head if the page is to be 
loaded from the memory. If any free frame is found then the page is loaded to that 
frame. If no free frame is found then the function starts checking from the tail to find 
the frame with fixcount 0. Then the new page from the memory is located in this frame. 
This frame is set as the head of the linked list. 
3. If the frame is found using the above mentioned process then exchangePageFrame 
function is called alse the function returns no more space in buffer.

`LRU()`:

1. Firstly the function checks whether the page is in memory. It calls the inPageList 
function if the page is present and then returns RC_OK.
2. The head is always the latest used frame because the every time a frame is referenced 
the function moves it  to the head of the framelist. Therefore the tail will be the 
least used frame.
3. The function starts checking from the tail of the list to look for a frame with 
fixcount 0 if the page is not in memory. If such a frame is found the exchangePageFrame 
is called else it returns no more space in buffer. 


`LRU_K()`:

1. Firstly the function checks whether the page is in memory. It calls the inPageList 
function if the page is present and then returns RC_OK. The reference number is updated 
every time a frame is referenced.
2. If the page is not in memory, The function searches for the page having fixcount 0 
starting from the head of the list and calculates the distance as the difference of 
current count of pinning and kth reference of the page if the page is not found in the 
memory. The page with the maximum distance is replaced.If finding such k is not 
possible, then the functions acts just like LRU.
3. If such a frame is found the exchangePageFrame is called else it returns no more 
space in buffer.

`LFU()`:

1. Firstly the function checks whether the page is in memory. It returns RC_OK if the 
page is present and calls the inPageList function.
2. The first free frame is searched for starting from the head if the page is to be 
loaded from the memory. If any free frame is found then the page is loaded to that 
frame. If no free frame is found then the function starts checking from the tail to find 
the frame with minimum page frequency. Then the new page from the memory is located in 
this frame. This frame is set as the head of the linked list. 
3. If such a frame is found the exchangePageFrame is called else it returns no more 
space in buffer.

`CLOCK()`:

1. Firstly the function checks whether the page is in memory. It returns RC_OK if the 
page is present.
2. The function searches for the first frame with a reference bit that is equal to zero 
if the page is not in memory. During the search the function sets all the reference bits 
to 0. The value found is used by the function exchangePageFrame.



