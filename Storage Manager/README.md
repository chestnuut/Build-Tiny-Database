

# How to Run:

To Run the first test case, follow the next three steps:

> **make**       /\*to compile the source code*/

>**make run** /\*to run the test file and store the output in a1_1.log file*/

> **vim a1_1.log** /\*To check the output, open the a1_1.log file.*/

> **make clean** /\*to clean the generated file*/

# Basic Functions:

`createPageFile`

Create a new page file fileName. The initial file size should be one page. 
This method should fill this single page with '\0' bytes.

`openPageFile`

Opens an existing page file. Should return RC_FILE_NOT_FOUND if the file does not exist. The second parameter is an existing file handle. If opening the file is successful, then the fields of this file handle should be initialized with the information about the opened file. For instance, you would have to read the total number of pages that are stored in the file from disk.

`closePageFile`, `destroyPageFile`

Close an open page file or destroy (delete) a page file.

#Read and Write Methods

## There are two types of read and write methods that have to be implemented:

### Methods with absolute addressing (e.g., readBlock) 

`readBlock`

The method reads the pageNumth block from a file and stores its content in the memory pointed to by the memPage page handle. If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE.

`getBlockPos`

Return the current page position in a file

`readFirstBlock`, `readLastBlock`

Read the first respective last page in a file

### Methods that address relative to the current page of a file (e.g., readNextBlock).



`readPreviousBlock`, `readCurrentBlock`, `readNextBlock`

Read the current, previous, or next page relative to the curPagePos of the file. The curPagePos should be moved to the page that was read. If the user tries to read a block before the first page of after the last page of the file, the method should return RC_READ_NON_EXISTING_PAGE.

`writeBlock`, `writeCurrentBlock`

Write a page to disk using either the current position or an absolute position.

`appendEmptyBlock`

Increase the number of pages in the file by one. The new last page should be filled with zero bytes.

`ensureCapacity`

If the file has less than numberOfPages pages then increase the size to numberOfPages.


























