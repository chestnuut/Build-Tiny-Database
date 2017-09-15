# How to Run:

> **make clean** /*\delete all file after compiled*/

> **make** /\*compile all target files*/

> **./test_assign3_1** /\*run the test cases*/


# Data Structure Descriptions:

1. Page 0 stores all the table information, including 

	a) the length of the record.

	b) the total number of records.

	c) the total number of pages.

	d) the number of maximum records in every page.

	e) the schema string
		
2. Page 1 is a reserved page. It's used to manage free space information. The page header is assigned the first 96 bytes, and the other 4000 bytes are assigned for free page information. I use only 1 bit for each page's free status: "1" indicates there EXIST free slots on that page, "0" indicates there EXIST NO free slots on that page. Therefore each reserve page can store 32000 pages'free status.
		
3. After every 32000 record pages, create a new reserved page. Each reserved page is linked with the previous and the next reserved page by information in the header. There is following information in a reserved page header:

	a) the first page number with free slots.

	b) the previous reserved page number.

	c) the next reserved page number.

4. Other pages are record pages. Information stored in a record page header includes:

	a) the status of page initialization.

	b) the total number of records stored on specified page.

	c) the status of occupancy for each slot. 

Records are stored from bottom on each page by calculating the offset.