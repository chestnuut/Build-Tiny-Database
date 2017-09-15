# Implemented a tiny database  
## Process:
**The basic process is**

1. [Storage Manager](#storage-manager)
2. [Buffer Manager](#buffer-manager)
3. [Record Manager](#record-manager)
4. [B+-Tree index](#btree-index)

### Storage Manager

The `storage manager` deals with pages of fixed size.

It is a module that is capable of reading blocks from a file on disk into memory and writing blocks from memory to a file on disk. 

**features:**

`reading_pages_from_a_file`

`writing_pages_from_a_file`

`creating`
`opening`
and `closing` files

### Buffer Manager

The `buffer manager` manages a fixed number of pages in memory that represent pages from a page file managed by the storage manager.

The memory pages managed by the buffer manager are called *page frames* or *frames* for short. We call the combination of a page file and the page frames storing pages from that file a **Buffer Pool**. The Buffer Manager is able to handle more than one open buffer pool at the same time. There is ONLY one buffer pool for each page file. Each buffer pool uses one page replacement strategy that is determined when the buffer pool is initialized. 
For this project, you can choose page replacement functions from:

`FIFO` `LRU` `LRU_K` `CLOCK` `LFU`

Please reference the readme file in the buffer manager for details.

### Record Manager

The `Record Manager` handles tables with a fixed schema. Clients can insert records, delete records, update records, and scan through the records in a table. A scan is associated with a search condition and only returns records that match the search condition. Each table is stored in a separate page file and the record manager can access the pages of the file through the buffer manager.

Please reference the [readme file](https://github.com/arnoldzl/Build-Tiny-Database/tree/master/Buffer%20Manager#data-structure) in the record manager for details.

### BTree Index

This `B+-Tree index` is backed up by a page file and pages of the index is accessed through the Buffer Manager. Each node occupies one page. A small fan-out was added for debugging purpose. A B+-tree stores pointer to records (the RID in Record Manager) index by a key of a given datatype. 

In this project, I only support DT_INT(datatype: integer). Pointers to intermediate nodes is represented by the page number of the page the node is stored in.

Recall B+-Tree conventions:

* **Leaf Split**: In case a leaf node need to be split during insertion and n is even, the left node should get the extra key. E.g, if n = 2 and we insert a key 4 into a node [1,5], then the resulting nodes should be [1,4] and [5]. For odd values of n we can always evenly split the keys between the two nodes. In both cases the value inserted into the parent is the smallest value of the right node.

* **Non-Leaf Split**: In case a non-leaf node needs to be split and n is odd, we cannot split the node evenly (one of the new nodes will have one more key). In this case the "middle" value inserted into the parent should be taken from the right node. E.g., if n = 3 and we have to split a non-leaf node [1,3,4,5], the resulting nodes would be [1,3] and [5]. The value inserted into the parent would be 4.


* **Leaf Underflow**: In case of a leaf underflow your implementation should first try to redistribute values from a sibling and only if this fails merge the node with one of its siblings. Both approaches should prefer the left sibling. E.g., if we can borrow values from both the left and right sibling, you should borrow from the left one.


## Extras

This project is a class project in CS525 Advance Dabase Organization. You can refer all contents from [cs525 website](http://cs.iit.edu/~cs525/index.html)