#ifndef BTREE_MGR_H
#define BTREE_MGR_H
#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "buffer_mgr.h"

// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;

/*
	Create a new data structure holding the content in an entry
*/
typedef struct EntryContent {
	RID rid;
} EntryContent;

/*
	Create a new data structure representing nodes in a B+-Tree
*/
typedef struct Node {
	void ** pointers; // Node's pointer'
	bool is_leaf; // check if it is a leaf
	int num_keys; 
	struct Node * next; // Used for queue.
	Value ** keys; // a pointer point to value
	struct Node * parent; // holds the information of its parent
} Node;

/*
	Create a new B+-Tree data structure 
*/
typedef struct BTreeManager {
	BM_BufferPool bufferPool;
	BM_PageHandle pageHandler;
	int rank; // n value in a btree
	int numNodes; // number of nodes
	int numEntries; // number of entries
	Node * root;
	Node * queue;
	DataType keyType;
} BTreeManager;

/*
	Create a new data structure for scanning of the B+-Tree
*/
typedef struct ScanManager {
	int rank; // store the rank of the tree
	int keyIndex;// store the indexes on keys
	int totalKeys; // store the total number of keys
	Node * node;
} ScanManager;

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);


/*******************Auxiliary Functions*************/

// Because our code can support not only integer but also other datatypes
// auxiliary functions should be defined to support KEYS of multiple datatypes.
bool lessThan(Value * v1, Value * v2);
bool isLarger(Value * key1, Value * key2);
bool isEqual(Value * key1, Value * key2);

/*
	seekLeafNode() returns a leaf node given its root and key 
	locateEntry() returns a record data given its root and key
*/
Node * seekLeafNode(Node * root, Value * key);
EntryContent * locateEntry(Node * root, Value * key);

// Functions to support printing of the B+ Tree
void enqueue(BTreeManager * treeManager, Node * new_node);
Node * dequeue(BTreeManager * treeManager);
int rootLevel(Node * root, Node * child);
/*
	newEntry() creates a new entry to store the data refered by given key
	insertEntry() inserts an entry pointer and its corresponding key to a leaf node
	makeNewTree() creates a new BTree after insert a node
	makeNode() creates a new Node
	makeLeaf() creates a new leaf node
	insertEntryAndSplit() split the leaf in half then insert a new entry in it
	generalInsert() inserts a new pointer and key to a safe node.
	insertIntoNodeAndSplit() split the non-leaf node in half and insert a new node in it
	parentInsert() insert a new node 
	makeNewRoot() creates a new root for two subtrees
	getLeftIndex() return the left pointer of given parent node
*/
EntryContent * newEntry(RID * rid);
Node * insertEntry(BTreeManager * treeManager, Node * leaf, Value * key, EntryContent * pointer);
Node * makeNewTree(BTreeManager * treeManager, Value * key, EntryContent * pointer);
Node * makeNode(BTreeManager * treeManager);
Node * insertEntryAndSplit(BTreeManager * treeManager, Node * leaf, Value * key, EntryContent * pointer);
Node * generalInsert(BTreeManager * treeManager, Node * parent, int left_index, Value * key, Node * right);
Node * insertIntoNodeAndSplit(BTreeManager * treeManager, Node * parent, int left_index, Value * key, Node * right);
Node * parentInsert(BTreeManager * treeManager, Node * left, Value * key, Node * right);
Node * makeNewRoot(BTreeManager * treeManager, Node * left, Value * key, Node * right);
int getLeftIndex(Node * parent, Node * left);

/*
	reorderBTree() After deleting a record, ajust the structrue of the BTree
	mergeNodes() merge two nodes after deletion
	reorderEntry() reorder two nodes after deletion
	removeEntry() delete an entry in BTree and adjust to maintain BTree properties
	deleteWithKey() delete entry or record given specific key
	removeEntryFromNode() delete an entry with specific key from the given node
	getLeftNeighbor() returns the index of a node's nearest neignbor to the left
*/

// Functions to support deleting of an element (record) in the B+ Tree
Node * reorderBTree(Node * root);
Node * mergeNodes(BTreeManager * treeManager, Node * n, Node * neighbor, int leftNeighbor, int parentKey);
Node * reorderEntry(Node * root, Node * n, Node * neighbor, int leftNeighbor, int parentKey_index, int parentKey);
Node * removeEntry(BTreeManager * treeManager, Node * n, Value * key, void * pointer);
Node * deleteWithKey(BTreeManager * treeManager, Value * key);
Node * removeEntryFromNode(BTreeManager * treeManager, Node * n, Value * key, Node * pointer);
int getLeftNeighbor(Node * n);


#endif // BTREE_MGR_H
