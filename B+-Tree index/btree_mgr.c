#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
//Initialize a new BTree Manager contains all data in the BTree
BTreeManager * btree = NULL;

/*********** Below functions are for inserting entry into B+-Tree *************/

// Create a new EntryContent structure to hold the value to which a vKey refer.
// Returns the EntryContent structure.
EntryContent * newEntry(RID * rid) {

	EntryContent * ec = (EntryContent *) malloc(sizeof(EntryContent));
	if (ec == NULL) {
		return RC_INSERT_ERROR;
	} 
	else {
		// Store the info into new ec structure
		ec->rid.slot = rid->slot;
		ec->rid.page = rid->page;
	}
	printf("New EntryContent = %d", ec->rid.page);
	return ec;
}

// makeNode() Create a new general node
// can be served as either a leaf or an internal node.
Node * makeNode(BTreeManager * btree) {
	btree->numNodes++;
	int treeRank = btree->rank; 
	// allocate memory for new_node
	Node * new_node = malloc(sizeof(Node));
	if (new_node == NULL) {
		return RC_INSERT_ERROR;
	}
	// allocate new_node's vKey'
	new_node->keys = malloc((treeRank - 1) * sizeof(Value *));
	if (new_node->keys == NULL) {
		return RC_INSERT_ERROR;
	}
	// allocate new_node's pointer'
	new_node->pointers = malloc(treeRank * sizeof(void *));
	if (new_node->pointers == NULL) {
		return RC_INSERT_ERROR;
	}
	// pass required value to new node
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->next = NULL;
	return new_node;
}

// makeNewTree() creates a new tree when inserted the first node.
// then return a node
Node * makeNewTree(BTreeManager * btree, Value * vKey, EntryContent * pointer) {
	Node * root = makeNode(btree);
	root->is_leaf = true;
	int treeRank = btree->rank;
	// initialize root node info
	root->keys[0] = vKey;
	root->pointers[treeRank - 1] = NULL;
	root->parent = NULL;
	root->pointers[0] = pointer;
	root->num_keys++;

	btree->numEntries++;
	printf("A new tree has made \n");
	return root;
}

// insertEntry() inserts a pointer and a vKey to the given leaf node and return the changed leaf node
Node * insertEntry(BTreeManager * btree, Node * leaf, Value * vKey, EntryContent * pointer) {
	// initialize the insertion pointer to the head
	btree->numEntries++;
	int ip = 0;
	// point the ip to where we need to insert
	while (ip < leaf->num_keys && leaf->keys[ip] < vKey)
		ip++;
	// insert the node
	for (int i = leaf->num_keys; i > ip; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}
	// altered the leaf node
	leaf->keys[ip] = vKey;
	leaf->pointers[ip] = pointer;
	leaf->num_keys++;
	return leaf;
}

// parentInsert() Insert a new node (leaf or internal node) into the B+ tree.
Node * parentInsert(BTreeManager * btree, Node * left, Value * vKey, Node * right) {

	printf("Inserting into BTree.... \n");
	int left_index;
	Node * parent = left->parent;
	int treeRank = btree->rank;

	// if it is not a new root, make one
	if (parent == NULL)
		return makeNewRoot(btree, left, vKey, right);

	// find the left index incase it's a leaf node
	left_index = getLeftIndex(parent, left);

	// If the node entry has enough space, insert into it.
	if (parent->num_keys < treeRank - 1) {
		return generalInsert(btree, parent, left_index, vKey, right);
	}

	// If not, call insertIntoNodeAndSplit() to split the node and then insert into it
	return insertIntoNodeAndSplit(btree, parent, left_index, vKey, right);
}

// insertEntryAndSplit() call the function when the leaf node +1 > tree's rank.
// Split the node and insert the entry
Node * insertEntryAndSplit(BTreeManager * btree, Node * leaf, Value * vKey, EntryContent * pointer) {
	
	Value ** temp_keys; // to help store the intermediate value
	Node * new_leaf; // create a new leaf
	void ** temp_pointers; //// to help store the intermediate value
	int ip, split, new_key, i, j; //ip point to the index where we want to insert
	int treeRank = btree->rank;

	new_leaf = makeNode(btree);
	new_leaf->is_leaf = true;
	// allocate temp_keys' memory
	temp_keys = malloc(treeRank * sizeof(Value));

	if (temp_keys == NULL) {
		return RC_INSERT_ERROR;
	}
	// allocate temp_pointer memory
	temp_pointers = malloc(treeRank * sizeof(void *));
	if (temp_pointers == NULL) {
		return RC_INSERT_ERROR;
	}

	printf("Split is needed \n");
	// insert the node
	ip = 0;
	while (ip < treeRank - 1 && leaf->keys[ip]<vKey)
		ip++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == ip)
			j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}

	temp_keys[ip] = vKey;
	temp_pointers[ip] = pointer;

	leaf->num_keys = 0;

	// Split procedure
	// Create the new node and copy half the keys and pointers to the old and half to the new.
	if ((treeRank - 1) % 2 == 0)
		split = (treeRank - 1) / 2;
	else 
		split = (treeRank - 1) / 2 + 1;
	// The left node get the extra vKey
	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < treeRank; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}
	// free the allocated memory
	free(temp_pointers);
	free(temp_keys);
	// initialize the new leaf data
	new_leaf->pointers[treeRank - 1] = leaf->pointers[treeRank - 1];
	leaf->pointers[treeRank - 1] = new_leaf;
	// for the leaf node and new leaf node, point to null
	for (i = leaf->num_keys; i < treeRank - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->num_keys; i < treeRank - 1; i++)
		new_leaf->pointers[i] = NULL;
	// pass the data in newly created structure
	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];
	// add one entry
	btree->numEntries++; 
	return parentInsert(btree, leaf, new_key, new_leaf);
}

// insertIntoNodeAndSplit() inserts a new vKey and pointer to a non-leaf node, call this function
// when the node's size is exceed the tree rank. Split the node and then insert
Node * insertIntoNodeAndSplit(BTreeManager * btree, Node * old_node, int left_index, Value * vKey, Node * right) {

	int i, j, split, parentKey;
	// Create temporary keys and pointers and node to hold data in current tree
	Node * new_node, *child;
	Value ** temp_keys;
	Node ** temp_pointers;

	int treeRank = btree->rank;
	// allocate temp keys
	temp_keys = malloc(treeRank * sizeof(Value *));
	if (temp_keys == NULL) {
		return RC_INSERT_ERROR;
	}
	// allocate temp pointers
	temp_pointers = malloc((treeRank + 1) * sizeof(Node *));
	if (temp_pointers == NULL) {
		return RC_INSERT_ERROR;
	}
	// insert the new node to the left part
	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1)
			j++;
		temp_pointers[j] = old_node->pointers[i];
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index)
			j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = vKey;

	// Create the new node and copy half the keys and pointers to the old and half to the new.
	if ((treeRank - 1) % 2 == 0)
		split = (treeRank - 1) / 2;
	else
		split = (treeRank - 1) / 2 + 1;
	// Create the new node
	new_node = makeNode(btree);
	// detach the old node
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	parentKey = temp_keys[split - 1];
	for (++i, j = 0; i < treeRank; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	// Insert a new vKey into the parent of the two nodes after split 
	new_node->pointers[j] = temp_pointers[i];
	// free the allocated memory
	free(temp_pointers);
	free(temp_keys);
	// attach the new node
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		child = new_node->pointers[i];
		child->parent = new_node;
	}
	// increment the tree's entry'
	btree->numEntries++;
	return parentInsert(btree, old_node, parentKey, new_node);
}

// getLeftIndex() get the Left index to be inserted.
int getLeftIndex(Node * parent, Node * left) {
	int left_index = 0;
	while (left_index <= parent->num_keys && parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

// generalInsert() Inserts a new vKey and pointer to a node without change the btree
Node * generalInsert(BTreeManager * btree, Node * parent, int left_index, Value * vKey, Node * right) {
	
	for (int i = parent->num_keys; i > left_index; i--) {
		parent->pointers[i + 1] = parent->pointers[i];
		parent->keys[i] = parent->keys[i - 1];
	}

	parent->pointers[left_index + 1] = right;
	parent->keys[left_index] = vKey;
	parent->num_keys++;

	return btree->root;
}

// makeNewRoot() Create a new root for two generated subtrees and
// inserts the appropriate vKey into the new root.
Node * makeNewRoot(BTreeManager * btree, Node * left, Value * vKey, Node * right) {
	// root created, pass value in data structure
	Node * root = makeNode(btree);
	root->keys[0] = vKey;
	root->pointers[0] = left;
	root->pointers[1] = right;
	root->num_keys++;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	return root;
}

// seekLeafNode() return the leaf with given vKey
Node * seekLeafNode(Node * root, Value * vKey) {
	
	int i = 0;
	Node * rt = root;
	if (rt == NULL) {
		printf("Empty tree.\n");
		return rt;
	}

	while (!rt->is_leaf) { // root is not a leaf
		i = 0;
		while (i < rt->num_keys) {
			if (vKey > rt->keys[i] || vKey == rt->keys[i]) {
				i++;
			} else
				break;
		}
		rt = (Node *) rt->pointers[i];
	}
	return rt;
}

// locateEntry() returns the EntryContent given the specific vKey
EntryContent * locateEntry(Node * root, Value *vKey) {

	int i = 0;
	Node * ln = seekLeafNode(root, vKey);
	if (ln == NULL)
		return NULL;
	for (i = 0; i < ln->num_keys; i++) {
		if (ln->keys[i]==vKey)
			break;
	}
	if (i == ln->num_keys)
		return NULL;
	else
		return (EntryContent *) ln->pointers[i];
}

/*********** Below functions are for deleting entry from B+-Tree *************/

// getLeftNeighbor() calls when we need to do deletion
// returns the index of a node's nearest left sibling '
int getLeftNeighbor(Node * n) {
	int i;

	// Return the index of the vKey to the left of the pointer in the parent pointing to n.
	// If n is the leftmost child, then return i-1 = -1.
	for (i = 0; i <= n->parent->num_keys; i++)
		if (n->parent->pointers[i] == n)
			return i - 1;

	// Error state.
	printf("Encountered nonexistent parent's pointer\n");
	printf("Node:  %#lx\n", (unsigned long) n);
	exit(RC_ERROR);
}

// removeEntryFromNode() Removes a record given the specified vKey from the the specified node.
Node * removeEntryFromNode(BTreeManager * btree, Node * n, Value * vKey, Node * pointer) {
	int i, num_pointers;
	int treeRank = btree->rank;
	// Remove the vKey and shift other keys accordingly.
	i = 0;
	while (n->keys[i] != vKey)
		i++;

	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	// Remove the pointer and shift other pointers accordingly.
	// First determine number of pointers.
	num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
	i = 0;
	while (n->pointers[i] != pointer)
		i++;
	for (++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];

	// decrement the vKey number.
	n->num_keys--;
	// decrement the entry number
	btree->numEntries--;

	// Set other pointers to NULL
	if (n->is_leaf)
		for (i = n->num_keys; i < treeRank - 1; i++)
			n->pointers[i] = NULL;
	else // A leaf uses its last pointer to point to the next leaf.
		for (i = n->num_keys + 1; i < treeRank; i++)
			n->pointers[i] = NULL;
	return n;
}

// reorderCTree reorders the root after a record was deleted from the B+-Tree
Node * reorderBTree(Node * root) {
	// create a new root
	Node * new_root;
	// root is not empty meansthe tree was already reordered. simply return
	if (root->num_keys > 0)
		return root;
	// if the root is not empty and it has a child, ellevate its child as the new root
	if (!root->is_leaf) {
		new_root = root->pointers[0];
		new_root->parent = NULL;
	} else {
		// If the root is not empty and it is a leaf (has no children) 
		// then the whole tree is empty.
		new_root = NULL;
	}

	// De-allocate memory space and free memory
	free(root->keys);
	free(root->pointers);
	free(root);

	return new_root;
}

// reorderEntry() calls when the node's neighbor is too big and this node is too small after deletion
// Then we need to append the node's entries when there is enough space
Node * reorderEntry(Node * root, Node * n, Node * neighbor, int leftNeighbor, int parentIndex, int parentKey) {
	int i;
	Node * temp_node;
	if (leftNeighbor != -1) {
		// swap the neighbor's vKey-pointer pair on its tail with its head when n has left neighbor'
		if (!n->is_leaf)
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
		for (i = n->num_keys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}
		if (!n->is_leaf) {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			temp_node = (Node *) n->pointers[0];
			temp_node->parent = n;
			neighbor->pointers[neighbor->num_keys] = NULL;
			n->keys[0] = parentKey;
			n->parent->keys[parentIndex] = neighbor->keys[neighbor->num_keys - 1];
		} else {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			neighbor->pointers[neighbor->num_keys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			n->parent->keys[parentIndex] = n->keys[0];
		}
	} else {
		// Take a vKey-pointer pair to the neighbor's right if n is the leftmost child'
		if (n->is_leaf) {
			n->keys[n->num_keys] = neighbor->keys[0];
			n->pointers[n->num_keys] = neighbor->pointers[0];
			n->parent->keys[parentIndex] = neighbor->keys[1];
		} else {
			n->keys[n->num_keys] = parentKey;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			temp_node = (Node *) n->pointers[n->num_keys + 1];
			temp_node->parent = n;
			n->parent->keys[parentIndex] = neighbor->keys[0];
		}
		// Move the neighbor's leftmost vKey-pointer pair to n's rightmost position.
		for (i = 0; i < neighbor->num_keys - 1; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}
	// increment n's vKey'
	n->num_keys++;
	// decrement neighbor's vKey'
	neighbor->num_keys--;

	return root;
}

// removeEntry() removes the record given specified vKey and pointer, and adjust the structure
// to fit btree's properties'
Node * removeEntry(BTreeManager * btree, Node * n, Value * vKey, void * pointer) {

	int min_keys, leftNeighbor,parentIndex, parentKey, currentRank;
	Node * neighbor;
	int treeRank = btree->rank;

	// delete its vKey and pointer
	n = removeEntryFromNode(btree, n, vKey, pointer);

	// reorder the tree when n is root
	if (n == btree->root)
		return reorderBTree(btree->root);

	// Determine node's min_key'
	if (n->is_leaf) {
		if ((treeRank - 1) % 2 == 0)
			min_keys = (treeRank - 1) / 2;
		else
			min_keys = (treeRank - 1) / 2 + 1;
	} else {
		if ((treeRank) % 2 == 0)
			min_keys = (treeRank) / 2;
		else
			min_keys = (treeRank) / 2 + 1;
		min_keys--;
	}

	// check if node's space is enough'
	if (n->num_keys >= min_keys)
		return btree->root;

	// Merging or Reorder the node when node's space is not satisfy.
	leftNeighbor = getLeftNeighbor(n);
	parentIndex = leftNeighbor == -1 ? 0 : leftNeighbor;
	parentKey = n->parent->keys[parentIndex];
	// locate the appropriate neighbor node to merge.
	neighbor = (leftNeighbor == -1) ? n->parent->pointers[1] : n->parent->pointers[leftNeighbor];
	// locate its parentKey between the pointer to node n and the pointer to the neighbor.
	currentRank = n->is_leaf ? treeRank : treeRank - 1;

	if (neighbor->num_keys + n->num_keys < currentRank)
		// doing Merging process
		return mergeNodes(btree, n, neighbor, leftNeighbor, parentKey);
	else
		// doing reorder process
		return reorderEntry(btree->root, n, neighbor, leftNeighbor, parentIndex, parentKey);
}

// mergeNode() calls when a neighbor node is too small after deletion ao we need to 
// embrace the additional entries when there is a space for it 
Node * mergeNodes(BTreeManager * btree, Node * n, Node * neighbor, int leftNeighbor, int parentKey) {
	// naborIP is neigbour's insertion pointer
	int i, j, naborIP, n_end;
	Node * temp_node;
	int treeRank = btree->rank;

	// if node is on the extreme left and neighbor is to its right, swap both.
	if (leftNeighbor == -1) {
		temp_node = n;
		n = neighbor;
		neighbor = temp_node;
	}
	
	// copy keys and pointers from n, start from neighbor pointer 
	// n and neighbor have swapped places.
	naborIP = neighbor->num_keys;
	if (!n->is_leaf) {// If its a non-leaf node, append parentKey and the following pointer.
		neighbor->keys[naborIP] = parentKey;
		// append all pointers and keys from the neighbor.
		neighbor->num_keys++;
		n_end = n->num_keys;
		// append all keys from the neighbor
		for (i = naborIP + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
			n->num_keys--;
		}
		// append pointers
		neighbor->pointers[i] = n->pointers[j];
		// point the parent to the neighbor
		for (i = 0; i < neighbor->num_keys + 1; i++) {
			temp_node = (Node *) neighbor->pointers[i];
			temp_node->parent = neighbor;
		}
	} else {
		// If it is in al leaf node, append n's pointers and keys to its neighbor'
		for (i = naborIP, j = 0; j < n->num_keys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
		}
		// The neighbor's last pointer point to n's previous right siblings
		neighbor->pointers[treeRank - 1] = n->pointers[treeRank - 1];
	}

	btree->root = removeEntry(btree, n->parent, parentKey, n);

	// Free allocated memory space.
	free(n->keys);
	free(n->pointers);
	free(n);
	return btree->root;
}

// deleteWithKey() deletes the entry given the specific vKey.
Node * deleteWithKey(BTreeManager * btree, Value * vKey) {
	// locate the entry we want to delete
	Node * entry = locateEntry(btree->root, vKey);
	EntryContent * key_leaf = seekLeafNode(btree->root, vKey);

	if (entry != NULL && key_leaf != NULL) {
		btree->root = removeEntry(btree, key_leaf, vKey, entry);
		free(entry);
	}
	return btree->root;
}



/*********** Below functions are for print a B+-Tree *************/

// enqueue() is used when print the B+-Tree
void enqueue(BTreeManager * btree, Node * new_node) {
	Node * temp_node;
	if (btree->queue == NULL) {
		btree->queue = new_node;
		btree->queue->next = NULL;
	} else {
		temp_node = btree->queue;
		while (temp_node->next != NULL) {
			temp_node = temp_node->next;
		}
		temp_node->next = new_node;
		new_node->next = NULL;
	}
}

// dequeue() is used when printing the B+ Tree
Node * dequeue(BTreeManager * btree) {
	Node * temp_node = btree->queue;
	btree->queue = btree->queue->next;
	temp_node->next = NULL;
	return temp_node;
}

// This function gives the length in edges of the path from any node to the root.
int rootLevel(Node * root, Node * child) {
	int length = 0;
	Node * temp_node = child;
	while (temp_node != root) {
		temp_node = temp_node->parent;
		length++;
	}
	return length;
}

/*
Required functions
*/
// Initializing Index Manager
RC initIndexManager(void *mgmtData) {
	printf("initializing IndexManager... \n");
	initStorageManager();
	printf("initializing Success \n");
	return RC_OK;
}

// close the Index Manager and free the BTree Manager structure
RC shutdownIndexManager() {
	btree = NULL; //free the Btree Structure 
	printf("shutdownIndexManager SUCCESS! \n");
	return RC_OK;
}
/*B+-Tree Functions*/

/***********************
**
createBtree(), openBtree(), closeBtree(), deleteBtree()
funstions to operate on an btree index
**
***********************/

// createBtree() creates a B+-Tree where named idxID, vKey's Datatype as keyType
// and number of elements
RC createBtree(char *idxId, DataType keyType, int n) {
	int maxNodes = PAGE_SIZE / sizeof(Node);

	// return error message if n > max nodes we can accomodate
	if (n > maxNodes) {
		printf("n = %d > Max. Nodes = %d \n", n, maxNodes);
		return RC_EXCEED_MAX_RANK;
	}

	// Initialize the BtreeManager
	btree = (BTreeManager *) malloc(sizeof(BTreeManager));
	btree->rank = n + 2;		// Setting the intial node of B+ Tree
	btree->numNodes = 0;		// initialize num of nodes =0
	btree->numEntries = 0;	// initialize num of entries=0
	btree->root = NULL;		// No root
	btree->queue = NULL;		// No node for printing
	btree->keyType = keyType;	// Set datatype to "keyType"

	// Initialize Buffer Manager and store in BTreeManager
	BM_BufferPool * bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
	btree->bufferPool = *bm;
	// Initialize storage Manager
	SM_FileHandle fileHandler;
	RC result;

	char data[PAGE_SIZE];
	// Running page functions
	if ((result = createPageFile(idxId)) != RC_OK)
		return result;//create page file

	if ((result = openPageFile(idxId, &fileHandler)) != RC_OK)
		return result;//open page file

	if ((result = writeBlock(0, &fileHandler, data)) != RC_OK)
		return result;//write empty content to block

	if ((result = closePageFile(&fileHandler)) != RC_OK)
		return result;//close page file

	printf("createBtree SUCCESS\n");
	return (RC_OK);
}

// Open a B+-Tree with a given 'idxID'
RC openBtree(BTreeHandle **tree, char *idxId) {
	// Set the tree structure
	*tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
	(*tree)->mgmtData = btree;

	// initializing a Buffer Manager
	RC result = initBufferPool(&btree->bufferPool, idxId, 1000, RS_FIFO, NULL);

	if (result == RC_OK) {
		printf("The tree was opened \n");
		return RC_OK;
	}
	return result;
}

// closes the B+-Tree, shutdowns the buffer pool and free all utilized memory space.
RC closeBtree(BTreeHandle *tree) {
	// Set a B+-Tree Manager
	BTreeManager * btree = (BTreeManager*) tree->mgmtData;

	// Mark page dirty so as to write back to the disk.
	markDirty(&btree->bufferPool, &btree->pageHandler);

	// Shutdown the buffer pool.
	shutdownBufferPool(&btree->bufferPool);

	// free memory spaces.
	free(btree);
	free(tree);

	printf("closeBtree SUCCESS");
	return RC_OK;
}

// deleted the given name B+-Tree 
// deleting the associated page with it.
RC deleteBtree(char *idxId) {
	RC result;
	if ((result = destroyPageFile(idxId)) != RC_OK)
		return result;
	printf("delete Btree Finished\n");
	return RC_OK;
}

/*************************************
* 
  getNumNodes()
  getNumEntries()
  getKeyType()
  Access Information about a B+-Tree
*
**************************************/
// getNumNodes() gets the number of nodes present in the B+ Tree and put in *result
RC getNumNodes(BTreeHandle *tree, int *result) {
	// Set B+-Tree manager
	BTreeManager * btree = (BTreeManager *) tree->mgmtData;
	printf("This tree has %d nodes! \n", btree->numNodes);

	// store it to *result
	*result = btree->numNodes;
	return RC_OK;
}

// getNumEntries gets the number of entries in the B+ Tree and put it in *result
RC getNumEntries(BTreeHandle *tree, int *result) {
	// Set B+-Tree Manager
	BTreeManager * btree = (BTreeManager *) tree->mgmtData;
	printf("This tree has %d entries! \n", btree->numEntries);

	// Store the numEntries into *result
	*result = btree->numEntries;
	return RC_OK;
}

// getKeyType() gets the datatype of the keys in the B+ Tree and store it in *result
RC getKeyType(BTreeHandle *tree, DataType *result) {
	// Set B+-Tree Manager
	BTreeManager * btree = (BTreeManager *) tree->mgmtData;
	// Store the keyType into *result
	*result = btree->keyType;
	return RC_OK;
}





/*************************
**
To access index
findkey(), insertKey(), deletekey()
**
*************************/

// findkey() return the RID for that entry with the search vKey in the b-tree.
// If the vKey does not exist should return an RC error
extern RC findKey(BTreeHandle *tree, Value *vKey, RID *result) {
	// Set B+-Tree manager
	BTreeManager *btree = (BTreeManager *) tree->mgmtData;

	// locate the vKey's entry in the tree
	EntryContent * tofind = locateEntry(btree->root, vKey);

	if (tofind == NULL) {
		// If no entry is found, return error
		return RC_IM_KEY_NOT_FOUND;
	} else {
		// show for which vKey is here
		printf("\n EntryContent -- vKey %d, page %d, slot = %d \n", vKey->v.intV, tofind->rid.page, tofind->rid.slot);
	}

	// If dound, store the rid to "result"
	*result = tofind->rid;
	return RC_OK;
}

// insert a new entry/record with the specified vKey and RID.
RC insertKey(BTreeHandle *tree, Value *vKey, RID rid) {
	// Set B+-Tree Manager Information
	BTreeManager *btree = (BTreeManager *) tree->mgmtData;
	EntryContent * pointer; // create a new pointer point to the entry
	Node * leaf; //create a leaf node

	int treeRank = btree->rank; // set the tree rank

	// Check if a record with the spcified vKey already exists.
	if (locateEntry(btree->root, vKey) != NULL) {
		printf("insertKey :: vKEY EXISTS \n");
		return RC_IM_KEY_ALREADY_EXISTS; // Return Error code
	}
	// if not, Create a new entry for the value RID.
	pointer = newEntry(&rid);

	// Create a new tree if the tree not exist
	if (btree->root == NULL) {
		btree->root = makeNewTree(btree, vKey, pointer);
		printf("Tree not exist, inserte the root node \n");
		// print tree for Debugging 
		//printf("\n");
		//printTree(tree);
		return RC_OK;
	}

	// If the tree already exists, find a leaf where we can insert
	leaf = seekLeafNode(btree->root, vKey);

	if (leaf->num_keys < treeRank - 1) {
		// If the leaf has space, insert the new vKey in it
		leaf = insertEntry(btree, leaf, vKey, pointer);
	} else {
		// If the leaf is full, split it and insert the new vKey in it.
		btree->root = insertEntryAndSplit(btree, leaf, vKey, pointer);
	}

	// Print the B+ Tree for debugging
	//printf("\n");
	//printTree(tree);
	return RC_OK;
}

// deleteKey() removes a vKey (and corresponding record pointer) from the index  
RC deleteKey(BTreeHandle *tree, Value *vKey) {
	// Set B+-Tree Manager
	BTreeManager *btree = (BTreeManager *) tree->mgmtData;

	// Delete the entry
	btree->root = deleteWithKey(btree, vKey);
	// For debuging, print the tree
	//print("\n")
	//printTree(tree);
	return RC_OK;
}






/********************
**
Clients Scan functions
**
********************/

// openTreeScan() let the clients san through all entries of a BTree.
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
	// Set B+-Tree Manager
	BTreeManager *btree = (BTreeManager *) tree->mgmtData;

	// Set B+-Tree Scan Manager
	ScanManager *sm = malloc(sizeof(ScanManager));

	// Allocate memory space for scan handle.
	*handle = malloc(sizeof(BT_ScanHandle));
	// set the root node
	Node * node = btree->root;
	
	if (btree->root == NULL) {
		// If there is no Tree, return an error.
		return RC_NO_RECORDS_TO_SCAN;
	} else {
		printf("Tree is scanning...\n");
		while (!node->is_leaf)
			node = node->pointers[0];

		// Initialize the ScanManager's info.
		sm->keyIndex = 0;
		sm->totalKeys = node->num_keys;
		sm->node = node;
		sm->rank = btree->rank;
		(*handle)->mgmtData = sm;
		// Print the Sanning Info
		printf("keyIndex = %d, totalKeys = %d \n", sm->keyIndex, sm->totalKeys);
		printf("Tree scanned \n");
	}
	return RC_OK;
}

// nextEntry() is used to parse the entries in the B+ Tree and store info into *result
RC nextEntry(BT_ScanHandle *handle, RID *result) {
	// Set ScanManager 
	ScanManager * sm = (ScanManager *) handle->mgmtData;

	// get all scan manager's information.
	int keyIndex = sm->keyIndex;
	int totalKeys = sm->totalKeys;
	int treeRank = sm->rank;
	RID rid;
	Node * node = sm->node;

	if (node == NULL) {	// Return error if no more entries to be returned
		return RC_IM_NO_MORE_ENTRIES;
	}

	if (keyIndex < totalKeys) {// If the entries are in the leaf node
		// store the node into rid
		rid = ((EntryContent *) node->pointers[keyIndex])->rid;
		// keep scan to next node
		sm->keyIndex++;
	} else {
		// If all the entries on the leaf node have been scanned, Go to next node
		if (node->pointers[treeRank - 1] != NULL) {
			node = node->pointers[treeRank - 1];
			sm->keyIndex = 1;
			sm->totalKeys = node->num_keys;
			sm->node = node;
			rid = ((EntryContent *) node->pointers[0])->rid;
		} else {
			// return error if there are no more entries to be returned
			return RC_IM_NO_MORE_ENTRIES;
		}
	}
	// Store the rid.
	*result = rid;
	return RC_OK;
}

// closeTreeScan() closes the scan Manager and free allocated memory.
extern RC closeTreeScan(BT_ScanHandle *handle) {
	handle->mgmtData = NULL;
	free(handle);
	return RC_OK;
}

// This function prints the B+ Tree
extern char *printTree(BTreeHandle *tree) {
	BTreeManager *btree = (BTreeManager *) tree->mgmtData;
	printf("Below is the B+-Tree:\n");
	Node * n = NULL;
	int i = 0;
	int row = 0;
	int new_row = 0;

	if (btree->root == NULL) {
		printf("Empty tree.\n");
		return '\0';
	}
	btree->queue = NULL;// set the queue to null
	enqueue(btree, btree->root); 
	while (btree->queue != NULL) {
		n = dequeue(btree);
		if (n->parent != NULL && n == n->parent->pointers[0]) {
			new_row = rootLevel(btree->root, n);
			if (new_row != row) {
				row = new_row;
				printf("\n");
			}
		}

		// Print vKey depending on datatype of the vKey.
		for (i = 0; i < n->num_keys; i++) {
			printf("%d ", (*n->keys[i]).v.intV);
			printf("(%d - %d) ", ((EntryContent *) n->pointers[i])->rid.page, ((EntryContent *) n->pointers[i])->rid.slot);
		}
		if (!n->is_leaf)
			for (i = 0; i <= n->num_keys; i++)
				enqueue(btree, n->pointers[i]);

		printf("~ ");
	}
	printf("\n");

	return '\0';
}
