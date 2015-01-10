#include "strtree.hpp"

#include <map>
#include <list>
#include <utility>
#include <algorithm>
#include <string.h>
#include "bytes.hpp"

using namespace std;

strtree_node::strtree_node() {
	memset(children, 0, sizeof(strtree_node*)*0x100);
	is_leaf = false;
}

strtree_node::~strtree_node() {
	for(int i=0;i<256;i++) {
		if(children[i] != NULL)
			delete children[i];
	}
}

strtree::strtree() {
	root = new strtree_node();
}

strtree::strtree(FILE* f) {
}

strtree::~strtree() {
	delete root;
}

bool strtree::has(ustring s) {
	strtree_node* node = root;
	for(ustring::iterator i=s.begin();node != NULL && i != s.end();i++)
		node = node->children[*i];
	return (node != NULL) && node->is_leaf;
}

void strtree::set(ustring s, uint32_t v) {
	strtree_node* node = root;

	for(ustring::iterator i=s.begin();i != s.end();i++) {
		if(node->children[*i] == NULL) {
			node->children[*i] = new strtree_node();
		}
		node = node->children[*i];
	}
	node->is_leaf = true;
	node->value = v;
}

uint32_t strtree::get(ustring s) {
	strtree_node* node = root;
	ustring::iterator i = s.begin();
	for(ustring::iterator i=s.begin();node != NULL && i != s.end();i++)
		node = node->children[*i];
	if(node == NULL || !node->is_leaf) return 0;
	return node->value;
}

void strtree::serialize(FILE* outFile) {
	// Address tables
	list<pair<uint32_t, strtree_node*> > relocations; // (loc, nodeptr)
	map<strtree_node*, uint32_t> addresses;

	list<strtree_node*> queue;
	queue.push_back(root);

	// Write initial structure
	unsigned long int processed = 0;
	while(!queue.empty()) {
		// Write the node structure
		strtree_node* node = queue.front();
		queue.pop_front();

		// Store leaf info
		addresses[node] = ftell(outFile);
		fwrite(&(node->is_leaf), 1, 1, outFile);
		if(node->is_leaf)
			writeInt32(node->value, outFile);
		
		// Store children
		for(int i=0;i<256;i++) {
			strtree_node* n = node->children[i];
			writeInt32(0, outFile); // write placeholder
			if(n != NULL) {
				// Add to relocation map and queue
				relocations.push_back(make_pair(ftell(outFile), n));
				queue.push_back(n);
			}
		}

		printf("\rWriting strtree... %d done", ++processed);
	}
	putchar('\n');

	// Sort relocation pairs, then go back and rewrite location pointers
	relocations.sort();
	list<pair<uint32_t, strtree_node*> >::iterator i;
	processed = 0;
	for(i=relocations.begin();i != relocations.end();i++) {
		pair<uint32_t, strtree_node*> tmp = *i;
		fseek(outFile, tmp.first, SEEK_SET);
		writeInt32(addresses[tmp.second], outFile);
		printf("\rWriting relocation data... %d done", ++processed);
	}
	putchar('\n');
}
