#pragma once

#include <string>

struct radix_node {
	char key;
	bool hasValue;
	uint32_t value;
	radix_node* children, *next;

	radix_node() {
		hasValue = false;
		children = NULL;
		next = NULL;
		key = '\0';
	}

	~radix_node() {
		for(radix_node* n=children;n != NULL;) {
			radix_node* nd = n->next;
			delete n;
			n = nd;
		}
	}

	radix_node* get(char c) {
		for(radix_node* n=children;n != NULL;n = n->next)
			if(n->key == c) return n;
		return NULL;
	}

	radix_node* put(uint8_t c) {
		radix_node* n = children;
		if(n == NULL) {
			children = new radix_node();
			children->key = c;
			return children;
		}
		while(true) {
			if(n->next == NULL && n->key != c) break;
			else if(n->key == c) return n;
			else n = n->next;
		}
		radix_node* newNode = new radix_node();
		newNode->key = c;
		n->next = newNode;
		return newNode;
	}
};

// Basic radix tree on a per-byte basis
struct radix_tree {
	radix_node* root;

	radix_tree() {
		root = new radix_node();
	}

	~radix_tree() {
		if(root != NULL) delete root;
	}

	void insert(std::string key, uint32_t value) {
		radix_node* n = root;
		while(!key.empty()) {
			n = n->put(key[0]);
			key = key.substr(1);
		}
		if(n->hasValue) return;
		n->value = value;
	}

	bool contains(std::string key) {
		radix_node* n = root;
		while(!key.empty()) {
			n = n->get(key[0]);
			key = key.substr(1);
			if(n == NULL) return false;
		}
		return n->hasValue;
	}

	uint32_t search(std::string key) {
		radix_node* n = root;
		while(!key.empty()) {
			n = n->get(key[0]);
			key = key.substr(1);
			if(n == NULL) return 0;
		}
		return n->value;
	}
};
