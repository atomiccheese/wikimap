#pragma once
#include <stdio.h>
#include <string.h>
#include <utility>
#include <string>
#include <list>

template<typename T, typename T2>
struct patricia_list {
	T first;
	T2 second;
	patricia_list<T,T2>* next;
	
	patricia_list(T fst, T2 snd) : next(NULL), first(fst), second(snd) {
	}

	~patricia_list() {
		if(next != NULL) delete next;
	}

	void append(T fst, T2 snd) {
		if(next != NULL) next->append(fst, snd);
		else next = new patricia_list<T,T2>(fst, snd);
	}
};

template<typename T, typename S>
struct patricia_node {
	typedef patricia_list<S, patricia_node<T,S>*>* edgelist;
	T value;
	bool hasValue;
	edgelist edges;

	patricia_node() : hasValue(false) {
		edges = NULL;
	}

	patricia_node(T v) : value(v), hasValue(true) {
		edges = NULL;
	}

	~patricia_node() {
		if(edges != NULL) delete edges;
	}

	void print(int indent=0) {
		char spaces[1024];
		memset(spaces, ' ', 1024);
		spaces[indent] = 0;
		edgelist i;
		for(i=edges;i != NULL;i = i->next) {
			printf("%s'%s'\n", spaces, i->first.c_str());
			i->second->print(indent+1);
		}
	}

	bool checkCycles() {
		std::list<patricia_node<T,S>*> stack;
		stack.push_back(this);
		while(!stack.empty()) {
			patricia_node<T,S>* elem = stack.back();
			if(elem == NULL) continue;
		}
		for(edgelist i = edges;i != NULL;i = i->next) {
			if(i->second == this) return true;
		}
		return false;
	}

	void freePtrs() {
		delete edges;
		edgelist i;
		for(i=edges;i != NULL;i=i->next)
			i->second->freePtrs();
	}

	void appendEdge(S s, patricia_node<T,S>* n) {
		if(edges != NULL) {
			edges->append(s,n);
		} else {
			edges = new patricia_list<S, patricia_node<T,S>*>(s,n);
		}
	}
};

void warnCycle() {
	printf("BREAK - CYCLE DETECTED\n");
}

template<typename T, typename S=std::string>
struct patricia_trie {
	typedef patricia_node<T, S> node_type;
	typedef patricia_list<S, node_type*>* edgelist;
	node_type* root;

	bool isPrefix(S pfx, S val) {
		return countCommonPrefix(pfx,val) == pfx.length();
	}

	int countCommonPrefix(S pfx, S val) {
		typename S::iterator i,j;
		int n = 0;
		for(i=pfx.begin(),j=val.begin();i != pfx.end();i++,j++,n++)
			if(*i != *j) return n;
		return n;
	}

	patricia_trie() {
		root = new node_type();
	}

	~patricia_trie() {
		delete root;
	}

	void clear() {
		delete root;
		root = new node_type();
	}

	void freePtrs() {
		// This assumes that T is a pointer type
		root->freePtrs();
	}

	T lookup(S key, T def) {
		node_type* node = root;
		while(node != NULL) {
			if(key.empty()) {
				if(node->hasValue) return node->value;
				else return def;
			}

			// Select edge
			edgelist i;
			bool modified = false;
			for(i=node->edges;i != NULL;i = i->next) {
				if(isPrefix(i->first, key)) {
					key.erase(0,i->first.length());
					node = i->second;
					modified = true;
					break;
				}
			}
			if(modified) continue;
			return def;
		}
		return def;
	}

	void insert(S key, T value) {
		node_type* node = root;

		while(node != NULL) {
			if(key.empty()) return;
			if(node->edges == NULL) break;

			// Select edge
			edgelist i;
			bool found_prefix = false;
			for(i=node->edges;i != NULL;i = i->next) {
				if(isPrefix(i->first, key)) {
					// Remove the prefix from the key
					key.erase(0,i->first.length());

					// Traverse to the child and mark that we did
					node = i->second;
					found_prefix = true;
					break;
				}
			}
			if(!found_prefix) break;
		}
		if(node == NULL) {
			printf("\n\aFUCK IT!\n");
			return;
		}

		// We hit a dead end. Search for common prefixes in the outgoing edges
		edgelist i;
		node_type* newNode = new node_type(value);
		int common;
		for(i=node->edges;i != NULL;i = i->next) {
			// Check if this outgoing edge shares a common prefix with the key
			common = countCommonPrefix(i->first, key);

			if(common > 0) {
				// It does. Get the suffix of the existing edge, the prefix, and
				// the suffix of the key.
				S existing_suffix = i->first.substr(common);
				S key_suffix = key.substr(common);
				S prefix = i->first.substr(0,common);

				// If the key is a substring of the existing edge, then just
				// split the edge and make the current target a child of the new
				// node. Otherwise, create a new node with no value to go
				// between the prefix and suffixes
				if(key_suffix.empty()) {
					// Create a new outgoing edge
					newNode->appendEdge(existing_suffix, i->second);

					// Adjust the existing edge
					i->first = prefix;
					i->second = newNode;
				} else {
					// Create an intermediate node
					node_type* middle_node = new node_type();
					node_type* existing_node = i->second;

					// Add it after the prefix
					i->first = prefix;
					i->second = middle_node;

					// Add the two targets as its children
					middle_node->appendEdge(existing_suffix, existing_node);
					middle_node->appendEdge(key_suffix, newNode);
				}
				return;
			}
		}

		// No edges share a prefix. Add the rest of the string as a new edge
		node->appendEdge(key, newNode);
	}
	
	void print() {
		root->print();
	}
};

typedef patricia_trie<uint32_t> ui32patricia; 
typedef patricia_trie<uint32_t, std::wstring> wui32patricia; 
