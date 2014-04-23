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

template<typename T>
struct patricia_node {
	typedef patricia_list<std::string, patricia_node<T>*>* edgelist;
	T value;
	bool hasValue;
	edgelist edges;
	unsigned int nEdges;

	patricia_node() : hasValue(false) {
		edges = NULL;
		nEdges = 0;
	}

	patricia_node(T v) : value(v), hasValue(true) {
		edges = NULL;
		nEdges = 0;
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
		std::list<patricia_node<T>*> stack;
		stack.push_back(this);
		while(!stack.empty()) {
			patricia_node<T>* elem = stack.back();
			if(elem == NULL) continue;
		}
		for(edgelist i = edges;i != NULL;i = i->next) {
			if(i->second == this) return true;
		}
		return false;
	}

	void appendEdge(std::string s, patricia_node<T>* n) {
		if(edges != NULL) {
			edges->append(s,n);
		} else {
			edges = new patricia_list<std::string, patricia_node<T>*>(s,n);
		}
		nEdges++;
	}
};

void warnCycle() {
	printf("BREAK - CYCLE DETECTED\n");
}

template<typename T>
struct patricia_trie {
	typedef patricia_list<std::string, patricia_node<T>*>* edgelist;
	patricia_node<T>* root;

	bool isPrefix(std::string pfx, std::string val) {
		return countCommonPrefix(pfx,val) == pfx.length();
	}

	int countCommonPrefix(std::string pfx, std::string val) {
		std::string::iterator i,j;
		int n = 0;
		for(i=pfx.begin(),j=val.begin();i != pfx.end();i++,j++,n++)
			if(*i != *j) return n;
		return n;
	}

	patricia_trie() {
		root = new patricia_node<T>();
	}

	~patricia_trie() {
		delete root;
	}

	T lookup(std::string key, T def) {
		patricia_node<T>* node = root;
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

	void insert(std::string key, T value) {
		patricia_node<T>* node = root;

		while(node != NULL) {
			if(key.empty()) return;
			if(node->edges == NULL) break;

			// Select edge
			edgelist i;
			bool adjusted = false;
			for(i=node->edges;i != NULL;i = i->next) {
				if(isPrefix(i->first, key)) {
					key.erase(0,i->first.length());
					node = i->second;
					adjusted = true;
					break;
				}
			}
			if(!adjusted) break;
		}

		// We hit a dead end. Search for common prefixes in the outgoing edges
		edgelist i;
		patricia_node<T>* newNode = new patricia_node<T>(value);
		int common;
		for(i=node->edges;i != NULL;i = i->next) {
			if((common = countCommonPrefix(i->first, key)) > 0) {
				// Initial state
				// [A]
				//  | prefix
				//  V
				// [B]
				
				// After state
				// [A]
				//  | prefix
				//  V
				// [C] middle node
				//  |---\
				//  V   V
				// [B] [D]
				
				// A = node
				// B = i->second
				// C = middleNode
				// D = newNode
				
				// Add a node in the middle
				std::string remainder = i->first.substr(common);
				std::string newEdge = key.substr(common);
				std::string newStarting = i->first.substr(0,common);

				// Check whether the remaining prefix would be empty, and
				// if not, then insert a middle node. Otherwise,
				// just attach the new nodes to the starting
				// node, and rewrite the edge name
				if(newStarting.empty()) {
					i->first = remainder;
					node->appendEdge(newEdge, newNode);
				} else {
					// It's not empty, so just clip it and attach
					// new nodes to an intermediary
					i->first = i->first.substr(0,common);
					patricia_node<T>* middleNode = new patricia_node<T>();
					middleNode->appendEdge(newEdge, newNode);
					middleNode->appendEdge(remainder, i->second);
					i->second = middleNode;
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
