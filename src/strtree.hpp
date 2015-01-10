#pragma once
#include <stdio.h>
#include <stdint.h>
#include <string>

struct strtree_node {
	strtree_node* children[0x100];
	uint8_t is_leaf;
	uint32_t value;

	strtree_node();
	~strtree_node();
};


class strtree {
	strtree_node* root;
	typedef std::basic_string<uint8_t> ustring;
public:
	strtree();
	strtree(FILE* f); // Load
	~strtree();

	// Accessors
	bool has(ustring s);
	void set(ustring s, uint32_t v);
	uint32_t get(ustring s);

	// Serialization
	void serialize(FILE* outFile);
};
