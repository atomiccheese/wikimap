#include <stdio.h>
#include <ctype.h>
#include <stdint.h>
#include <stdexcept>
#include <map>
#include <list>
#include <string>
#include <algorithm>

#include "aqueue.hpp"
#include "rbt.hpp"
#include "bytes.hpp"

using namespace std;

// Phase 2 page - assigned an ID, and has localized links
struct p2_page {
	unsigned int wikiID;
	bool redirect;
	list<unsigned int> links;
};

uint16_t readInt16(FILE* f) {
	uint16_t out;
	fread(&out, sizeof(out), 1, f);
	out = swap16(out);
	return out;
}

uint32_t readInt32(FILE* f) {
	uint32_t out;
	fread(&out, sizeof(out), 1, f);
	out = swap32(out);
	return out;
}

uint64_t readInt64(FILE* f) {
	uint64_t out;
	fread(&out, sizeof(out), 1, f);
	out = swap64(out);
	return out;
}

void writeInt16(uint16_t d, FILE* f) {
	d = swap16(d);
	fwrite(&d, sizeof(d), 1, f);
}

void writeInt32(uint32_t d, FILE* f) {
	d = swap32(d);
	fwrite(&d, sizeof(d), 1, f);
}

void writeInt64(uint64_t d, FILE* f) {
	d = swap64(d);
	fwrite(&d, sizeof(d), 1, f);
}

void tolower(char* s) {
	for(;*s != '\0';s++) *s = tolower(*s);
}

/*
void serializeTrie(patricia_node<uint32_t>* trie, FILE* f) {
	list<patricia_node<uint32_t>*> stack;
	stack.push_back(trie);

	uint32_t processed = 0;
	while(!stack.empty()) {
		patricia_node<uint32_t>* elem = stack.back();
		stack.pop_back();
		if(elem == NULL) {
			printf("\nWarning: Null element in patricia trie\n");
			continue;
		}
		uint8_t hasvalue;
		if(elem->hasValue) {
			hasvalue = 1;
			fwrite(&hasvalue, 1, 1, f);
			writeInt32(elem->value, f);
			//printf("Writing patricia node with value %u\n", elem->value);
		} else {
			//printf("Writing patricia node without value\n");
			hasvalue = 0;
			fwrite(&hasvalue, 1, 1, f);
		}
		//printf("Writing %d edges\n", elem->nEdges);
		writeInt32(elem->nEdges, f);
		for(patricia_list<string, patricia_node<uint32_t>*>* i=elem->edges;i != NULL;i = i->next) {
			//printf(" Writing node '%s' with len=%u and %u children\n",
					//i->first.c_str(), i->first.length(), i->second->nEdges);
			writeInt16(i->first.length(), f);
			fwrite(i->first.c_str(), i->first.length(), 1, f);
			if(find(stack.begin(), stack.end(), i->second) != stack.end()) {
				printf("\n-----------------\nCRITICAL WARNING\n----------------\nTrie contains a loop!\n");
				fflush(stdout);
			}
			stack.push_back(i->second);
		}
		if(++processed % 2000 == 0) {
			printf("\rWriting PATRICIA trie - %12u nodes written        ", processed);
			fflush(stdout);
		}
	}
}
*/

void serializeBinaryTree(RBTree<string, uint32_t>* elem, FILE* f) {
	putchar('\n');
	list<RBNode<string, uint32_t>* > queue;
	map<uint32_t, uint32_t> requestedRewrites, elementLocations;

	queue.push_back(elem->getRoot());
	uint32_t processed = 0;

	// Write the elements
	while(!queue.empty()) {
		RBNode<string, uint32_t>* elem = queue.front();
		queue.pop_front();

		// Note the location
		elementLocations[elem->payload] = ftell(f);
		
		// Write the string and payload
		writeInt16(elem->value.size(), f);
		fwrite(elem->value.c_str(), elem->value.size(), 1, f);
		writeInt32(elem->payload, f);

		// Write child info
		uint8_t childInfo = 0;
		if(elem->left != NULL) childInfo |= 1;
		if(elem->right != NULL) childInfo |= 2;
		fwrite(&childInfo, 1, 1, f);

		// Write the placeholders for the children and add them to the
		// rewrite request queue
		if(elem->left != NULL) {
			requestedRewrites[elem->left->payload] = ftell(f);
			writeInt32(0, f);
			queue.push_back(elem->left);
		}
		if(elem->right != NULL) {
			requestedRewrites[elem->right->payload] = ftell(f);
			writeInt32(0, f);
			queue.push_back(elem->right);
		}

		if(++processed % 10000 == 0) {
			printf("\rWriting RBTree - %d nodes done", processed);
			fflush(stdout);
		}
	}

	// Fix up the pointers
	putchar('\n');
	processed = 0;
	for(map<uint32_t, uint32_t>::iterator i=requestedRewrites.begin();i != requestedRewrites.end();i++) {
		fseek(f, i->second, SEEK_SET);
		writeInt32(elementLocations[i->first], f);
		if(++processed % 10000 == 0) printf("\rCorrecting forward references: %d refs done", processed);
	}
}

void writeIdsNames(char* fname) {
	FILE* f = fopen(fname, "rb");
	RBTree<unsigned int, string> idsNames;
	char strBuf[0x10000];
	uint8_t type;
	uint16_t titleLen;
	uint32_t identifier;
	uint16_t targetLen;
	uint64_t nLinks;
	uint32_t currentID = 1;
	int printPhase = 0;
	while(fread(&type, 1, 1, f) == 1) {
		// Read header fields
		titleLen = readInt16(f);
		identifier = readInt32(f);
		if(type == 0) { // Page
			nLinks = readInt32(f);

			fread(strBuf, 1, titleLen, f);
			strBuf[titleLen] = 0;
			tolower(strBuf);
			string title(strBuf);

			idsNames.insert(currentID, title);

			for(uint64_t j=0;j<nLinks;j++) {
				targetLen = readInt16(f);
				fread(strBuf, 1, targetLen, f);
			}
		} else { // Redirect
			// Read data
			targetLen = readInt16(f);

			fread(strBuf, 1, titleLen, f);
			tolower(strBuf);
			strBuf[titleLen] = 0;
			string title(strBuf);

			fread(strBuf, 1, targetLen, f);

			idsNames.insert(currentID, title);
		}

		// Print progress
		currentID++;
		if(++printPhase == 10000) {
			printf("\rBuilding ID->Name mapping: %12d records processed", currentID-1);
			fflush(stdout);
			printPhase = 0;
			//if(phase1Pages.size() > 10000) break;
		}
	}
	
	// Serialize ID->name mapping
	f = fopen("id_name.bin", "wb");
	writeInt32(currentID-1, f);
	uint32_t vseek = 0;
	printf("\n");
	for(unsigned int i=0;i<currentID;i++) {
		string name = (idsNames.contains(i+1)) ? idsNames.search(i+1) : "";
		writeInt32(vseek, f);
		vseek += (2 + name.length());
		if(i % 200 == 0) {
			printf("\rWriting ID->Name header: %12d entries written", i);
			fflush(stdout);
		}
	}
	for(unsigned int i=0;i<currentID;i++) {
		if(idsNames.contains(i+1)) {
			string name = idsNames.search(i+1);
			writeInt16(name.length(), f);
			fwrite(name.c_str(), name.length(), 1, f);
		} else {
			writeInt16(0,f);
		}
		if(i % 200 == 0) {
			printf("\rWriting ID->Name records: %12d entries written", i);
			fflush(stdout);
		}
	}
	fclose(f);
	printf("\n");
}

uint32_t writeNamesIds(char* fname, RBTree<string, uint32_t>& trie) {
	FILE* f = fopen(fname, "rb");
	char strBuf[0x10000];
	uint8_t type;
	uint16_t titleLen;
	uint32_t identifier;
	uint16_t targetLen;
	uint64_t nLinks;
	uint32_t currentID = 1;
	int printPhase = 0;
	while(fread(&type, 1, 1, f) == 1) {
		// Read header fields
		titleLen = readInt16(f);
		identifier = readInt32(f);
		if(type == 0) { // Page
			nLinks = readInt32(f);

			fread(strBuf, 1, titleLen, f);
			strBuf[titleLen] = 0;
			tolower(strBuf);
			string title(strBuf);

			if(!trie.contains(title))
				trie.insert(title, currentID);

			for(uint64_t j=0;j<nLinks;j++) {
				targetLen = readInt16(f);
				fread(strBuf, 1, targetLen, f);
			}
		} else { // Redirect
			// Read data
			targetLen = readInt16(f);

			fread(strBuf, 1, titleLen, f);
			strBuf[titleLen] = 0;
			tolower(strBuf);
			string title(strBuf);

			fread(strBuf, 1, targetLen, f);

			if(!trie.contains(title))
				trie.insert(title, currentID);
		}

		// Print progress
		currentID++;
		if(++printPhase == 10000) {
			printf("\rBuilding Name->ID mapping: %12d records processed", currentID-1);
			fflush(stdout);
			printPhase = 0;
			// if(currentID > 3000000) break;
		}
	}
	
	// Serialize name trie
	f = fopen("name_id.bin", "wb");
	serializeBinaryTree(&trie, f);
	fclose(f);

	printf("\n");
	return currentID;
}

int main(int argc, char **argv) {
	if(argc != 2) {
		fprintf(stderr, "Usage: %s [page stream dump]\n", argv[0]);
		return 1;
	}

	// Parse the pagestream dump and emit id_name.bin
	//writeIdsNames(argv[1]);

	// Parse the pagestream dump and emit name_id.bin
	RBTree<string, uint32_t> namesIDs;
	uint32_t maxID = writeNamesIds(argv[1], namesIDs);

	uint32_t x1 = namesIDs.search("aardvark");
	uint32_t x2 = namesIDs.search("zebra");
	printf("%d\n%d\n", x1, x2);

	// Write out the id->links mapping
	// First emit an empty header
	FILE* of = fopen("id_links.bin", "wb");
	writeInt32(maxID-1, of);
	for(uint32_t i=0;i<maxID;i++)
		fwrite("\x00\x00\x00\x00", 4, 1, of);

	// Start reading the page stream and emit pages as they come
	FILE* f = fopen(argv[1], "rb");
	char strBuf[0x10000];
	uint8_t type;
	uint16_t titleLen;
	uint32_t identifier;
	uint16_t targetLen;
	uint64_t nLinks;
	uint32_t currentID = 1;
	int printPhase = 0;
	while(fread(&type, 1, 1, f) == 1) {
		// Store the address
		long fpos = ftell(of);
		fseek(of, 4*(currentID), SEEK_SET);
		writeInt32(fpos, of);
		fseek(of, fpos, SEEK_SET);
		
		// Read header fields
		titleLen = readInt16(f);
		identifier = readInt32(f);
		if(type == 0) { // Page
			nLinks = readInt32(f);

			fread(strBuf, 1, titleLen, f);
			strBuf[titleLen] = 0;

			long nLinksPos = ftell(of);
			writeInt32(0, of);
			uint32_t nvalid = 0;
			for(uint64_t j=0;j<nLinks;j++) {
				targetLen = readInt16(f);
				fread(strBuf, 1, targetLen, f);
				strBuf[targetLen] = 0;
				tolower(strBuf);
				string target(strBuf);
				try {
					uint32_t lval = namesIDs.search(target);
					nvalid++;
					writeInt32(lval, of);
				} catch(std::runtime_error e) {
				}
			}

			// Go back and update the header field
			long tmp = ftell(of);
			fseek(of, nLinksPos, SEEK_SET);
			writeInt32(nvalid, of);
			fseek(of, tmp, SEEK_SET);
		} else { // Redirect
			// Read data
			targetLen = readInt16(f);

			fread(strBuf, 1, titleLen, f);
			strBuf[titleLen] = 0;

			fread(strBuf, 1, targetLen, f);
			strBuf[targetLen] = 0;
			tolower(strBuf);
			string target(strBuf);

			// Create the page
			try {
				uint32_t lval = namesIDs.search(target);
				writeInt32(1, of);
				writeInt32(lval, of);
			} catch(std::runtime_error e) {
				writeInt32(0, of);
			}
		}

		// Print progress
		currentID++;
		if(++printPhase == 2000) {
			printf("\rWriting links: %12d records processed", currentID);
			fflush(stdout);
			printPhase = 0;
			//if(phase1Pages.size() > 10000) break;
		}
	}
	fclose(f);
	fclose(of);
	printf("\nProcessed %u records\n", currentID-1);
}
