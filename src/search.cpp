#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <stdexcept>
#include <ctype.h>

#include <algorithm>
#include <map>
#include <utility>
#include <set>
#include <list>

#include "rbt.hpp"
#include "bytes.hpp"
//#include "patricia.hpp"
#include "bag.hpp"

#include <boost/thread.hpp>
#include <boost/chrono.hpp>

#define THREADS 4

using namespace std;
using namespace boost;

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

uint32_t lookupName(FILE* f, string tgtName) {
	uint32_t addr = 0;
	char buffer[0x10000];
	std::less<string> comparer;
	while(true) {
		// Read a node header and determine which direction to go in
		fseek(f, addr, SEEK_SET);
		uint16_t nameLen = readInt16(f);
		fread(buffer, nameLen, 1, f);
		string name = string(buffer, nameLen);
		uint32_t pl = readInt32(f);
		uint8_t childInfo;
		fread(&childInfo, 1, 1, f);

		int dir = 0;
		if(!comparer(tgtName, name) && !comparer(name, tgtName)) return pl;
		else if(comparer(name, tgtName)) dir = -1;
		else dir = 1;

		uint32_t left = 0,
			right = 0;
		if((childInfo & 1) > 0) left = readInt32(f);
		if((childInfo & 2) > 0) right = readInt32(f);

		if(dir == -1) addr = right;
		else addr = left;
		if(addr == 0) return 0;
	}
}

struct LinkDatabase {
	RBTree<uint32_t, vector<uint32_t>* > *cache;
	vector<uint32_t> empty;
	FILE* f;
	uint32_t elements;
	shared_mutex lck;
	mutex diskLock;

	LinkDatabase(FILE* file) : f(file) {
		cache = new RBTree<uint32_t, vector<uint32_t>* >();
		fseek(f, 0, SEEK_SET);
		elements = readInt32(f);
	}

	~LinkDatabase() {
		fclose(f);
		if(cache != NULL) delete cache;
	}

	const vector<uint32_t>& retrieve(uint32_t id) {
		upgrade_lock<shared_mutex> readLock(lck);

		// Check boundaries
		if(id > elements) return empty;

		// Check cache first
		if(cache->contains(id)) return *cache->search(id);

		// Check disk backing store
		vector<uint32_t>* nv = new vector<uint32_t>();
		{
			lock_guard<mutex> disklck(diskLock);
			fseek(f,sizeof(uint32_t)*id,SEEK_SET);
			uint32_t dataOffset = readInt32(f);
			fseek(f,dataOffset,SEEK_SET);
			uint32_t nLinks = readInt32(f);
			for(uint32_t i=0;i<nLinks;i++) {
				nv->push_back(readInt32(f));
			}
		}

		upgrade_to_unique_lock<shared_mutex> writeLock(readLock);
		cache->insert(id, nv);
		return *nv;
	}

	void try_expand(uint32_t n) {
		upgrade_lock<shared_mutex> readLock(lck);

		if(n > elements || cache->contains(n)) return;

		vector<uint32_t>* nv = new vector<uint32_t>();
		{
			lock_guard<mutex> disklck(diskLock);
			fseek(f,sizeof(uint32_t)*n,SEEK_SET);
			uint32_t dataOffset = readInt32(f);
			fseek(f,dataOffset,SEEK_SET);
			uint32_t nLinks = readInt32(f);
			for(uint32_t i=0;i<nLinks;i++)
				nv->push_back(readInt32(f));
		}

		upgrade_to_unique_lock<shared_mutex> writeLock(readLock);
		cache->insert(n, nv);
	}
};

void expanderThread(LinkDatabase& db) {
	uint32_t elems = db.elements;
	for(uint32_t i=1;i<elems;i++) {
		db.try_expand(i);
		this_thread::interruption_point();
	}
}

string find_name(FILE* f, uint32_t id) {
	fseek(f,0,SEEK_SET);
	uint32_t nItems = readInt32(f);
	if(id > nItems) return "--==<<INVALID ITEM IDENTIFIER>>==--";
	fseek(f,(sizeof(uint32_t)*id),SEEK_SET);
	uint32_t nameAddr = (readInt32(f)) + 4 + nItems*sizeof(uint32_t);
	fseek(f,nameAddr,SEEK_SET);
	uint16_t nameLen = readInt16(f);
	char name[nameLen];
	fread(name, nameLen, 1, f);
	return string(name, nameLen);
}

inline void tolower(char* s) {
	for(;*s != '\0';s++) *s = tolower(*s);
}

struct result {
	bool valid;
	int n;
	mutex lck;
};

void searcher(int n, uint32_t dst, LinkDatabase& dbase, bag& input, bag& output, result& r) {
	for(bag_iterator itr=input.begin();itr != input.end();itr++) {
		uint32_t k = itr.key();

		const vector<uint32_t>& vec = dbase.retrieve(k);
		for(vector<uint32_t>::const_iterator i=vec.begin();i != vec.end();i++) {
			output.insert(*i, k);
			if(*i == dst) {
				r.lck.lock();
				if(!r.valid) r.n = n;
				r.valid = true;
				r.lck.unlock();
				return;
			}
		}
		boost::this_thread::interruption_point();
	}
}

// Run a breadth-first search of the tree for a path between the two nodes
// named on the command line.
int main(int argc, char **argv) {
	if(argc != 3) {
		fprintf(stderr, "Usage: %s [source] [dest]\n", argv[0]);
		return 1;
	}

	// Try opening the databases
	FILE* f_names = fopen("id_name.bin", "rb");
	FILE* f_links = fopen("id_links.bin", "rb");
	FILE* f_ids = fopen("name_id.bin", "rb");
	if((f_names == NULL) ||
			(f_links == NULL) ||
			(f_ids == NULL)) {
		fprintf(stderr, "Cannot open one or more database files\n");
		if(f_names != NULL) fclose(f_names);
		if(f_links != NULL) fclose(f_links);
		if(f_ids != NULL) fclose(f_ids);
		return 1;
	}

	// Start loading the links database
	LinkDatabase dbase(f_links);
	thread expander(expanderThread, ref(dbase));

	// Load the name trie and dereference the names
	uint32_t src, dst;
	tolower(argv[1]); tolower(argv[2]);
	src = lookupName(f_ids, argv[1]);
	dst = lookupName(f_ids, argv[2]);
	fclose(f_ids);
	if(src == 0) {
		fprintf(stderr, "Unable to find node: %s\n", argv[1]);
		expander.interrupt();
		expander.join();

		fclose(f_names);
		return 1;
	} else if(dst == 0) {
		fprintf(stderr, "Unable to find node: %s\n", argv[2]);
		expander.interrupt();
		expander.join();
	
		fclose(f_names);
		return 1;
	}
	printf("%d %d\n", src, dst);

	// Breadth-first search
	list<bag> previousBags;
	bag collated;
	bag* inputBags[THREADS];
	bag outputBags[THREADS];
	thread* workers[THREADS];
	int depth = 1;
	result res;
	res.valid = false;

	// Prepopulate the collated list
	{
		const vector<uint32_t>& vec = dbase.retrieve(src);
		for(vector<uint32_t>::const_iterator i=vec.begin();i != vec.end();i++)
			collated.insert(*i, src);
	}

	list<thread*> running;
	bool found_result = false;
	list<uint32_t> path;
	while(!found_result) {
		previousBags.push_back(collated);

		// Split collated list into sublists for each thread. This will
		// have to be changed for alternate values of THREADS
		inputBags[0] = collated.split();
		inputBags[1] = new bag(collated);
		inputBags[2] = inputBags[0]->split();
		inputBags[3] = inputBags[1]->split();

		// Spark threads
		running.clear();
		for(int i=0;i<THREADS;i++) {
			outputBags[i].clear();
			workers[i] = new thread(searcher, i, dst, ref(dbase),
					ref(*inputBags[i]), ref(outputBags[i]), ref(res));
			running.push_back(workers[i]);
		}

		// Wait for threads to finish
		while(true) {
			// Print status

			// Check if all threads are gone
			if(running.empty()) break;

			for(list<thread*>::iterator i=running.begin();i != running.end();) {
				if((*i)->try_join_for(chrono::seconds(1))) {
					list<thread*>::iterator itr = i;
					i++;
					running.erase(itr);
					int j;
					for(j=0;j<THREADS && workers[j] != (*i);j++);
					break;
				} else i++;
			}

			// Check whether the result has completed
			res.lck.lock();
			if(res.valid) {
				// Terminate all threads
				for(list<thread*>::iterator i=running.begin();i != running.end();i++) {
					(*i)->interrupt();
					(*i)->join();
				}
				res.lck.unlock();

				// Get the initial bag
				previousBags.push_back(outputBags[res.n]);
				list<bag>::reverse_iterator bagItr = previousBags.rbegin();

				// Reconstruct the path
				uint32_t n = dst;
				path.push_front(n);
				for(;bagItr != previousBags.rend();bagItr++) {
					n = bagItr->find(n);
					path.push_front(n);
				}

				// And finish
				found_result = true;
				break;
			}
			res.lck.unlock();
		}
		if(found_result) break;

		// Delete threads
		for(int i=0;i<THREADS;i++) delete workers[i];

		// Collate results
		for(int i=0;i<THREADS;i++)
			collated.merge(outputBags[i]);
		
		// Print depth
		printf("\nMoving to depth %d\n", ++depth);
		fflush(stdout);
	}
	putchar('\n');
	
	// Print output
	for(list<uint32_t>::iterator i=path.begin();i != path.end();i++) {
		printf("%s", find_name(f_names, *i).c_str());
		if(*i != dst) printf(" -> ");
	}
	putchar('\n');

	// Terminate database expander thread
	expander.interrupt();
	expander.join();

	fclose(f_names);
	return 0;

}
