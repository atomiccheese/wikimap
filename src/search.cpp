#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <stdexcept>
#include <ctype.h>

#include <algorithm>
#include <map>
#include <utility>
#include <unordered_set>
#include <set>
#include <list>

#include "rbt.hpp"
#include "bytes.hpp"
#include "queue.hpp"

#include <boost/thread.hpp>
#include <boost/chrono.hpp>

#define THREADS 8

using namespace std;
using namespace boost;

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
	typedef RBTree<uint32_t, vector<uint32_t>*, delete_cleanup<vector<uint32_t> > > treetype;
	treetype *cache;
	vector<uint32_t> empty;
	FILE* f;
	uint32_t elements;
	uint32_t n_cached;
	shared_mutex lck;
	mutex diskLock;

	LinkDatabase(FILE* file) : f(file) {
		cache = new treetype();
		fseek(f, 0, SEEK_SET);
		elements = readInt32(f);
		n_cached = 0;
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
		n_cached += nv->size();
		cache->insert(id, nv);
		return *nv;
	}

	size_t rough_expanded() {
		return n_cached;
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
		n_cached += nv->size();
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

struct nodetuple {
	uint32_t node, parent;
	uint32_t distance;
};

void searcher(int n, uint32_t dst, LinkDatabase& dbase, SynchronizedQueue<nodetuple>& input,
		SynchronizedQueue<nodetuple>& output) {
	nodetuple t;
	while(true) {
		try {
			t = input.get(0.1);
		} catch(runtime_error& e) {
			return;
		}
		const vector<uint32_t>& vec = dbase.retrieve(t.node);
		for(vector<uint32_t>::const_iterator i=vec.begin();i != vec.end();i++) {
			output.put({*i, t.node, t.distance+1});
			if(*i == dst) return;
		}
		boost::this_thread::interruption_point();
	}
}

class redirect_map {
	FILE* mapfile;
	size_t entries;

public:
	redirect_map(FILE* f) : mapfile(f) {
		fseek(mapfile, 0, SEEK_END);
		long n = ftell(mapfile);
		entries = n / 8;
	}

	uint32_t resolve(uint32_t elem) {
		uint32_t r = search_impl(elem, entries-1, 0);
		return (r == 0) ? elem : r;
	}

private:
	uint32_t search_impl(uint32_t elem, uint32_t upper, uint32_t lower) {
		// Use a simple binary search
		if(lower > upper) return 0;
		uint32_t searchIdx = lower + ((upper - lower) / 2);
		if(upper == lower) searchIdx = lower;
		
		// Read that entry
		fseek(mapfile, 8*searchIdx, SEEK_SET);
		uint32_t src = readInt32(mapfile);

		// I love tail-call optimization
		if(src == elem) {
			return readInt32(mapfile);
		} else if(src < elem) {
			return search_impl(elem, upper, searchIdx+1);
		} else if(src > elem) {
			return search_impl(elem, searchIdx-1, lower);
		}
		return 0;
	}
};

list<uint32_t> pathfind(uint32_t src, uint32_t dst, LinkDatabase& dbase) {
	// Breadth-first search
	map<uint32_t, pair<uint32_t, uint32_t> > parents;
	set<uint32_t> nextRound;
	SynchronizedQueue<nodetuple> input, output;
	thread* workers[THREADS];
	int depth = 1;

	// Prepopulate the input queue
	{
		const vector<uint32_t>& vec = dbase.retrieve(src);
		for(vector<uint32_t>::const_iterator i=vec.begin();i != vec.end();i++) {
			input.put({*i, src, 1});
			parents[*i] = make_pair(src, 1);
		}
	}

	list<thread*> running;
	bool found_result = false;
	list<uint32_t> path;
	while(!found_result) {
		// Spark threads
		running.clear();
		for(int i=0;i<THREADS;i++) {
			workers[i] = new thread(searcher, i, dst, boost::ref(dbase),
					boost::ref(input), boost::ref(output));
			running.push_back(workers[i]);
		}

		// Wait for threads to finish
		while(true) {
			// Print queue status
			printf("\rI=%18d P=%18d C=%18d A=%2d D=%3d", input.approxSize(),
					output.approxSize(), nextRound.size(),
					running.size(), depth);
			fflush(stdout);

			// Check if all threads are gone
			if(running.empty()) break;

			for(list<thread*>::iterator i=running.begin();i != running.end();) {
				if((*i)->try_join_for(chrono::milliseconds(200))) {
					list<thread*>::iterator itr = i;
					i++;
					running.erase(itr);
					for(int j=0;j<THREADS && workers[j] != (*i);j++);
					break;
				} else i++;
			}

			// Process the output list
			nodetuple t;
			map<uint32_t, pair<uint32_t, uint32_t> >::iterator itr;
			for(int i=0;i<8192;i++) {
				try {
					t = output.get(0.5);
				} catch(runtime_error& e) {
					break;
				}

				// Store parent and queue for next round
				itr = parents.find(t.node);
				if(itr == parents.end() || t.distance < itr->second.second) {
					parents[t.node] = make_pair(t.parent, t.distance);
					nextRound.insert(t.node);
				} else if(itr == parents.end())
					nextRound.insert(t.node);
			}

			// Check whether the result has completed
			if(parents.find(dst) != parents.end()) {
				found_result = true;
				break;
			}
		}
		if(found_result) break;
		putchar('\n');

		// Delete threads
		for(int i=0;i<THREADS;i++) delete workers[i];

		// Make sure all output is processed
		nodetuple t;
		map<uint32_t, pair<uint32_t, uint32_t> >::iterator itr;
		while(true) {
			try {
				t = output.get(0.5);
			} catch(runtime_error& e) {
				break;
			}

			// Store parent and queue for next round
			itr = parents.find(t.node);
			if(itr == parents.end() || t.distance < itr->second.second) {
				parents[t.node] = make_pair(t.parent, t.distance);
				nextRound.insert(t.node);
			} else if(itr == parents.end())
				nextRound.insert(t.node);
			if(output.approxSize() % 1000000 == 0) {
				printf("\rPostprocessing (%8d remain)", output.approxSize());
				fflush(stdout);
			}
		}

		// Collate results
		for(set<uint32_t>::iterator i=nextRound.begin();i != nextRound.end();i++) {
			input.put({*i, parents[*i].first, parents[*i].second});
		}
		depth++;
		nextRound.clear();
		putchar('\n');
	}
	putchar('\n');
	
	for(int i=0;i<THREADS;i++) {
		workers[i]->interrupt();
		workers[i]->join();
		delete workers[i];
	}
	
	if(found_result) {
		// Reconstruct path and print it
		uint32_t elem;
		for(elem=dst;elem != src;) {
			path.push_front(elem);
			elem = parents[elem].first;
		}
		path.push_front(src);
		return path;
	}
	return path;
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
	FILE* f_redirects = fopen("redirects.bin", "rb");
	if((f_names == NULL) ||
			(f_links == NULL) ||
			(f_ids == NULL) ||
			(f_redirects == NULL)) {
		fprintf(stderr, "Cannot open one or more database files\n");
		if(f_names != NULL)	fclose(f_names);
		if(f_links != NULL)	fclose(f_links);
		if(f_ids != NULL)	fclose(f_ids);
		if(f_redirects != NULL)	fclose(f_redirects);
		return 1;
	}

	// Start loading the links database
	LinkDatabase dbase(f_links);
	thread expander(expanderThread, boost::ref(dbase));

	// Load the redirect mapping into memory
	redirect_map redirects(f_redirects);

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

	printf("src=%8d\tdst=%8d\n", src, dst);

	// Resolve redirects
	src = redirects.resolve(src);
	dst = redirects.resolve(dst);

	printf("src=%8d\tdst=%8d\n", src, dst);
	printf("That is, %s -> %s\n", find_name(f_names, src).c_str(),
			find_name(f_names, dst).c_str());

	list<uint32_t> path = pathfind(src, dst, dbase);
	if(!path.empty()) {
		printf("%s -> ", find_name(f_names, src).c_str());
		for(list<uint32_t>::iterator i=++path.begin();i != path.end();i++) {
			printf("%s", find_name(f_names, *i).c_str());
			if(*i != dst) printf(" -> ");
		}
		putchar('\n');
	}

	// Terminate database expander thread
	expander.interrupt();
	expander.join();

	fclose(f_names);
	return 0;

}
