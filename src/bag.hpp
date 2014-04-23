#pragma once
#include <stdint.h>
#include <stdlib.h>

#define HASH_BITS 16
#define HASH_MASK 0xffff
#define BUCKETS 0xffff // 2^HASH_BITS

struct bucket_entry {
	uint32_t key, parent;
	bucket_entry *next;
};

// Maintains keys in sorted order
struct bucket {
	bucket_entry *first;
	int length;

	bucket() {
		length = 0;
		first = NULL;
	}

	// Make sure there's only one occurrence of each key
	void reduce();

	void takeFrom(bucket& other);
	void insert(uint32_t v, uint32_t p);

	// Inserts half of the elements in this bucket into another bucket.
	void splitInto(bucket* other);

	uint32_t find(uint32_t n);
	void clear();
};

class bag;

class bag_iterator {
	bucket_entry* m_entry;
	bag* m_bag;
	int m_bucket;
public:
	bag_iterator(bag* b, bucket_entry* e, int bg);
	bag_iterator();

	uint32_t& key();
	uint32_t& parent();
	void operator++(int dummy);
	bool operator==(const bag_iterator& other);
	bool operator!=(const bag_iterator& other);
};

// Implements a bag data structure using a hash table.
class bag {
	bucket buckets[BUCKETS];
public:
	bag() {
	}

	bag(const bag& b) {
		for(bag_iterator i=b.begin();i != b.end();i++)
			insert(i.key(), i.parent());
	}

	// Extract all elements into a new bag
	bag* extract() {
		bag* n = new bag();
		for(int i=0;i<BUCKETS;i++) {
			n->buckets[i].first = buckets[i].first;
			n->buckets[i].length = buckets[i].length;
			buckets[i].first = NULL;
			buckets[i].length = 0;
		}
		return n;
	}

	~bag() {
		clear();
	}

	void clear() {
		for(int i=0;i<BUCKETS;i++) buckets[i].clear();
	}

	bucket* getBucket(int n) {
		return &(buckets[n]);
	}

	bag_iterator begin() const {
		for(int i=0;i<BUCKETS;i++)
			if(buckets[i].length > 0)
				return bag_iterator(const_cast<bag*>(this), buckets[i].first, i);
		return bag_iterator();
	}

	bag_iterator end() const {
		return bag_iterator();
	}

	// Gets the parent for a given key, or 0 on failure
	uint32_t find(uint32_t n) {
		uint32_t k = n & HASH_MASK;
		if(buckets[k].length == 0) return 0;
		return buckets[k].find(n);
	}

	void insert(uint32_t n, uint32_t p) {
		uint32_t key = n & HASH_MASK;
		buckets[key].insert(n, p);
	}

	// Insert all entries of another bag into this bag. This clears the other bag.
	void merge(bag& other) {
		for(int i=0;i<BUCKETS;i++)
			buckets[i].takeFrom(other.buckets[i]);
	}

	bag* split() {
		bag* other = new bag();
		for(int i=0;i<BUCKETS;i++)
			buckets[i].splitInto(&other->buckets[i]);
		return other;
	}
};
