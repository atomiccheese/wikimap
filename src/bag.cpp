#include "bag.hpp"

uint32_t& bag_iterator::key() {
	return m_entry->key;
}

uint32_t& bag_iterator::parent() {
	return m_entry->parent;
}

bag_iterator::bag_iterator(bag* b, bucket_entry* e, int bg) {
	m_bag = b;
	m_entry = e;
	m_bucket = bg;
}

bag_iterator::bag_iterator() {
	m_bag = NULL;
	m_entry = NULL;
	m_bag = 0;
}

void bag_iterator::operator++(int dummy) {
	if(m_bag == NULL) return;
	if(m_entry->next != NULL) {
		m_entry = m_entry->next;
	} else {
		for(m_bucket++;m_bucket < BUCKETS;m_bucket++) {
			if(m_bag->getBucket(m_bucket) == NULL) continue;
			m_entry = m_bag->getBucket(m_bucket)->first;
			if(m_entry != NULL) return;
		}
		m_bag = NULL;
		m_bucket = 0;
		m_entry = NULL;
	}
}

bool bag_iterator::operator==(const bag_iterator& other) {
	if(other.m_bag == NULL) return m_bag == NULL;
	return (other.m_bucket == m_bucket) && (other.m_entry == m_entry);
}

bool bag_iterator::operator!=(const bag_iterator& other) {
	return !(*this == other);
}

void bucket::reduce() {
	bucket_entry* e = first;
	bucket_entry* last = NULL;
	while(e != NULL) {
		if(last != NULL && last->key == e->key) {
			bucket_entry* tmp = e->next;
			delete e;
			e = tmp;
		} else {
			last = e;
			e = e->next;
		}
	}

	length = 0;
	for(e=first;e != NULL;length++,e=e->next);
}

void bucket::takeFrom(bucket& other) {
	for(bucket_entry* e=other.first;e != NULL;e=e->next)
		insert(e->key, e->parent);
}

void bucket::insert(uint32_t v, uint32_t p) {
	bucket_entry* e = new bucket_entry();
	e->key = v;
	e->parent = p;
	if(length == 0) {
		e->next = NULL;
		first = e;
		length = 1;
		return;
	} else if(length == 1) {
		if(v < first->key) {
			e->next = first;
			first = e;
		} else if(v > first->key) {
			first->next = e;
		} else {
			delete e;
			return;
		}
		length++;
		return;
	}

	// Figure out where it goes and insert it
	bucket_entry *i, *last;
	i = first;
	while((i->next != NULL) && (i->next->key <= e->key)) i = i->next;
	if(i != NULL && i->key == e->key) {
		delete e;
		return;
	}

	e->next = i->next;
	i->next = e;
	length++;
}

void bucket::splitInto(bucket* other) {
	if((length == 0) || (length == 1)) return;

	uint32_t max = length / 2;
	bucket_entry* pfirst = first;
	for(uint32_t i=0;i<max;i++) {
		if(first == NULL) break;
		other->insert(first->key, first->parent);
		first = first->next;
		length--;
	}

	// Clean up deleted elements
	for(;pfirst != first;) {
		bucket_entry* tmp = pfirst->next;
		delete pfirst;
		pfirst = tmp;
	}

	if(first == NULL) length = 0;
}

uint32_t bucket::find(uint32_t n) {
	for(bucket_entry* i=first;i != NULL;i++)
		if(n == i->key) return i->parent;
	return 0;
}

void bucket::clear() {
	bucket_entry* i = first;
	bucket_entry* tmp;
	while(i != NULL) {
		tmp = i->next;
		delete i;
		i = tmp;
	}
	length = 0;
	first = NULL;
}
