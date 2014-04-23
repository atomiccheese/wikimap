#pragma once

#include <boost/atomic.hpp>

template<typename T>
struct synclist_item {
	T value;
	boost::atomic<synclist_item*> prev, next;
};

/* A synchronized list that guarantees that multiple threads can safely write to
 * and iterate over it without worrying about synchronization. It gives priority
 * to writers over readers. However, this guarantee is invalidated if deletion is
 * performed. If the element represented by a given iterator is deleted, the
 * iterator is invalidated, and any attempts to use it will result in an exception
 * being thrown. */
template<typename T>
class synclist {
	friend class iterator;
	boost::atomic<synclist_item<T>*> m_first, m_last;
	boost::atomic<uint32_t> m_length;
public:
	class iterator {
		friend class synclist;
		synclist_item<T>* contained;
		iterator(synclist_item<T>* v) : contained(v) {
		}

		void check_invalidated() {
		}
	public:
		iterator& operator++() {
			check_invalidated();
			if(contained == NULL) return *this;
			contained = contained->next.load(boost::memory_order_release);
			return *this;
		}

		iterator& operator--() {
			check_invalidated();
			if(contained == NULL) return *this;
			contained = contained->prev.load(boost::memory_order_release);
			return *this;
		}

		bool operator==(iterator i) {
			check_invalidated();
			return (contained == i.contained);
		}

		bool operator!=(iterator i) {
			check_invalidated();
			return (contained != i.contained);
		}

		T& operator*() {
			return contained->value;
		}

		T* operator->() {
			return &(contained->value);
		}
	};
public:
	synclist() {
		m_first.store(NULL,boost::memory_order_acquire);
		m_last.store(NULL,boost::memory_order_acquire);
		m_length.store(0,boost::memory_order_acquire);
	}

	~synclist() {
	}

	void push_back(T elem) {
		// Construct an element to hold it
		synclist_item<T>* itm = new synclist_item<T>();
		itm->value = elem;
		itm->prev.store(m_last.load(boost::memory_order_release), boost::memory_order_acquire);
		itm->next.store(NULL, boost::memory_order_acquire);

		// Insert the element in the list
		synclist_item<T>* tmpItm = itm;
		synclist_item<T>* prevLast = m_last.exchange(tmpItm, boost::memory_order_consume);
		tmpItm = itm;
		synclist_item<T>* null = NULL;
		m_first.compare_exchange_strong(null, tmpItm, boost::memory_order_consume, boost::memory_order_acquire);
		if(prevLast != NULL) {
			prevLast->next.store(itm, boost::memory_order_consume);
		}
		m_length.fetch_add(1, boost::memory_order_consume);
	}

	uint32_t size() {
		return m_length.load(boost::memory_order_release);
	}

	iterator begin() {
		return iterator(m_first.load(boost::memory_order_release));
	}

	iterator end() {
		return iterator(NULL);
	}
};
