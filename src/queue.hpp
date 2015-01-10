#pragma once
#include <boost/thread.hpp>
#include <queue>
#include <stdexcept>

/** \brief A thread-safe wrapper for std::queue
 * \tparam T The class type contained within
 */
template<class T>
struct SynchronizedQueue {
	typedef boost::chrono::duration<double> dsecs;
public:
	SynchronizedQueue() {
	}

	// All timeout methods raise std::runtime_error on timeout
	bool empty(float timeout=-1) {
		if(acquire(timeout)) throw std::runtime_error("Timeout");
		bool is = m_entry.empty();
		release();
		return is;
	}

	size_t size(float timeout=-1) {
		if(acquire(timeout)) throw std::runtime_error("Timeout");
		size_t s = m_entry.size();
		release();
		return s;
	}

	size_t approxSize() {
		return m_entry.size();
	}

	T get(float timeout=-1) {
		if(acquire(timeout)) throw std::runtime_error("Timeout");
		boost::unique_lock<boost::mutex> lock(m_cMutex);
		while(m_entry.empty()) {
			release();
			if(boost::cv_status::timeout == m_cond.wait_for(lock, dsecs(timeout)))
				throw std::runtime_error("Timeout");
			if(acquire(timeout)) throw std::runtime_error("Timeout");
		}
		T out = m_entry.front();
		m_entry.pop();
		release();
		return out;
	}

	void put(T obj, float timeout=-1) {
		if(acquire(timeout)) throw std::runtime_error("Timeout");
		m_entry.push(obj);
		boost::lock_guard<boost::mutex> lock(m_cMutex);
		m_cond.notify_one();
		release();
	}

private:
	SynchronizedQueue(SynchronizedQueue<T>& q) {
	}

	// If timeout is below 0, no timeout
	// Returns whether timeout expired
	bool acquire(float timeout=-1) {
		if(timeout < 0) {
			m_mutex.lock();
		} else if(timeout == 0) {
			return !m_mutex.try_lock();
		} else {
			return !m_mutex.try_lock_for(dsecs(timeout));
		}
		return false;
	}

	void release() {
		m_mutex.unlock();
	}

	boost::timed_mutex m_mutex;
	boost::condition_variable m_cond;
	boost::mutex m_cMutex;
	std::queue<T> m_entry;
};

