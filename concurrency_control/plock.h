#ifndef _PLOCK_H_
#define _PLOCK_H_

#include "global.h"
#include "helper.h"
#include "remote_query.h"
#include "query.h"

class txn_man;

// Parition manager for HSTORE
class PartMan {
public:
	void init();
	RC lock(txn_man * txn);
	void unlock(txn_man * txn);
	void unpack(base_query * query, char * data);
	void pack(void ** data, void * size);
private:
	pthread_mutex_t latch;
	txn_man * owner;
	txn_man ** waiters;
	UInt32 waiter_cnt;
};

// Partition Level Locking
class Plock {
public:
	void init();
	// lock all partitions in parts
	RC lock(txn_man * txn, uint64_t * parts, uint64_t part_cnt);
	void unlock(txn_man * txn, uint64_t * parts, uint64_t part_cnt);
private:
	PartMan ** part_mans;
};

#endif
