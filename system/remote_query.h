#ifndef _REMOTE_QUERY_H_
#define _REMOTE_QUERY_H_

#include "global.h"
#include "helper.h"

#define MAX_DATA_SIZE 100

// different for different CC algos?
// read/write
struct r_query {
	RemReqType rtype;
	uint64_t return_id;
	uint64_t part_id;
	uint64_t txn_id;
	uint64_t ts; //timestamp
	uint64_t address;
	char data[MAX_DATA_SIZE];
};

enum RemReqType {LOCK, UNLOCK, IRD, IWR, RD, WR, LOCK_RSP, UNLOCK_RSP, RD_RSP, WR_RSP};

class Remote_query_queue {
public:
	void init();
	r_query * get_next_query(uint64_t thd_id); 
	void add_query(uint64_t thd_id, r_query * query); 
	void add_query(const char * buf); 

};
#endif
