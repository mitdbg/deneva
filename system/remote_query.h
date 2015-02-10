#ifndef _REMOTE_QUERY_H_
#define _REMOTE_QUERY_H_

#include "global.h"
#include "helper.h"

#define MAX_DATA_SIZE 100

class tpcc_query;

enum RemReqType {RLK, RULK, RQRY, RLK_RSP, RULK_RSP, RQRY_RSP};

// different for different CC algos?
// read/write
struct r_query {
public:
	RemReqType rtype;
	uint64_t len;
	uint64_t return_id;
	uint64_t txn_id;
	uint64_t part_cnt;
	uint64_t * parts;
	char * data;
};


class Remote_query {
public:
	void init();
	void unpack(r_query * query, char * data);
	int q_idx;

};
#endif
