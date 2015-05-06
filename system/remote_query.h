#ifndef _REMOTE_QUERY_H_
#define _REMOTE_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "txn_pool.h"

#define MAX_DATA_SIZE 100 
class tpcc_query;
class tpcc_txn_man;
class txn_man;
class base_query;


// different for different CC algos?
// read/write
/*
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
*/

/*
struct txn_node {
 public:
    txn_man * txn;
    struct txn_node * next;
};

typedef txn_node * txn_node_t;
*/

class Remote_query {
public:
	void init(uint64_t node_id, workload * wl);
	txn_man * get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id);
	txn_man * save_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, txn_man * txn_to_save);
	void remote_qry(base_query * query, int type, int dest_id, txn_man * txn);
  void ack_response(RC rc, txn_man * txn);
  void ack_response(base_query * query);
	void send_init(base_query * query, uint64_t dest_part_id);
	void send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num);
  void remote_rsp(base_query * query, txn_man * txn);
	void send_remote_rsp(uint64_t dest_id, void ** data, int * sizes, int num);
	void unpack(base_query * query, void * d, int len);
	int q_idx;
	/*
#if WORKLOAD == TPCC
	tpcc_query * queries;
#endif
*/
private:
	pthread_cond_t cnd;
	pthread_mutex_t mtx;
	
	uint64_t _node_id;
	workload * _wl;
  //txn_node_t **txns;

};
#endif
