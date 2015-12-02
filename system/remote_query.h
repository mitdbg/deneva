#ifndef _REMOTE_QUERY_H_
#define _REMOTE_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "txn_pool.h"

#define MAX_DATA_SIZE 100 
class ycsb_query;
class ycsb_txn_man;
class tpcc_query;
class tpcc_txn_man;
class txn_man;
class base_query;


class Remote_query {
public:
	void init(uint64_t node_id, workload * wl);
	txn_man * get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id);
	txn_man * save_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, txn_man * txn_to_save);
	void unpack(void * d, uint64_t len);
  void unpack_query(base_query *& query,char * data,  uint64_t & ptr,uint64_t dest_id,uint64_t return_id); 
	//base_query * unpack(void * d, int len);
	base_client_query * unpack_client_query(void * d, int len);
  
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

};
#endif
