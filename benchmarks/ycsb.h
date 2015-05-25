#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class ycsb_query;
class ycsb_request;

class ycsb_wl : public workload {
public :
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;
private:
	void init_table_parallel();
	void * init_table_slice();
	static void * threadInitTable(void * This) {
		((ycsb_wl *)This)->init_table_slice(); 
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class ycsb_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
  bool conflict(base_query * query1,base_query * query2);
	RC run_txn(base_query * query);
	RC run_rem_txn(base_query * query);
	void rem_txn_rsp(base_query * query);
private:
void 		merge_txn_rsp(base_query * m_query1, base_query *m_query2) ;
void rtn_ycsb_state(base_query * query);
void next_ycsb_state(base_query * query);
RC run_txn_state(base_query * query);
RC run_ycsb_0(ycsb_request * req,row_t *& row_local);
RC run_ycsb_1(access_t acctype, row_t * row_local);
	uint64_t row_cnt;
  bool fin;
  bool rem_done;
  row_t * row;
	ycsb_wl * _wl;
};

#endif
