#ifndef _THREAD_H_
#define _THREAD_H_

#include "global.h"

class workload;

class thread_t {
public:
//	thread_t();
//	~thread_t();
	uint64_t _thd_id;
	uint64_t _node_id;
	workload * _wl;

	uint64_t 	get_thd_id();
	uint64_t 	get_node_id();

	uint64_t 	get_host_cid();
	void 	 	set_host_cid(uint64_t cid);

	uint64_t 	get_cur_cid();
	void 		set_cur_cid(uint64_t cid);

	void 		init(uint64_t thd_id, uint64_t node_id, workload * workload);
	// the following function must be in the form void* (*)(void*)
	// to run with pthread.
	// conversion is done within the function.
	RC 			run();
	RC 			run_send();
	RC 			run_remote();

  RC process_rfin(base_query *& m_query,txn_man *& m_txn);
  RC process_rack(base_query *& m_query,txn_man *& m_txn);
  RC process_rqry_rsp(base_query *& m_query,txn_man *& m_txn);
  RC process_rqry(base_query *& m_query,txn_man *& m_txn);
  RC process_rinit(base_query *& m_query,txn_man *& m_txn);
  RC process_rprepare(base_query *& m_query,txn_man *& m_txn);
  RC process_rpass(base_query *& m_query,txn_man *& m_txn);
  RC process_rtxn(base_query *& m_query,txn_man *& m_txn);
  RC init_phase(base_query * m_query, txn_man * m_txn); 

private:
  uint64_t _thd_txn_id;
	uint64_t 	_host_cid;
	uint64_t 	_cur_cid;
	ts_t 		_curr_ts;
	ts_t 		get_next_ts();
  uint64_t run_starttime;
  uint64_t txn_starttime;

	RC	 		runTest(txn_man * txn);
};

#endif
