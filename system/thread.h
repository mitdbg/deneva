/*
   Copyright 2015 Rachael Harding

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _THREAD_H_
#define _THREAD_H_

#include "global.h"

class Workload;

class Thread {
public:
//	Thread();
//	~Thread();
	uint64_t _thd_id;
	uint64_t _node_id;
	Workload * _wl;

	uint64_t 	get_thd_id();
	uint64_t 	get_node_id();

	uint64_t 	get_host_cid();
	void 	 	set_host_cid(uint64_t cid);

	uint64_t 	get_cur_cid();
	void 		set_cur_cid(uint64_t cid);

	void 		init(uint64_t thd_id, uint64_t node_id, Workload * workload);
	// the following function must be in the form void* (*)(void*)
	// to run with pthread.
	// conversion is done within the function.
	RC 			run();
	RC 			run_send();
	RC 			run_abort();
	RC 			run_remote();
	RC 			run_calvin_lock();
	RC 			run_calvin_seq();
	RC 			run_calvin();

  RC process_rfin(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rack(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rqry_rsp(uint64_t tid, BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rqry(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rinit(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rprepare(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rpass(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rtxn(BaseQuery *& m_query,TxnManager *& m_txn);
  RC init_phase(BaseQuery * m_query, TxnManager * m_txn); 

private:
  uint64_t _thd_txn_id;
	uint64_t 	_host_cid;
	uint64_t 	_cur_cid;
	ts_t 		_curr_ts;
	ts_t 		get_next_ts();
  uint64_t run_starttime;
  uint64_t txn_starttime;

	RC	 		runTest(TxnManager * txn);
};

#endif
