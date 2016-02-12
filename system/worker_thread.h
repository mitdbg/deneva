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

#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"

class Workload;

class WorkerThread : public Thread {
public:
	RC 			run();
  void setup();
  void send_init_done_to_all_nodes(); 
  RC process_rfin(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rack(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rqry_rsp(uint64_t tid, BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rqry(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rinit(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rprepare(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rpass(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_rtxn(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_log_msg(BaseQuery *& m_query,TxnManager *& m_txn);
  RC process_log_msg_rsp(BaseQuery *& m_query,TxnManager *& m_txn);
  RC init_phase(BaseQuery * m_query, TxnManager * m_txn); 

private:
  uint64_t _thd_txn_id;
	ts_t 		_curr_ts;
	ts_t 		get_next_ts();
  uint64_t txn_starttime;


};

#endif
