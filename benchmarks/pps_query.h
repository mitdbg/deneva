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

#ifndef _PPSQuery_H_
#define _PPSQuery_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class Workload;
class Message;
class PPSQueryMessage;
class PPSClientQueryMessage;

class PPSQueryGenerator : public QueryGenerator {
public:
  BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id);

private:
	BaseQuery * gen_requests(uint64_t home_partition_id, Workload * h_wl);
  BaseQuery * gen_parts(uint64_t home_partition); 
	myrand * mrand;
};

class PPSQuery : public BaseQuery {
public:
	void init(uint64_t thd_id, Workload * h_wl);
  void init();
  void reset();
  void release();
  void release_items();
  void print();
  static std::set<uint64_t> participants(Message * msg, Workload * wl); 
  uint64_t participants(bool *& pps,Workload * wl); 
  uint64_t get_participants(Workload * wl); 
  bool readonly();

	PPSTxnType txn_type;
	// txn input 

  uint64_t rqry_req_cnt;

};

#endif
