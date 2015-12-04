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

#ifndef _YCSBQuery_H_
#define _YCSBQuery_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "remote_query.h"

class Workload;

enum YCSBRemTxnType {
  YCSB_0,
  YCSB_1,
  YCSB_FIN,
  YCSB_RDONE
};
// Each YCSBQuery contains several ycsb_requests, 
// each of which is a RD, WR or SCAN 
// to a single table

class ycsb_request {
public:
//	char table_name[80];
	access_t acctype; 
	uint64_t key;
	// for each field (string) in the row, shift the string to left by 1 character
	// and fill the right most character with value
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

class YCSBClientQuery : public BaseClientQuery {
  public:
	void client_init(uint64_t thd_id, Workload * h_wl);
  void client_init(uint64_t thd_id, Workload * h_wl, uint64_t node_id);
  void client_init();
  
  // calvin
  //void client_query(BaseClientQuery * query, uint64_t dest_id, uint64_t batch_num,
//		  txnid_t txn_id);

	uint64_t request_cnt;
	ycsb_request * requests;

private:
	void gen_requests(uint64_t thd_id, Workload * h_wl);
	void gen_requests2(uint64_t thd_id, Workload * h_wl);
	void gen_requests3(uint64_t thd_id, Workload * h_wl);
	// for Zipfian distribution
	double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	myrand * mrand;
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;

};

// Beware diamond inheritance...
class YCSBQuery : public BaseQuery {
public:
	void init(uint64_t thd_id, Workload * h_wl);
  void init();
  void reset();
  void unpack_rsp(BaseQuery * query, void * d); 
  void client_query(BaseQuery * query, uint64_t dest_id) {} 
  uint64_t participants(bool *& pps,Workload * wl); 
  void deep_copy(BaseQuery * qry); 
  bool readonly();
	
  BaseQuery * merge(BaseQuery * query); 
void unpack(BaseQuery * query, void * d) ;
void remote_qry(BaseQuery * query, int type, int dest_id) ;
void remote_rsp(BaseQuery * query) ;
//void unpack_client(BaseQuery * query, void * d) ;

  uint64_t access_cnt;
	uint64_t request_cnt;
	ycsb_request * requests;

	YCSBRemTxnType txn_rtype;
  uint64_t rid;
	uint64_t req_i;
  ycsb_request req;
  uint64_t rqry_req_cnt;

};

#endif
