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


// Each YCSBQuery contains several ycsb_requests, 
// each of which is a RD, WR or SCAN 
// to a single table

class ycsb_request {
public:
//	char table_name[80];
	access_t acctype; 
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

class YCSBQueryGenerator : public QueryGenerator {
public:
  BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id);

private:
	BaseQuery * gen_requests(uint64_t home_partition_id, Workload * h_wl);
	// for Zipfian distribution
	double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	myrand * mrand;
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;


};

class YCSBQuery : public BaseQuery {
public:

  void print();
  
  std::vector<ycsb_request *> requests;
	void init(uint64_t thd_id, Workload * h_wl);
  void init();
  uint64_t participants(bool *& pps,Workload * wl); 
  bool readonly();
	
  uint64_t access_cnt;
	uint64_t request_cnt;

  uint64_t rid;
	uint64_t req_i;
  ycsb_request req;
  uint64_t rqry_req_cnt;

};

#endif
