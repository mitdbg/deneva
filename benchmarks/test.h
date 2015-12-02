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

#ifndef _TEST_H_
#define _TEST_H_

#include "global.h"
#include "txn.h"
#include "wl.h"

class TestWorkload : public workload
{
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager);
	void summarize();
	void tick() { time = get_sys_clock(); };
	INDEX * the_index;
	table_t * the_table;
private:
	uint64_t time;
};

class TestTxnMan : public txn_man 
{
public:
	void init(workload * h_wl); 
	RC run_txn(int type, int access_num);
	RC run_txn(base_query * m_query) { assert(false); };
	RC run_rem_txn(base_query * m_query) {assert(false);};
	void rem_txn_rsp(base_query * m_query) {assert(false);};
private:
	RC testReadwrite(int access_num);
	RC testConflict(int access_num);
	
	TestWorkload * _wl;
};

#endif
