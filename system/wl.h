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

#ifndef _WORKLOAD_H_
#define _WORKLOAD_H_

#include "global.h"
//#include "txn.h"
//#include "thread.h"
class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class lock_man;
class txn_man;
class thread_t;
class index_base;
class Timestamp;
class Mvcc;

// TODO write a new class Partition and put partition specific information 
// into that class.

// this is the base class for all workload
class workload
{
public:
//	table_t * table;
	// tables indexed by table name
	map<string, table_t *> tables;
	map<string, INDEX *> indexes;

	
	// FOR TPCC
/*	*/
	// initialize the tables and indexes.
	virtual RC init();
	virtual RC init_schema(const char * schema_file);
	virtual RC init_table()=0;
	virtual RC get_txn_man(txn_man *& txn_manager)=0;
	// get the global timestamp.
//	uint64_t get_ts(uint64_t thread_id);
	//uint64_t cur_txn_id;
	bool sim_init_done;
	bool sim_done;
	bool sim_timeout;
  uint64_t done_cnt;
  uint64_t txn_cnt;
  uint64_t rsp_cnt;
  uint64_t epoch;
  uint64_t curr_epoch;
  int64_t epoch_txn_cnt;
protected:
	void index_insert(string index_name, uint64_t key, row_t * row);
	void index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id = -1);
};

#endif
