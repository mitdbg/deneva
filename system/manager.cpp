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

#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"
#include "remote_query.h"
//#include <jemallloc.h>

void Manager::init() {
	timestamp = 1;
	last_min_ts_time = 0;
	min_ts = 0; 
	all_ts = (ts_t *) malloc(sizeof(ts_t) * (g_thread_cnt * g_node_cnt));
	_all_txns = new txn_man * [g_thread_cnt + g_rem_thread_cnt];
	for (UInt32 i = 0; i < g_thread_cnt + g_rem_thread_cnt; i++) {
		//all_ts[i] = 0;
		//all_ts[i] = UINT64_MAX;
		_all_txns[i] = NULL;
	}
	for (UInt32 i = 0; i < BUCKET_CNT; i++)
		pthread_mutex_init( &mutexes[i], NULL );
  for (UInt32 i = 0; i < g_thread_cnt * g_node_cnt; ++i)
      all_ts[i] = 0;
}

uint64_t 
Manager::get_ts(uint64_t thread_id) {
	if (g_ts_batch_alloc)
		assert(g_ts_alloc == TS_CAS);
	uint64_t time;
	uint64_t starttime = get_sys_clock();
	switch(g_ts_alloc) {
	case TS_MUTEX :
		pthread_mutex_lock( &ts_mutex );
		time = ++timestamp;
		pthread_mutex_unlock( &ts_mutex );
		break;
	case TS_CAS :
		if (g_ts_batch_alloc)
			time = ATOM_FETCH_ADD(timestamp, g_ts_batch_num);
		else 
			time = ATOM_FETCH_ADD(timestamp, 1);
		break;
	case TS_HW :
		assert(false);
		break;
	case TS_CLOCK :
		time = get_sys_clock() * (g_node_cnt + g_thread_cnt) + (g_node_id * g_thread_cnt + thread_id);
		break;
	default :
		assert(false);
	}
	INC_STATS(thread_id, time_ts_alloc, get_sys_clock() - starttime);
	return time;
}

// FIXME: get min ts from remote_query txns, which is min of all queries in flight
ts_t Manager::get_min_ts(uint64_t tid) {
	uint64_t now = get_sys_clock();
	if (now - last_min_ts_time > MIN_TS_INTVL) { 
		last_min_ts_time = now;
    uint64_t min = txn_table.get_min_ts();
    /*
    assert(min != UINT64_MAX);
		assert(min >= min_ts);
    */
    if(min > min_ts)
		  min_ts = min;
	} 
//uint64_t tt4 = get_sys_clock() - now;
//INC_STATS(tid, debug4, tt4);
	return min_ts;
}

void Manager::add_ts(uint64_t node_id, uint64_t thd_id, ts_t ts) {
  //uint64_t id = g_thread_cnt + node_id;
  /*
  uint64_t id = g_thread_cnt * node_id + thd_id;
	assert( ts >= all_ts[id]); 
	all_ts[id] = ts;
  */
}

void Manager::add_ts(uint64_t thd_id, ts_t ts) {
//uint64_t t4 = get_sys_clock();
	//assert( ts >= all_ts[thd_id]); 
//		|| all_ts[thd_id] == UINT64_MAX);
	//all_ts[thd_id] = ts;
//uint64_t tt4 = get_sys_clock() - t4;
//INC_STATS(thd_id, debug4, tt4);
  //add_ts(g_node_id,thd_id,ts);
}

void Manager::set_txn_man(txn_man * txn) {
	int thd_id = txn->get_thd_id();
	_all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
	uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}
 
void Manager::lock_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_lock( &mutexes[bid] );	
}

void Manager::release_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_unlock( &mutexes[bid] );
}
