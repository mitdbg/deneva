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

#include "global.h"
#include "helper.h"
#include "sim_manager.h"

void SimManager::init() {
	sim_done = false;
	start_set = false;
	sim_init_done = false;
	sim_timeout = false;
  txn_cnt = 0;
  inflight_cnt = 0;
  epoch_txn_cnt = 0;
  worker_epoch = 0;
  seq_epoch = 1;
  rsp_cnt = g_node_cnt + g_client_node_cnt + g_node_cnt*g_repl_cnt - 1;
#if TIME_ENABLE
  run_starttime = get_sys_clock();
#else
  run_starttime = get_wall_clock();
#endif
}


void SimManager::set_starttime(uint64_t starttime) {
    if(ATOM_CAS(start_set, false, true)) {
      run_starttime = starttime;
      DEBUG("Starttime set\n");
    }
}

bool SimManager::timeout() {
#if TIME_ENABLE
  return (get_sys_clock() - run_starttime) >= g_done_timer;
#else
  return (get_wall_clock() - run_starttime) >= g_done_timer;
#endif
}

bool SimManager::is_done() {
  bool done = sim_done || timeout();
  if(done && !sim_done) {
    set_done();
  }
  return done;
}

bool SimManager::is_setup_done() {
  return sim_init_done;
}

void SimManager::set_setup_done() {
    ATOM_CAS(sim_init_done, false, true);
}

void SimManager::set_done() {
    if(ATOM_CAS(sim_done, false, true)) {
				SET_STATS(0, total_runtime, get_sys_clock() - run_starttime); 
    }
}

void SimManager::process_setup_msg() {
  uint64_t rsp_left = ATOM_SUB_FETCH(rsp_cnt,1);
  if(rsp_left == 0) {
    set_setup_done();
  }
}

void SimManager::inc_txn_cnt() {
  ATOM_ADD(txn_cnt,1);
}

void SimManager::inc_inflight_cnt() {
  ATOM_ADD(inflight_cnt,1);
}

void SimManager::dec_inflight_cnt() {
  ATOM_SUB(inflight_cnt,1);
}

void SimManager::inc_epoch_txn_cnt() {
  ATOM_ADD(epoch_txn_cnt,1);
}

void SimManager::decr_epoch_txn_cnt() {
  ATOM_SUB(epoch_txn_cnt,1);
}

uint64_t SimManager::get_seq_epoch() {
  return seq_epoch;
}

void SimManager::advance_seq_epoch() {
  ATOM_ADD(seq_epoch,1);
}

uint64_t SimManager::get_worker_epoch() {
  return worker_epoch;
}

void SimManager::next_worker_epoch() {
  ATOM_ADD(worker_epoch,1);
}

double SimManager::seconds_from_start(uint64_t time) {
  return (double)(time - run_starttime) / BILLION;
}
