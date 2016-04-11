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
	sim_init_done = false;
	sim_timeout = false;
  txn_cnt = 0;
  epoch_txn_cnt = 0;
  worker_epoch = 0;
  sched_epoch = 1;
  rsp_cnt = g_node_cnt + g_client_node_cnt + g_node_cnt*g_repl_cnt - 1;
  run_starttime = get_sys_clock();
}


void SimManager::set_starttime(uint64_t starttime) {
    ATOM_CAS(run_starttime, 0, starttime);
}

bool SimManager::timeout() {
  return (get_sys_clock() - run_starttime) >= g_done_timer;
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

void SimManager::inc_epoch_txn_cnt() {
  ATOM_ADD(epoch_txn_cnt,1);
}

void SimManager::decr_epoch_txn_cnt() {
  ATOM_SUB(epoch_txn_cnt,1);
}

uint64_t SimManager::get_sched_epoch() {
  return sched_epoch;
}

void SimManager::next_sched_epoch() {
  ATOM_ADD(sched_epoch,1);
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
