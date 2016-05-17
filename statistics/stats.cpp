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
#include "stats.h"
#include "mem_alloc.h"
#include "client_txn.h"
#include "work_queue.h"
#include "stats_array.h"
#include <time.h>
#include <sys/times.h>
#include <sys/vtimes.h>

void Stats_thd::init(uint64_t thd_id) {
  DEBUG_M("Stats_thd::init part_cnt alloc\n");
  part_cnt = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t)*g_part_cnt);
  DEBUG_M("Stats_thd::init part_acc alloc\n");
  part_acc = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t)*g_part_cnt);
  DEBUG_M("Stats_thd::init worker_process_cnt_by_type alloc\n");
  worker_process_cnt_by_type= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * NO_MSG);
  DEBUG_M("Stats_thd::init worker_process_time_by_type alloc\n");
  worker_process_time_by_type= (double *) mem_allocator.align_alloc(sizeof(double) * NO_MSG);
  DEBUG_M("Stats_thd::init mtx alloc\n");
  mtx= (double *) mem_allocator.align_alloc(sizeof(double) * 30);
	clear();
	//all_lat.init(g_max_txn_per_part,ArrIncr);

}

void Stats_thd::clear() {

  total_runtime=0;

  // Execution
  txn_cnt=0;
  remote_txn_cnt=0;
  local_txn_cnt=0;
  local_txn_start_cnt=0;
  total_txn_commit_cnt=0;
  local_txn_commit_cnt=0;
  remote_txn_commit_cnt=0;
  total_txn_abort_cnt=0;
  local_txn_abort_cnt=0;
  remote_txn_abort_cnt=0;
  txn_run_time=0;
  multi_part_txn_cnt=0;
  multi_part_txn_run_time=0;
  single_part_txn_cnt=0;
  single_part_txn_run_time=0;
  txn_write_cnt=0;
  record_write_cnt=0;

  // Transaction stats
  txn_total_process_time=0;
  txn_process_time=0;
  txn_total_local_wait_time=0;
  txn_local_wait_time=0;
  txn_total_remote_wait_time=0;
  txn_remote_wait_time=0;
  txn_total_twopc_time=0;
  txn_twopc_time=0;

  // Client
  txn_sent_cnt=0;
  cl_send_intv=0;

  // Abort queue
  abort_queue_enqueue_cnt=0;
  abort_queue_dequeue_cnt=0;
  abort_queue_enqueue_time=0;
  abort_queue_dequeue_time=0;
  abort_queue_penalty=0;
  abort_queue_penalty_extra=0;

  // Work queue
  work_queue_wait_time=0;
  work_queue_cnt=0;
  work_queue_enq_cnt=0;
  work_queue_mtx_wait_time=0;
  work_queue_new_cnt=0;
  work_queue_new_wait_time=0;
  work_queue_old_cnt=0;
  work_queue_old_wait_time=0;
  work_queue_enqueue_time=0;
  work_queue_dequeue_time=0;
  work_queue_conflict_cnt=0;

  // Worker thread
  worker_idle_time=0;
  worker_activate_txn_time=0;
  worker_deactivate_txn_time=0;
  worker_process_time=0;
  worker_process_cnt=0;
  for(uint64_t i = 0; i < NO_MSG; i ++) {
    worker_process_cnt_by_type[i]=0;
    worker_process_time_by_type[i]=0;
  }

  // IO
  msg_queue_delay_time=0;
  msg_queue_cnt=0;
  msg_queue_enq_cnt=0;
  msg_send_time=0;
  msg_recv_time=0;
  msg_recv_idle_time=0;
  msg_batch_cnt=0;
  msg_batch_size_msgs=0;
  msg_batch_size_bytes=0;
  msg_batch_size_bytes_to_server=0;
  msg_batch_size_bytes_to_client=0;
  msg_send_cnt=0;
  msg_recv_cnt=0;
  msg_unpack_time=0;
  mbuf_send_intv_time=0;
  msg_copy_output_time=0;

  // Concurrency control, general
  cc_conflict_cnt=0;
  txn_wait_cnt=0;
  txn_conflict_cnt=0;

  // 2PL
  twopl_already_owned_cnt=0;
  twopl_owned_cnt=0;
  twopl_sh_owned_cnt=0;
  twopl_ex_owned_cnt=0;
  twopl_owned_time=0;
  twopl_sh_owned_time=0;
  twopl_ex_owned_time=0;
  twopl_diff_time=0;

  //OCC
  occ_validate_time=0;
  occ_cs_wait_time=0;
  occ_cs_time=0;
  occ_hist_validate_time=0;
  occ_act_validate_time=0;
  occ_hist_validate_fail_time=0;
  occ_act_validate_fail_time=0;
  occ_check_cnt=0;
  occ_abort_check_cnt=0;
  occ_ts_abort_cnt=0;
  occ_finish_time=0;

  // MAAT
  maat_validate_cnt=0;
  maat_validate_time=0;
  maat_cs_wait_time=0;
  maat_case1_cnt=0;
  maat_case2_cnt=0;
  maat_case3_cnt=0;
  maat_case4_cnt=0;
  maat_case5_cnt=0;
  maat_range=0;
  maat_commit_cnt=0;


  // Logging
  log_write_cnt=0;
  log_write_time=0;
  log_flush_cnt=0;
  log_flush_time=0;
  log_process_time=0;

  // Transaction Table
  txn_table_new_cnt=0;
  txn_table_get_cnt=0;
  txn_table_release_cnt=0;
  txn_table_cflt_cnt=0;
  txn_table_cflt_size=0;
  txn_table_get_time=0;
  txn_table_release_time=0;
  txn_table_min_ts_time=0;

  for(uint64_t i = 0; i < 30; i ++) {
    mtx[i]=0;
  }
}

void Stats_thd::print_client(FILE * outf) {
  double txn_run_avg_time = 0;
  double tput = 0;
  if(txn_cnt > 0)
    txn_run_avg_time = txn_run_time / txn_cnt;
  if(total_runtime > 0) 
    tput = txn_cnt / (total_runtime / BILLION);
  fprintf(outf,
      "total_runtime=%f"
      ",tput=%f"
      ",txn_cnt=%ld"
      ",txn_sent_cnt=%ld"
      ",txn_run_time=%f"
      ",txn_run_avg_time=%f"
      ",cl_send_intv=%f"
      ,total_runtime/BILLION
      ,tput
      ,txn_cnt
      ,txn_sent_cnt
      ,txn_run_time / BILLION
      ,txn_run_avg_time / BILLION
      ,cl_send_intv / BILLION
  );
  // IO
  double mbuf_send_intv_time_avg = 0;
  double msg_unpack_time_avg = 0;
  double msg_send_time_avg = 0;
  double msg_recv_time_avg = 0;
  double msg_batch_size_msgs_avg = 0;
  double msg_batch_size_bytes_avg = 0;
  double msg_queue_delay_time_avg = 0;
  if(msg_queue_cnt > 0)
    msg_queue_delay_time_avg = msg_queue_delay_time / msg_queue_cnt;
  if(msg_batch_cnt > 0) {
    mbuf_send_intv_time_avg = mbuf_send_intv_time / msg_batch_cnt;
    msg_batch_size_msgs_avg = msg_batch_size_msgs / msg_batch_cnt;
    msg_batch_size_bytes_avg = msg_batch_size_bytes / msg_batch_cnt;
  }
  if(msg_recv_cnt > 0) {
    msg_recv_time_avg = msg_recv_time / msg_recv_cnt;
    msg_unpack_time_avg = msg_unpack_time / msg_recv_cnt;
  }
  if(msg_send_cnt > 0) {
    msg_send_time_avg = msg_send_time / msg_send_cnt;
  }
  fprintf(outf,
  ",msg_queue_delay_time=%f"
  ",msg_queue_cnt=%ld"
  ",msg_queue_enq_cnt=%ld"
  ",msg_queue_delay_time_avg=%f"
  ",msg_send_time=%f"
  ",msg_send_time_avg=%f"
  ",msg_recv_time=%f"
  ",msg_recv_time_avg=%f"
  ",msg_recv_idle_time=%f"
  ",msg_batch_cnt=%ld"
  ",msg_batch_size_msgs=%ld"
  ",msg_batch_size_msgs_avg=%f"
  ",msg_batch_size_bytes=%ld"
  ",msg_batch_size_bytes_avg=%f"
  ",msg_batch_size_bytes_to_server=%ld"
  ",msg_batch_size_bytes_to_client=%ld"
  ",msg_send_cnt=%ld"
  ",msg_recv_cnt=%ld"
  ",msg_unpack_time=%f"
  ",msg_unpack_time_avg=%f"
  ",mbuf_send_intv_time=%f"
  ",mbuf_send_intv_time_avg=%f"
  ",msg_copy_output_time=%f"
  ,msg_queue_delay_time / BILLION
  ,msg_queue_cnt
  ,msg_queue_enq_cnt
  ,msg_queue_delay_time_avg / BILLION
  ,msg_send_time / BILLION
  ,msg_send_time_avg / BILLION
  ,msg_recv_time / BILLION
  ,msg_recv_time_avg / BILLION
  ,msg_recv_idle_time / BILLION
  ,msg_batch_cnt
  ,msg_batch_size_msgs
  ,msg_batch_size_msgs_avg
  ,msg_batch_size_bytes
  ,msg_batch_size_bytes_avg
  ,msg_batch_size_bytes_to_server
  ,msg_batch_size_bytes_to_client
  ,msg_send_cnt
  ,msg_recv_cnt
  ,msg_unpack_time / BILLION
  ,msg_unpack_time_avg / BILLION
  ,mbuf_send_intv_time / BILLION
  ,mbuf_send_intv_time_avg / BILLION
  ,msg_copy_output_time / BILLION
  );


}

void Stats_thd::print(FILE * outf) {
  fprintf(outf,
      "total_runtime=%f"
      ,total_runtime/BILLION
  );
  // Execution
  double tput = 0;
  double txn_run_avg_time = 0;
  double multi_part_txn_avg_time = 0;
  double single_part_txn_avg_time = 0;
  if(total_runtime > 0) 
    tput = txn_cnt / (total_runtime / BILLION);
  if(txn_cnt > 0) 
    txn_run_avg_time = txn_run_time / txn_cnt;
  if(multi_part_txn_cnt > 0)
    multi_part_txn_avg_time = multi_part_txn_run_time / multi_part_txn_cnt;
  if(single_part_txn_cnt > 0)
    single_part_txn_avg_time = single_part_txn_run_time / single_part_txn_cnt;
  fprintf(outf,
  ",tput=%f"
  ",txn_cnt=%ld"
  ",remote_txn_cnt=%ld"
  ",local_txn_cnt=%ld"
  ",local_txn_start_cnt=%ld"
  ",total_txn_commit_cnt=%ld"
  ",local_txn_commit_cnt=%ld"
  ",remote_txn_commit_cnt=%ld"
  ",total_txn_abort_cnt=%ld"
  ",local_txn_abort_cnt=%ld"
  ",remote_txn_abort_cnt=%ld"
  ",txn_run_time=%f"
  ",txn_run_avg_time=%f"
  ",multi_part_txn_cnt=%ld"
  ",multi_part_txn_run_time=%f"
  ",multi_part_txn_avg_time=%f"
  ",single_part_txn_cnt=%ld"
  ",single_part_txn_run_time=%f"
  ",single_part_txn_avg_time=%f"
  ",txn_write_cnt=%ld"
  ",record_write_cnt=%ld"
  ,tput
  ,txn_cnt
  ,remote_txn_cnt
  ,local_txn_cnt
  ,local_txn_start_cnt
  ,total_txn_commit_cnt
  ,local_txn_commit_cnt
  ,remote_txn_commit_cnt
  ,total_txn_abort_cnt
  ,local_txn_abort_cnt
  ,remote_txn_abort_cnt
  ,txn_run_time / BILLION
  ,txn_run_avg_time / BILLION
  ,multi_part_txn_cnt
  ,multi_part_txn_run_time / BILLION
  ,multi_part_txn_avg_time / BILLION
  ,single_part_txn_cnt
  ,single_part_txn_run_time / BILLION
  ,single_part_txn_avg_time / BILLION
  ,txn_write_cnt
  ,record_write_cnt
  );

  // Transaction stats
  double txn_total_process_time_avg=0;
  double txn_process_time_avg=0;
  double txn_total_local_wait_time_avg=0;
  double txn_local_wait_time_avg=0;
  double txn_total_remote_wait_time_avg=0;
  double txn_remote_wait_time_avg=0;
  double txn_total_twopc_time_avg=0;
  double txn_twopc_time_avg=0;
  if(local_txn_commit_cnt > 0) {
    txn_total_process_time_avg = txn_total_process_time / local_txn_commit_cnt;
    txn_process_time_avg = txn_process_time / local_txn_commit_cnt;
    txn_total_local_wait_time_avg = txn_total_local_wait_time / local_txn_commit_cnt;
    txn_local_wait_time_avg = txn_local_wait_time / local_txn_commit_cnt;
    txn_total_remote_wait_time_avg = txn_total_remote_wait_time / local_txn_commit_cnt;
    txn_remote_wait_time_avg = txn_remote_wait_time / local_txn_commit_cnt;
    txn_total_twopc_time_avg = txn_total_twopc_time / local_txn_commit_cnt;
    txn_twopc_time_avg = txn_twopc_time / local_txn_commit_cnt;
  }
  fprintf(outf,
  ",txn_total_process_time=%f"
  ",txn_process_time=%f"
  ",txn_total_local_wait_time=%f"
  ",txn_local_wait_time=%f"
  ",txn_total_remote_wait_time=%f"
  ",txn_remote_wait_time=%f"
  ",txn_total_twopc_time=%f"
  ",txn_twopc_time=%f"
  ",txn_total_process_time_avg=%f"
  ",txn_process_time_avg=%f"
  ",txn_total_local_wait_time_avg=%f"
  ",txn_local_wait_time_avg=%f"
  ",txn_total_remote_wait_time_avg=%f"
  ",txn_remote_wait_time_avg=%f"
  ",txn_total_twopc_time_avg=%f"
  ",txn_twopc_time_avg=%f"
  ,txn_total_process_time / BILLION
  ,txn_process_time / BILLION
  ,txn_total_local_wait_time / BILLION
  ,txn_local_wait_time / BILLION
  ,txn_total_remote_wait_time / BILLION
  ,txn_remote_wait_time / BILLION
  ,txn_total_twopc_time / BILLION
  ,txn_twopc_time / BILLION
  ,txn_total_process_time_avg / BILLION
  ,txn_process_time_avg / BILLION
  ,txn_total_local_wait_time_avg / BILLION
  ,txn_local_wait_time_avg / BILLION
  ,txn_total_remote_wait_time_avg / BILLION
  ,txn_remote_wait_time_avg / BILLION
  ,txn_total_twopc_time_avg / BILLION
  ,txn_twopc_time_avg / BILLION
  );

  // Abort queue
  double abort_queue_penalty_avg = 0;
  double abort_queue_penalty_extra_avg = 0;
  if(abort_queue_enqueue_cnt > 0)
    abort_queue_penalty_extra_avg = abort_queue_penalty_extra / abort_queue_enqueue_cnt;
  if(abort_queue_dequeue_cnt > 0)
    abort_queue_penalty_avg = abort_queue_penalty / abort_queue_enqueue_cnt;
  fprintf(outf,
  ",abort_queue_enqueue_cnt=%ld"
  ",abort_queue_dequeue_cnt=%ld"
  ",abort_queue_enqueue_time=%f"
  ",abort_queue_dequeue_time=%f"
  ",abort_queue_penalty=%f"
  ",abort_queue_penalty_extra=%f"
  ",abort_queue_penalty_avg=%f"
  ",abort_queue_penalty_extra_avg=%f"
  // Abort queue
  ,abort_queue_enqueue_cnt
  ,abort_queue_dequeue_cnt
  ,abort_queue_enqueue_time / BILLION
  ,abort_queue_dequeue_time / BILLION
  ,abort_queue_penalty / BILLION
  ,abort_queue_penalty_extra / BILLION
  ,abort_queue_penalty_avg / BILLION
  ,abort_queue_penalty_extra_avg / BILLION
  );


  double work_queue_wait_avg_time = 0;
  double work_queue_mtx_wait_avg = 0;
  double work_queue_new_wait_avg_time = 0;
  double work_queue_old_wait_avg_time = 0;
  if(work_queue_cnt > 0) {
    work_queue_wait_avg_time = work_queue_wait_time / work_queue_cnt;
    work_queue_mtx_wait_avg = work_queue_mtx_wait_time / work_queue_cnt;
  }
  if(work_queue_new_cnt > 0)
    work_queue_new_wait_avg_time = work_queue_new_wait_time / work_queue_new_cnt;
  if(work_queue_old_cnt > 0)
    work_queue_old_wait_avg_time = work_queue_old_wait_time / work_queue_old_cnt;
  // Work queue
  fprintf(outf,
  ",work_queue_wait_time=%f"
  ",work_queue_cnt=%ld"
  ",work_queue_enq_cnt=%ld"
  ",work_queue_wait_avg_time=%f"
  ",work_queue_mtx_wait_time=%f"
  ",work_queue_mtx_wait_avg=%f"
  ",work_queue_new_cnt=%ld"
  ",work_queue_new_wait_time=%f"
  ",work_queue_new_wait_avg_time=%f"
  ",work_queue_old_cnt=%ld"
  ",work_queue_old_wait_time=%f"
  ",work_queue_old_wait_avg_time=%f"
  ",work_queue_enqueue_time=%f"
  ",work_queue_dequeue_time=%f"
  ",work_queue_conflict_cnt=%ld"
  ,work_queue_wait_time / BILLION
  ,work_queue_cnt
  ,work_queue_enq_cnt
  ,work_queue_wait_avg_time / BILLION
  ,work_queue_mtx_wait_time / BILLION
  ,work_queue_mtx_wait_avg / BILLION
  ,work_queue_new_cnt
  ,work_queue_new_wait_time / BILLION
  ,work_queue_new_wait_avg_time / BILLION
  ,work_queue_old_cnt
  ,work_queue_old_wait_time / BILLION
  ,work_queue_old_wait_avg_time / BILLION
  ,work_queue_enqueue_time / BILLION
  ,work_queue_dequeue_time / BILLION
  ,work_queue_conflict_cnt
  );


  // Worker thread
  double worker_process_avg_time = 0;
  if(worker_process_cnt > 0)
    worker_process_avg_time = worker_process_time / worker_process_cnt;
  fprintf(outf,
    ",worker_idle_time=%f"
    ",worker_activate_txn_time=%f"
    ",worker_deactivate_txn_time=%f"
    ",worker_process_time=%f"
    ",worker_process_cnt=%ld"
    ",worker_process_avg_time=%f"
    ,worker_idle_time / BILLION
    ,worker_activate_txn_time / BILLION
    ,worker_deactivate_txn_time / BILLION
    ,worker_process_time / BILLION
    ,worker_process_cnt
    ,worker_process_avg_time / BILLION
  );
  for(uint64_t i = 0; i < NO_MSG; i ++) {
    fprintf(outf,
      ",proc_cnt_type%ld=%ld"
      ",proc_time_type%ld=%f"
      ,i
      ,worker_process_cnt_by_type[i]
      ,i
      ,worker_process_time_by_type[i] / BILLION
    );
  }

  // IO
  double mbuf_send_intv_time_avg = 0;
  double msg_unpack_time_avg = 0;
  double msg_send_time_avg = 0;
  double msg_recv_time_avg = 0;
  double msg_batch_size_msgs_avg = 0;
  double msg_batch_size_bytes_avg = 0;
  double msg_queue_delay_time_avg = 0;
  if(msg_queue_cnt > 0)
    msg_queue_delay_time_avg = msg_queue_delay_time / msg_queue_cnt;
  if(msg_batch_cnt > 0) {
    mbuf_send_intv_time_avg = mbuf_send_intv_time / msg_batch_cnt;
    msg_batch_size_msgs_avg = msg_batch_size_msgs / msg_batch_cnt;
    msg_batch_size_bytes_avg = msg_batch_size_bytes / msg_batch_cnt;
  }
  if(msg_recv_cnt > 0) {
    msg_recv_time_avg = msg_recv_time / msg_recv_cnt;
    msg_unpack_time_avg = msg_unpack_time / msg_recv_cnt;
  }
  if(msg_send_cnt > 0) {
    msg_send_time_avg = msg_send_time / msg_send_cnt;
  }
  fprintf(outf,
  ",msg_queue_delay_time=%f"
  ",msg_queue_cnt=%ld"
  ",msg_queue_enq_cnt=%ld"
  ",msg_queue_delay_time_avg=%f"
  ",msg_send_time=%f"
  ",msg_send_time_avg=%f"
  ",msg_recv_time=%f"
  ",msg_recv_time_avg=%f"
  ",msg_recv_idle_time=%f"
  ",msg_batch_cnt=%ld"
  ",msg_batch_size_msgs=%ld"
  ",msg_batch_size_msgs_avg=%f"
  ",msg_batch_size_bytes=%ld"
  ",msg_batch_size_bytes_avg=%f"
  ",msg_batch_size_bytes_to_server=%ld"
  ",msg_batch_size_bytes_to_client=%ld"
  ",msg_send_cnt=%ld"
  ",msg_recv_cnt=%ld"
  ",msg_unpack_time=%f"
  ",msg_unpack_time_avg=%f"
  ",mbuf_send_intv_time=%f"
  ",mbuf_send_intv_time_avg=%f"
  ",msg_copy_output_time=%f"
  ,msg_queue_delay_time / BILLION
  ,msg_queue_cnt
  ,msg_queue_enq_cnt
  ,msg_queue_delay_time_avg / BILLION
  ,msg_send_time / BILLION
  ,msg_send_time_avg / BILLION
  ,msg_recv_time / BILLION
  ,msg_recv_time_avg / BILLION
  ,msg_recv_idle_time / BILLION
  ,msg_batch_cnt
  ,msg_batch_size_msgs
  ,msg_batch_size_msgs_avg
  ,msg_batch_size_bytes
  ,msg_batch_size_bytes_avg
  ,msg_batch_size_bytes_to_server
  ,msg_batch_size_bytes_to_client
  ,msg_send_cnt
  ,msg_recv_cnt
  ,msg_unpack_time / BILLION
  ,msg_unpack_time_avg / BILLION
  ,mbuf_send_intv_time / BILLION
  ,mbuf_send_intv_time_avg / BILLION
  ,msg_copy_output_time / BILLION
  );

  // Concurrency control, general
  fprintf(outf,
    ",cc_conflict_cnt=%ld"
    ",txn_wait_cnt=%ld"
    ",txn_conflict_cnt=%ld"
    ,cc_conflict_cnt
    ,txn_wait_cnt
    ,txn_conflict_cnt
  );

  // 2PL
  double twopl_sh_owned_avg_time = 0;
  if(twopl_sh_owned_cnt > 0)
    twopl_sh_owned_avg_time = twopl_sh_owned_time / twopl_sh_owned_cnt;
  double twopl_ex_owned_avg_time = 0;
  if(twopl_ex_owned_cnt > 0)
    twopl_ex_owned_avg_time = twopl_ex_owned_time / twopl_ex_owned_cnt;
  fprintf(outf,
    ",twopl_already_owned_cnt=%ld"
    ",twopl_owned_cnt=%ld"
    ",twopl_sh_owned_cnt=%ld"
    ",twopl_ex_owned_cnt=%ld"
    ",twopl_owned_time=%f"
    ",twopl_sh_owned_time=%f"
    ",twopl_ex_owned_time=%f"
    ",twopl_sh_owned_avg_time=%f"
    ",twopl_ex_owned_avg_time=%f"
    ",twopl_diff_time=%f"
    ,twopl_already_owned_cnt
    ,twopl_owned_cnt
    ,twopl_sh_owned_cnt
    ,twopl_ex_owned_cnt
    ,twopl_owned_time / BILLION
    ,twopl_sh_owned_time / BILLION
    ,twopl_ex_owned_time / BILLION
    ,twopl_sh_owned_avg_time / BILLION
    ,twopl_ex_owned_avg_time / BILLION
    ,twopl_diff_time / BILLION
  );

  //OCC
  fprintf(outf,
  ",occ_validate_time=%f"
  ",occ_cs_wait_time=%f"
  ",occ_cs_time=%f"
  ",occ_hist_validate_time=%f"
  ",occ_act_validate_time=%f"
  ",occ_hist_validate_fail_time=%f"
  ",occ_act_validate_fail_time=%f"
  ",occ_check_cnt=%ld"
  ",occ_abort_check_cnt=%ld"
  ",occ_ts_abort_cnt=%ld"
  ",occ_finish_time=%f"
  ,occ_validate_time / BILLION
  ,occ_cs_wait_time / BILLION
  ,occ_cs_time / BILLION
  ,occ_hist_validate_time / BILLION
  ,occ_act_validate_time / BILLION
  ,occ_hist_validate_fail_time / BILLION
  ,occ_act_validate_fail_time / BILLION
  ,occ_check_cnt
  ,occ_abort_check_cnt
  ,occ_ts_abort_cnt
  ,occ_finish_time / BILLION
  );

  //MAAT
  double maat_range_avg = 0;
  double maat_validate_avg = 0;
  double maat_cs_wait_avg = 0;
  uint64_t maat_commit_avg = 0;
  if(maat_commit_cnt > 0)
    maat_range_avg = maat_range / maat_commit_cnt;
  if(maat_validate_cnt > 0) {
    maat_validate_avg = maat_validate_time / maat_validate_cnt;
    maat_cs_wait_avg = maat_cs_wait_time / maat_validate_cnt;
    maat_commit_avg = maat_commit_cnt / maat_validate_cnt;
  }
  fprintf(outf,
  ",maat_validate_cnt=%ld"
  ",maat_validate_time=%f"
  ",maat_validate_avg=%f"
  ",maat_cs_wait_time=%f"
  ",maat_cs_wait_avg=%f"
  ",maat_case1_cnt=%ld"
  ",maat_case2_cnt=%ld"
  ",maat_case3_cnt=%ld"
  ",maat_case4_cnt=%ld"
  ",maat_case5_cnt=%ld"
  ",maat_range=%f"
  ",maat_commit_cnt=%ld"
  ",maat_commit_avg=%ld"
  ",maat_range_avg=%f"
  ,maat_validate_cnt
  ,maat_validate_time / BILLION
  ,maat_validate_avg / BILLION
  ,maat_cs_wait_time / BILLION
  ,maat_cs_wait_avg / BILLION
  ,maat_case1_cnt
  ,maat_case2_cnt
  ,maat_case3_cnt
  ,maat_case4_cnt
  ,maat_case5_cnt
  ,maat_range / BILLION
  ,maat_commit_cnt
  ,maat_commit_avg
  ,maat_range_avg
  );


  // Logging
  double log_write_avg_time = 0;
  if(log_write_cnt > 0)
    log_write_avg_time = log_write_time / log_write_cnt;
  double log_flush_avg_time = 0;
  if(log_flush_cnt > 0)
    log_flush_avg_time = log_flush_time / log_flush_cnt;
  fprintf(outf,
    ",log_write_cnt=%ld"
    ",log_write_time=%f"
    ",log_write_avg_time=%f"
    ",log_flush_cnt=%ld"
    ",log_flush_time=%f"
    ",log_flush_avg_time=%f"
    ",log_process_time=%f"
    ,log_write_cnt
    ,log_write_time / BILLION
    ,log_write_avg_time / BILLION
    ,log_flush_cnt
    ,log_flush_time / BILLION
    ,log_flush_avg_time / BILLION
    ,log_process_time / BILLION
  );

  // Transaction Table
  double txn_table_get_avg_time = 0;
  if(txn_table_get_cnt > 0)
    txn_table_get_avg_time = txn_table_get_time / txn_table_get_cnt;
  double txn_table_release_avg_time = 0;
  if(txn_table_release_cnt > 0)
    txn_table_release_avg_time = txn_table_release_time / txn_table_release_cnt;
  fprintf(outf,
    ",txn_table_new_cnt=%ld"
    ",txn_table_get_cnt=%ld"
    ",txn_table_release_cnt=%ld"
    ",txn_table_cflt_cnt=%ld"
    ",txn_table_cflt_size=%ld"
    ",txn_table_get_time=%f"
    ",txn_table_release_time=%f"
    ",txn_table_min_ts_time=%f"
    ",txn_table_get_avg_time=%f"
    ",txn_table_release_avg_time=%f"
    // Transaction Table
    ,txn_table_new_cnt
    ,txn_table_get_cnt
    ,txn_table_release_cnt
    ,txn_table_cflt_cnt
    ,txn_table_cflt_size
    ,txn_table_get_time / BILLION
    ,txn_table_release_time / BILLION
    ,txn_table_min_ts_time / BILLION
    ,txn_table_get_avg_time / BILLION
    ,txn_table_release_avg_time / BILLION
  );

  for(uint64_t i = 0; i < 30; i ++) {
    fprintf(outf,
      ",mtx%ld=%f"
      ,i
      ,mtx[i] / BILLION
      );
  }
}

void Stats_thd::combine(Stats_thd * stats) {
  if(stats->total_runtime > total_runtime)
    total_runtime = stats->total_runtime;
  // Execution
  txn_cnt+=stats->txn_cnt;
  remote_txn_cnt+=stats->remote_txn_cnt;
  local_txn_cnt+=stats->local_txn_cnt;
  local_txn_start_cnt+=stats->local_txn_start_cnt;
  total_txn_commit_cnt+=stats->total_txn_commit_cnt;
  local_txn_commit_cnt+=stats->local_txn_commit_cnt;
  remote_txn_commit_cnt+=stats->remote_txn_commit_cnt;
  total_txn_abort_cnt+=stats->total_txn_abort_cnt;
  local_txn_abort_cnt+=stats->local_txn_abort_cnt;
  remote_txn_abort_cnt+=stats->remote_txn_abort_cnt;
  txn_run_time+=stats->txn_run_time;
  multi_part_txn_cnt+=stats->multi_part_txn_cnt;
  multi_part_txn_run_time+=stats->multi_part_txn_run_time;
  single_part_txn_cnt+=stats->single_part_txn_cnt;
  single_part_txn_run_time+=stats->single_part_txn_run_time;
  txn_write_cnt+=stats->txn_write_cnt;
  record_write_cnt+=stats->record_write_cnt;

  // Transaction stats
  txn_total_process_time+=stats->txn_total_process_time;
  txn_process_time+=stats->txn_process_time;
  txn_total_local_wait_time+=stats->txn_total_local_wait_time;
  txn_local_wait_time+=stats->txn_local_wait_time;
  txn_total_remote_wait_time+=stats->txn_total_remote_wait_time;
  txn_remote_wait_time+=stats->txn_remote_wait_time;
  txn_total_twopc_time+=stats->txn_total_twopc_time;
  txn_twopc_time+=stats->txn_twopc_time;

  // Client
  txn_sent_cnt+=stats->txn_sent_cnt;
  cl_send_intv+=stats->cl_send_intv;

  // Abort queue
  abort_queue_enqueue_cnt+=stats->abort_queue_enqueue_cnt;
  abort_queue_dequeue_cnt+=stats->abort_queue_dequeue_cnt;
  abort_queue_enqueue_time+=stats->abort_queue_enqueue_time;
  abort_queue_dequeue_time+=stats->abort_queue_dequeue_time;
  abort_queue_penalty+=stats->abort_queue_penalty;
  abort_queue_penalty_extra+=stats->abort_queue_penalty_extra;

  // Work queue
  work_queue_wait_time+=stats->work_queue_wait_time;
  work_queue_cnt+=stats->work_queue_cnt;
  work_queue_enq_cnt+=stats->work_queue_enq_cnt;
  work_queue_mtx_wait_time+=stats->work_queue_mtx_wait_time;
  work_queue_new_cnt+=stats->work_queue_new_cnt;
  work_queue_new_wait_time+=stats->work_queue_new_wait_time;
  work_queue_old_cnt+=stats->work_queue_old_cnt;
  work_queue_old_wait_time+=stats->work_queue_old_wait_time;
  work_queue_enqueue_time+=stats->work_queue_enqueue_time;
  work_queue_dequeue_time+=stats->work_queue_dequeue_time;
  work_queue_conflict_cnt+=stats->work_queue_conflict_cnt;

  // Worker thread
  worker_idle_time+=stats->worker_idle_time;
  worker_activate_txn_time+=stats->worker_activate_txn_time;
  worker_deactivate_txn_time+=stats->worker_deactivate_txn_time;
  worker_process_time+=stats->worker_process_time;
  worker_process_cnt+=stats->worker_process_cnt;
  for(uint64_t i = 0; i < NO_MSG; i ++) {
    worker_process_cnt_by_type[i]+=stats->worker_process_cnt_by_type[i];
    worker_process_time_by_type[i]+=stats->worker_process_time_by_type[i];
  }

  // IO
  msg_queue_delay_time+=stats->msg_queue_delay_time;
  msg_queue_cnt+=stats->msg_queue_cnt;
  msg_queue_enq_cnt+=stats->msg_queue_enq_cnt;
  msg_send_time+=stats->msg_send_time;
  msg_recv_time+=stats->msg_recv_time;
  msg_recv_idle_time+=stats->msg_recv_idle_time;
  msg_batch_cnt+=stats->msg_batch_cnt;
  msg_batch_size_msgs+=stats->msg_batch_size_msgs;
  msg_batch_size_bytes+=stats->msg_batch_size_bytes;
  msg_batch_size_bytes_to_server+=stats->msg_batch_size_bytes_to_server;
  msg_batch_size_bytes_to_client+=stats->msg_batch_size_bytes_to_client;
  msg_send_cnt+=stats->msg_send_cnt;
  msg_recv_cnt+=stats->msg_recv_cnt;
  msg_unpack_time+=stats->msg_unpack_time;
  mbuf_send_intv_time+=stats->mbuf_send_intv_time;
  msg_copy_output_time+=stats->msg_copy_output_time;

  // Concurrency control, general
  cc_conflict_cnt+=stats->cc_conflict_cnt;
  txn_wait_cnt+=stats->txn_wait_cnt;
  txn_conflict_cnt+=stats->txn_conflict_cnt;

  // 2PL
  twopl_already_owned_cnt+=stats->twopl_already_owned_cnt;
  twopl_owned_cnt+=stats->twopl_owned_cnt;
  twopl_sh_owned_cnt+=stats->twopl_sh_owned_cnt;
  twopl_ex_owned_cnt+=stats->twopl_ex_owned_cnt;
  twopl_owned_time+=stats->twopl_owned_time;
  twopl_sh_owned_time+=stats->twopl_sh_owned_time;
  twopl_ex_owned_time+=stats->twopl_ex_owned_time;
  twopl_diff_time+=stats->twopl_diff_time;

  //OCC
  occ_validate_time+=stats->occ_validate_time;
  occ_cs_wait_time+=stats->occ_cs_wait_time;
  occ_cs_time+=stats->occ_cs_time;
  occ_hist_validate_time+=stats->occ_hist_validate_time;
  occ_act_validate_time+=stats->occ_act_validate_time;
  occ_hist_validate_fail_time+=stats->occ_hist_validate_fail_time;
  occ_act_validate_fail_time+=stats->occ_act_validate_fail_time;
  occ_check_cnt+=stats->occ_check_cnt;
  occ_abort_check_cnt+=stats->occ_abort_check_cnt;
  occ_ts_abort_cnt+=stats->occ_ts_abort_cnt;
  occ_finish_time+=stats->occ_finish_time;

  // MAAT
  maat_validate_cnt+=stats->maat_validate_cnt;
  maat_validate_time+=stats->maat_validate_time;
  maat_cs_wait_time+=stats->maat_cs_wait_time;
  maat_case1_cnt+=stats->maat_case1_cnt;
  maat_case2_cnt+=stats->maat_case2_cnt;
  maat_case3_cnt+=stats->maat_case3_cnt;
  maat_case4_cnt+=stats->maat_case4_cnt;
  maat_case5_cnt+=stats->maat_case5_cnt;
  maat_range+=stats->maat_range;
  maat_commit_cnt+=stats->maat_commit_cnt;

  // Logging
  log_write_cnt+=stats->log_write_cnt;
  log_write_time+=stats->log_write_time;
  log_flush_cnt+=stats->log_flush_cnt;
  log_flush_time+=stats->log_flush_time;
  log_process_time+=stats->log_process_time;

  // Transaction Table
  txn_table_new_cnt+=stats->txn_table_new_cnt;
  txn_table_get_cnt+=stats->txn_table_get_cnt;
  txn_table_release_cnt+=stats->txn_table_release_cnt;
  txn_table_cflt_cnt+=stats->txn_table_cflt_cnt;
  txn_table_cflt_size+=stats->txn_table_cflt_size;
  txn_table_get_time+=stats->txn_table_get_time;
  txn_table_release_time+=stats->txn_table_release_time;
  txn_table_min_ts_time+=stats->txn_table_min_ts_time;

  for(uint64_t i = 0; i < 30; i ++) {
    mtx[i]+=stats->mtx[i];
  }
}


void Stats::init(uint64_t thread_cnt) {
	if (!STATS_ENABLE) 
		return;

  thd_cnt = thread_cnt;
	_stats = new Stats_thd * [thread_cnt];
	totals = new Stats_thd;

  for(uint64_t i = 0; i < thread_cnt; i++) {
    _stats[i] = (Stats_thd *) 
      mem_allocator.align_alloc(sizeof(Stats_thd));
    _stats[i]->init(i);
    _stats[i]->clear();
  }

  totals->init(0);
  totals->clear();

}

void Stats::clear(uint64_t tid) {
}

void Stats::print_client(bool prog) {
  fflush(stdout);
  if(!STATS_ENABLE)
    return;

  totals->clear();
  for(uint64_t i = 0; i < thd_cnt; i++)
    totals->combine(_stats[i]);


	FILE * outf;
	if (output_file != NULL) 
		outf = fopen(output_file, "w");
  else 
    outf = stdout;
  if(prog)
	  fprintf(outf, "[prog] ");
  else
	  fprintf(outf, "[summary] ");
  totals->print_client(outf);
  mem_util(outf);
  cpu_util(outf);

    if(prog) {
      fprintf(outf,"\n");
		  //for (uint32_t k = 0; k < g_node_id; ++k) {
		  for (uint32_t k = 0; k < g_servers_per_client; ++k) {
        printf("tif_node%u=%d, "
            ,k,client_man.get_inflight(k)
            );
      }
      printf("\n");
    } else {

      /*
      uint64_t tid = 0;
      uint64_t max_idx = 0;
      if(_stats[tid]->all_lat.cnt > 0)
        max_idx = _stats[tid]->all_lat.cnt -1;
      _stats[tid]->all_lat.quicksort(0,_stats[tid]->all_lat.cnt-1);
	    fprintf(outf, 
          ",lat_min=%ld"
          ",lat_max=%ld"
          ",lat_mean=%ld"
          ",lat_99ile=%ld"
          ",lat_98ile=%ld"
          ",lat_95ile=%ld"
          ",lat_90ile=%ld"
          ",lat_80ile=%ld"
          ",lat_75ile=%ld"
          ",lat_70ile=%ld"
          ",lat_60ile=%ld"
          ",lat_50ile=%ld"
          ",lat_40ile=%ld"
          ",lat_30ile=%ld"
          ",lat_25ile=%ld"
          ",lat_20ile=%ld"
          ",lat_10ile=%ld"
          ",lat_5ile=%ld\n"
          ,_stats[tid]->all_lat.get_idx(0)
          ,_stats[tid]->all_lat.get_idx(max_idx)
          ,_stats[tid]->all_lat.get_avg()
          ,_stats[tid]->all_lat.get_percentile(99)
          ,_stats[tid]->all_lat.get_percentile(98)
          ,_stats[tid]->all_lat.get_percentile(95)
          ,_stats[tid]->all_lat.get_percentile(90)
          ,_stats[tid]->all_lat.get_percentile(80)
          ,_stats[tid]->all_lat.get_percentile(75)
          ,_stats[tid]->all_lat.get_percentile(70)
          ,_stats[tid]->all_lat.get_percentile(60)
          ,_stats[tid]->all_lat.get_percentile(50)
          ,_stats[tid]->all_lat.get_percentile(40)
          ,_stats[tid]->all_lat.get_percentile(30)
          ,_stats[tid]->all_lat.get_percentile(25)
          ,_stats[tid]->all_lat.get_percentile(20)
          ,_stats[tid]->all_lat.get_percentile(10)
          ,_stats[tid]->all_lat.get_percentile(5)
          );
	    print_lat_distr(99,100);
      */
    }

	if (output_file != NULL) {
    fflush(outf);
		fclose(outf);
  }
  fflush(stdout);
}

void Stats::print(bool prog) {

  fflush(stdout);
  if(!STATS_ENABLE)
    return;
	
  totals->clear();
  for(uint64_t i = 0; i < thd_cnt; i++) 
    totals->combine(_stats[i]);
	FILE * outf;
	if (output_file != NULL) 
		outf = fopen(output_file, "w");
  else
    outf = stdout;
  if(prog)
	  fprintf(outf, "[prog] ");
  else
	  fprintf(outf, "[summary] ");
  totals->print(outf);
  mem_util(outf);
  cpu_util(outf);

  fprintf(outf,"\n");
  fflush(outf);
  if(!prog) {
    //print_cnts(outf);
	  //print_lat_distr();
  }
  fprintf(outf,"\n");
  fflush(outf);
	if (output_file != NULL) {
		fclose(outf);
  }

}

uint64_t Stats::get_txn_cnts() {
    if(!STATS_ENABLE || g_node_id >= g_node_cnt)
        return 0;
    uint64_t limit =  g_thread_cnt + g_rem_thread_cnt;
    uint64_t total_txn_cnt = 0;
	for (uint64_t tid = 0; tid < limit; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
    }
    //printf("total_txn_cnt: %lu\n",total_txn_cnt);
    return total_txn_cnt;
}

  /*
void Stats::print_cnts(FILE * outf) {
  if(!STATS_ENABLE || g_node_id >= g_node_cnt)
    return;
  uint64_t all_abort_cnt = 0;
  uint64_t w_cflt_cnt = 0;
  uint64_t d_cflt_cnt = 0;
  uint64_t cnp_cflt_cnt = 0;
  uint64_t c_cflt_cnt = 0;
  uint64_t ol_cflt_cnt = 0;
  uint64_t s_cflt_cnt = 0;
  uint64_t w_abrt_cnt = 0;
  uint64_t d_abrt_cnt = 0;
  uint64_t cnp_abrt_cnt = 0;
  uint64_t c_abrt_cnt = 0;
  uint64_t ol_abrt_cnt = 0;
  uint64_t s_abrt_cnt = 0;
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
   all_abort_cnt += _stats[tid]->all_abort.cnt;
   w_cflt_cnt += _stats[tid]->w_cflt.cnt;
   d_cflt_cnt += _stats[tid]->d_cflt.cnt;
   cnp_cflt_cnt += _stats[tid]->cnp_cflt.cnt;
   c_cflt_cnt += _stats[tid]->c_cflt.cnt;
   ol_cflt_cnt += _stats[tid]->ol_cflt.cnt;
   s_cflt_cnt += _stats[tid]->s_cflt.cnt;
   w_abrt_cnt += _stats[tid]->w_abrt.cnt;
   d_abrt_cnt += _stats[tid]->d_abrt.cnt;
   cnp_abrt_cnt += _stats[tid]->cnp_abrt.cnt;
   c_abrt_cnt += _stats[tid]->c_abrt.cnt;
   ol_abrt_cnt += _stats[tid]->ol_abrt.cnt;
   s_abrt_cnt += _stats[tid]->s_abrt.cnt;
  }
  printf("\n[all_abort %ld] ",all_abort_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->all_abort.print(outf);
#if WORKLOAD == TPCC
  printf("\n[w_cflt %ld] ",w_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->w_cflt.print(outf);
  printf("\n[d_cflt %ld] ",d_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->d_cflt.print(outf);
  printf("\n[cnp_cflt %ld] ",cnp_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->cnp_cflt.print(outf);
  printf("\n[c_cflt %ld] ",c_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->c_cflt.print(outf);
  printf("\n[ol_cflt %ld] ",ol_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->ol_cflt.print(outf);
  printf("\n[s_cflt %ld] ",s_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->s_cflt.print(outf);
  printf("\n[w_abrt %ld] ",w_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->w_abrt.print(outf);
  printf("\n[d_abrt %ld] ",d_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->d_abrt.print(outf);
  printf("\n[cnp_abrt %ld] ",cnp_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->cnp_abrt.print(outf);
  printf("\n[c_abrt %ld] ",c_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->c_abrt.print(outf);
  printf("\n[ol_abrt %ld] ",ol_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->ol_abrt.print(outf);
  printf("\n[s_abrt %ld] ",s_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->s_abrt.print(outf);
#endif

  fprintf(outf,"\n");

}
*/

void Stats::print_lat_distr() {
#if PRT_LAT_DISTR
  printf("\n[all_lat] ");
  uint64_t limit = 0;
  if(g_node_id < g_node_cnt)
    limit = g_thread_cnt;
  else
    limit = g_client_thread_cnt;
	for (UInt32 tid = 0; tid < limit; tid ++) 
    _stats[tid]->all_lat.print(stdout);
#endif
}

void Stats::print_lat_distr(uint64_t min, uint64_t max) {
#if PRT_LAT_DISTR
  printf("\n[all_lat] ");
  _stats[0]->all_lat.print(stdout,min,max);
#endif
}

void Stats::util_init() {
  struct tms timeSample;
  lastCPU = times(&timeSample);
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;
}

void Stats::print_util() {
}

int Stats::parseLine(char* line){
  int i = strlen(line);
  while (*line < '0' || *line > '9') line++;
  line[i-3] = '\0';
  i = atoi(line);
  return i;
}

void Stats::mem_util(FILE * outf) {
  FILE* file = fopen("/proc/self/status", "r");
  int result = -1;
  char line[128];

// Physical memory used by current process, in KB
  while (fgets(line, 128, file) != NULL){
      if (strncmp(line, "VmRSS:", 6) == 0){
          result = parseLine(line);
          fprintf(outf,
            ",phys_mem_usage=%d"
            ,result
            );
      }
      if (strncmp(line, "VmSize:", 7) == 0){
          result = parseLine(line);
          fprintf(outf,
            ",virt_mem_usage=%d"
            ,result
            );
      }
  }
  fclose(file);

}

void Stats::cpu_util(FILE * outf) {
  clock_t now;
  struct tms timeSample;
  double percent;

  now = times(&timeSample);
  if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
      //Overflow detection. Just skip this value.
      percent = -1.0;
  }
  else{
      percent = (timeSample.tms_stime - lastSysCPU) +
          (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      if(ISSERVER) {
        percent /= (g_total_thread_cnt);//numProcessors;
      } else if(ISCLIENT){
        percent /= (g_total_client_thread_cnt);//numProcessors;
      }
      percent *= 100;
  }
  fprintf(outf,
      ",cpu_ttl=%f"
      ,percent
    );
  lastCPU = now;
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;
}


