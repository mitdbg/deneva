#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"
#include "client_txn.h"

void StatsArr::init(uint64_t size,StatsArrType type) {
	arr = (uint64_t *)
		mem_allocator.alloc(sizeof(uint64_t) * (size+1), 0);
  for(uint64_t i=0;i<size+1;i++) {
    arr[i] = 0;
  }
  this->size = size+1;
  this->type = type;
  cnt = 0;
}

void StatsArr::resize() {
  size = size * 2;
	arr = (uint64_t *)
		mem_allocator.realloc(arr,sizeof(uint64_t) * size, 0);
  for(uint64_t i=size/2;i<size;i++) {
    arr[i] = 0;
  }
}

void StatsArr::insert(uint64_t item) {
  if(type == ArrIncr) {
    if(cnt == size)
      resize();
    arr[cnt++] = item;
  }
  else if(type == ArrInsert) {
    /*
    while(item >= size) {
      resize();
    }
    */
    if(item >= size) {
      arr[size-1]++;
    }
    else {
      arr[item]++;
    }
    cnt++;
  }
}

void StatsArr::print(FILE * f) {
  if(type == ArrIncr) {
	  for (UInt32 i = 0; i < cnt; i ++) {
	    fprintf(f,"%ld,", arr[i]);
	  }
  }
  else if(type == ArrInsert) {
	  for (UInt64 i = 0; i < size; i ++) {
      if(arr[i] > 0)
	      fprintf(f,"%ld=%ld,", i,arr[i]);
	  }
  }
}

void Stats_thd::init(uint64_t thd_id) {
	clear();
//	all_lat = new uint64_t [MAX_TXN_PER_PART]; 
	all_lat.init(MAX_TXN_PER_PART,ArrIncr);
  /*
	all_lat = (uint64_t *)
		mem_allocator.alloc(sizeof(uint64_t) * MAX_TXN_PER_PART, thd_id);
    */

  all_abort.init(STAT_ARR_SIZE,ArrInsert);
  w_cflt.init(WH_TAB_SIZE,ArrInsert);
  d_cflt.init(DIST_TAB_SIZE,ArrInsert);
  cnp_cflt.init(CUST_TAB_SIZE,ArrInsert);
  c_cflt.init(CUST_TAB_SIZE,ArrInsert);
  ol_cflt.init(ITEM_TAB_SIZE,ArrInsert);
  s_cflt.init(STOC_TAB_SIZE,ArrInsert);
  w_abrt.init(WH_TAB_SIZE,ArrInsert);
  d_abrt.init(DIST_TAB_SIZE,ArrInsert);
  cnp_abrt.init(CUST_TAB_SIZE,ArrInsert);
  c_abrt.init(CUST_TAB_SIZE,ArrInsert);
  ol_abrt.init(ITEM_TAB_SIZE,ArrInsert);
  s_abrt.init(STOC_TAB_SIZE,ArrInsert);

}

void Stats_thd::clear() {
	txn_cnt = 0;
	abort_cnt = 0;
	txn_abort_cnt = 0;
	rbk_abort_cnt = 0;
	tot_run_time = 0;
	run_time = 0;
	time_clock_wait = 0;
	time_clock_rwait = 0;
	time_work = 0;
	time_man = 0;
	time_rqry = 0;
	time_lock_man = 0;
	time_clock_wait = 0;
	time_clock_rwait = 0;
	debug1 = 0;
	debug2 = 0;
	debug3 = 0;
	debug4 = 0;
	debug5 = 0;
	rlk = 0;
	rulk = 0;
	rlk_rsp = 0;
	rulk_rsp = 0;
	rqry = 0;
	rqry_rsp = 0;
	rfin = 0;
	rack = 0;
	rprep = 0;
	rinit = 0;
	rtxn = 0;
	time_index = 0;
	rtime_index = 0;
	time_abort = 0;
	time_cleanup = 0;
	time_qq = 0;
	time_wait = 0;
	time_wait_lock_rem = 0;
	time_wait_lock = 0;
	time_wait_rem = 0;
  time_tport_send = 0;
  time_tport_rcv = 0;
  time_validate = 0;
	time_ts_alloc = 0;
	latency = 0;
	tport_lat = 0;
	time_query = 0;
	rtime_proc = 0;
	time_unpack = 0;
	lock_diff = 0;
  qq_full = 0;
  qq_cnt = 0;
  qq_lat = 0;
  aq_full = 0;

  time_getqry = 0;
  client_latency = 0;
  txn_sent = 0;

	cc_wait_cnt = 0;
	cc_wait_abrt_cnt = 0;
	cc_wait_time = 0;
	cc_wait_abrt_time = 0;
	cc_hold_time = 0;
	cc_hold_abrt_time = 0;

  txn_time_idx = 0;
  txn_time_man = 0;
  txn_time_ts = 0;
  txn_time_abrt = 0;
  txn_time_clean = 0;
  txn_time_copy = 0;
  txn_time_wait = 0;
  txn_time_twopc = 0;
  txn_time_q_abrt = 0;
  txn_time_q_work = 0;
  txn_time_net = 0;
  txn_time_misc = 0;

  cflt_cnt = 0;
	mpq_cnt = 0;
	msg_bytes = 0;
	msg_sent_cnt = 0;
	msg_rcv_cnt = 0;
	time_msg_sent = 0;

  spec_abort_cnt = 0;
  spec_commit_cnt = 0;
  batch_cnt = 0;

}

void Stats_tmp::init() {
	clear();
}

void Stats_tmp::clear() {	
	/*
	time_man = 0;
	time_index = 0;
	time_wait = 0;
	time_wait_lock = 0;
	time_wait_rem = 0;
	*/
	mpq_cnt = 0;
	cc_wait_cnt = 0;
	cc_wait_abrt_cnt = 0;
	cc_wait_time = 0;
	cc_wait_abrt_time = 0;
	cc_hold_time = 0;
	cc_hold_abrt_time = 0;
}

void Stats::init() {
	if (!STATS_ENABLE) 
		return;
	_stats = new Stats_thd * [g_thread_cnt + g_rem_thread_cnt];
	tmp_stats = new Stats_tmp * [g_thread_cnt + g_rem_thread_cnt];
	dl_detect_time = 0;
	dl_wait_time = 0;
	deadlock = 0;
	cycle_detect = 0;
}

void Stats::init(uint64_t thread_id) {
	if (!STATS_ENABLE) 
		return;
	_stats[thread_id] = (Stats_thd *) 
		mem_allocator.alloc(sizeof(Stats_thd), thread_id);
	tmp_stats[thread_id] = (Stats_tmp *)
		mem_allocator.alloc(sizeof(Stats_tmp), thread_id);

	_stats[thread_id]->init(thread_id);
	tmp_stats[thread_id]->init();
}

void Stats::clear(uint64_t tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		tmp_stats[tid]->clear();

		dl_detect_time = 0;
		dl_wait_time = 0;
		cycle_detect = 0;
		deadlock = 0;
	}
}

/*
void Stats::add_lat(uint64_t thd_id, uint64_t latency) {
#if PRT_LAT_DISTR
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		_stats[thd_id]->all_lat[tnum] = latency;
	}
#endif
}
*/

void Stats::commit(uint64_t thd_id) {
	if (STATS_ENABLE) {
		_stats[thd_id]->mpq_cnt += tmp_stats[thd_id]->mpq_cnt;
		_stats[thd_id]->cc_wait_cnt += tmp_stats[thd_id]->cc_wait_cnt;
		_stats[thd_id]->cc_wait_abrt_cnt += tmp_stats[thd_id]->cc_wait_abrt_cnt;
		_stats[thd_id]->cc_wait_time += tmp_stats[thd_id]->cc_wait_time;
		_stats[thd_id]->cc_wait_abrt_time += tmp_stats[thd_id]->cc_wait_abrt_time;
		_stats[thd_id]->cc_hold_time += tmp_stats[thd_id]->cc_hold_time;
		_stats[thd_id]->cc_hold_abrt_time += tmp_stats[thd_id]->cc_hold_abrt_time;
		tmp_stats[thd_id]->init();
	}
}

void Stats::abort(uint64_t thd_id) {	
	if (STATS_ENABLE) 
		tmp_stats[thd_id]->init();
}

void Stats::print_sequencer(bool prog) {
	fflush(stdout);

	uint64_t total_txn_cnt = 0;
	uint64_t total_txn_sent = 0;
	double total_tot_run_time = 0;
	double total_seq_latency = 0;
	double total_time_tport_send = 0;
	double total_time_tport_rcv = 0;
	double total_tport_lat = 0;
	uint64_t total_msg_bytes = 0;
	uint64_t total_msg_sent_cnt = 0;
	uint64_t total_msg_rcv_cnt = 0;
	double total_time_msg_sent = 0;
	double total_time_getqry = 0;
	uint32_t total_batches_sent = 0;

	uint64_t limit = g_seq_thread_cnt;
	for (uint64_t tid = 0; tid < limit; tid ++) {
		if(!prog)
			total_tot_run_time += _stats[tid]->tot_run_time;
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_txn_sent += _stats[tid]->txn_sent;
		total_seq_latency += _stats[tid]->client_latency;
		total_time_getqry += _stats[tid]->time_getqry;
		total_time_tport_send += _stats[tid]->time_tport_send;
		total_time_tport_rcv += _stats[tid]->time_tport_rcv;
		total_tport_lat += _stats[tid]->tport_lat;
		total_msg_bytes += _stats[tid]->msg_bytes;
		total_msg_sent_cnt += _stats[tid]->msg_sent_cnt;
		total_msg_rcv_cnt += _stats[tid]->msg_rcv_cnt;
		total_time_msg_sent += _stats[tid]->time_msg_sent;
		total_batches_sent += _stats[tid]->batch_cnt;
  }
  if(prog)
		total_tot_run_time += _stats[0]->tot_run_time;
  else
		total_tot_run_time = total_tot_run_time / g_seq_thread_cnt;

	FILE * outf;
	if (output_file != NULL) 
		outf = fopen(output_file, "w");
  else 
    outf = stdout;
  if(prog)
	  fprintf(outf, "[prog] ");
  else
	  fprintf(outf, "[summary] ");
	fprintf(outf, 
      "clock_time=%f"
      ",txn_cnt=%ld"
      ",txns_sent=%ld"
      ",time_getqry=%f"
      ",latency=%f"
      ",msg_bytes=%ld"
      ",msg_rcv=%ld"
      ",msg_sent=%ld"
			",time_msg_sent=%f"
      ",time_tport_send=%f"
      ",time_tport_rcv=%f"
      ",tport_lat=%f"
      ",batches_sent=%u"
			"\n",
			total_tot_run_time / BILLION,
			total_txn_cnt, 
			total_txn_sent, 
			total_time_getqry / BILLION,
			total_seq_latency,
			total_msg_bytes, 
			total_msg_rcv_cnt, 
			total_msg_sent_cnt, 
			total_time_msg_sent / BILLION,
			total_time_tport_send / BILLION,
			total_time_tport_rcv / BILLION,
			total_tport_lat / BILLION / total_msg_rcv_cnt,
			total_batches_sent
		);
  /*
	fprintf(outf, 
      "clock_time=%f"
      ",txns_sent=%ld"
      ",time_getqry=%f"
      ",latency=%f"
			"\n",
			(_stats[tid]->tot_run_time ) / BILLION,
			_stats[tid]->txn_cnt,
			_stats[tid]->time_getqry / BILLION,
			_stats[tid]->client_latency / BILLION / _stats[tid]->txn_cnt
		);
    */
    //if(prog) {
	//	  //for (uint32_t k = 0; k < g_node_id; ++k) {
	//	  for (uint32_t k = 0; k < g_servers_per_client; ++k) {
    //    printf("tif_node%u=%d, "
    //        ,k,client_man.get_inflight(k)
    //        );
    //  }
      printf("\n");
    }

void Stats::print_client(bool prog) {
  fflush(stdout);

	uint64_t total_txn_cnt = 0;
	uint64_t total_txn_sent = 0;
	double total_tot_run_time = 0;
	double total_client_latency = 0;
	double total_time_tport_send = 0;
	double total_time_tport_rcv = 0;
	double total_tport_lat = 0;
	uint64_t total_msg_bytes = 0;
	uint64_t total_msg_sent_cnt = 0;
	uint64_t total_msg_rcv_cnt = 0;
	double total_time_msg_sent = 0;
  double total_time_getqry = 0;

  uint64_t limit;
  if(g_node_id < g_node_cnt)
    limit =  g_thread_cnt + g_rem_thread_cnt;
  else
    limit =  g_client_thread_cnt + g_client_rem_thread_cnt;
	for (uint64_t tid = 0; tid < limit; tid ++) {
    if(!prog)
		  total_tot_run_time += _stats[tid]->tot_run_time;
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_txn_sent += _stats[tid]->txn_sent;
		total_client_latency += _stats[tid]->client_latency;
		total_time_getqry += _stats[tid]->time_getqry;
		total_time_tport_send += _stats[tid]->time_tport_send;
		total_time_tport_rcv += _stats[tid]->time_tport_rcv;
		total_tport_lat += _stats[tid]->tport_lat;
		total_msg_bytes += _stats[tid]->msg_bytes;
		total_msg_sent_cnt += _stats[tid]->msg_sent_cnt;
		total_msg_rcv_cnt += _stats[tid]->msg_rcv_cnt;
		total_time_msg_sent += _stats[tid]->time_msg_sent;
  }
  if(prog)
		total_tot_run_time += _stats[0]->tot_run_time;
  else
		total_tot_run_time = total_tot_run_time / g_client_thread_cnt;

	FILE * outf;
	if (output_file != NULL) 
		outf = fopen(output_file, "w");
  else 
    outf = stdout;
  if(prog)
	  fprintf(outf, "[prog] ");
  else
	  fprintf(outf, "[summary] ");
	fprintf(outf, 
      "clock_time=%f"
      ",txn_cnt=%ld"
      ",txn_sent=%ld"
      ",time_getqry=%f"
      ",latency=%f"
      ",msg_bytes=%ld"
      ",msg_rcv=%ld"
      ",msg_sent=%ld"
			",time_msg_sent=%f"
      ",time_tport_send=%f"
      ",time_tport_rcv=%f"
      ",tport_lat=%f"
			"\n",
			total_tot_run_time / BILLION,
			total_txn_cnt, 
			total_txn_sent, 
			total_time_getqry / BILLION,
		  total_client_latency / total_txn_cnt / BILLION,
			total_msg_bytes, 
			total_msg_rcv_cnt, 
			total_msg_sent_cnt, 
      total_time_msg_sent / BILLION,
			total_time_tport_send / BILLION,
			total_time_tport_rcv / BILLION,
			total_tport_lat / BILLION / total_msg_rcv_cnt
		);
  /*
	fprintf(outf, 
      "clock_time=%f"
      ",txns_sent=%ld"
      ",time_getqry=%f"
      ",latency=%f"
			"\n",
			(_stats[tid]->tot_run_time ) / BILLION,
			_stats[tid]->txn_cnt,
			_stats[tid]->time_getqry / BILLION,
			_stats[tid]->client_latency / BILLION / _stats[tid]->txn_cnt
		);
    */
    if(prog) {
		  //for (uint32_t k = 0; k < g_node_id; ++k) {
		  for (uint32_t k = 0; k < g_servers_per_client; ++k) {
        printf("tif_node%u=%d, "
            ,k,client_man.get_inflight(k)
            );
      }
      printf("\n");
    } else {

	    print_lat_distr();
    }

	if (output_file != NULL) {
    fflush(outf);
		fclose(outf);
  }
  fflush(stdout);
}
 
void Stats::print_prog(uint64_t tid) {
	FILE * outf;
	if (output_file != NULL) 
		outf = fopen(output_file, "w");
  else 
    outf = stdout;
  uint64_t qry_cnt = _stats[tid]->rtxn +_stats[tid]->rqry_rsp +_stats[tid]->rack +_stats[tid]->rinit +_stats[tid]->rqry +_stats[tid]->rprep +_stats[tid]->rfin;
	fprintf(outf, "[prog %ld] "
      "txn_cnt=%ld"
			",clock_time=%f"
      ",abort_cnt=%ld"
      ",txn_abort_cnt=%ld"
      ",rbk_abort_cnt=%ld"
      ",latency=%f"
      ",run_time=%f"
      ",aq_full=%f"
			",cc_wait_cnt=%ld"
			",cc_wait_time=%f"
			",cc_hold_time=%f"
			",cc_wait_abrt_cnt=%ld"
			",cc_wait_abrt_time=%f"
			",cc_hold_abrt_time=%f"
      ",cflt_cnt=%ld"
			",mpq_cnt=%ld"
      ",msg_bytes=%ld"
      ",msg_rcv=%ld"
      ",msg_sent=%ld"
      ",qq_full=%f"
      ",qq_lat=%f"
      ",qry_cnt=%ld"
      ",qry_rtxn=%ld"
      ",qry_rqry_rsp=%ld"
      ",qry_rack=%ld"
      ",qry_rinit=%ld"
      ",qry_rqry=%ld"
      ",qry_rprep=%ld"
      ",qry_rfin=%ld"
      ",spec_abort_cnt=%ld"
      ",spec_commit_cnt=%ld"
      ",time_abort=%f"
      ",time_cleanup=%f"
			",time_index=%f"
      ",time_lock_man=%f"
			",time_man=%f"
			",time_msg_sent=%f"
      ",time_tport_send=%f"
      ",time_tport_rcv=%f"
      ",time_ts_alloc=%f"
      ",time_clock_wait=%f"
      ",time_clock_rwait=%f"
      ",time_validate=%f"
      ",time_wait=%f"
      ",time_wait_lock=%f"
      ",time_wait_lock_rem=%f"
      ",time_wait_rem=%f"
      ",time_work=%f"
      ",time_rqry=%f"
      ",tport_lat=%f"
			"\n",
      tid,
			_stats[tid]->txn_cnt, 
			(_stats[tid]->tot_run_time ) / BILLION,
			_stats[tid]->abort_cnt,
			_stats[tid]->txn_abort_cnt,
			_stats[tid]->rbk_abort_cnt,
			((float)_stats[tid]->latency) / BILLION / _stats[tid]->txn_cnt,
			_stats[tid]->run_time / BILLION,
			_stats[tid]->aq_full / BILLION,
			_stats[tid]->cc_wait_cnt, 
			_stats[tid]->cc_wait_time / BILLION, 
			_stats[tid]->cc_hold_time / BILLION, 
			_stats[tid]->cc_wait_abrt_cnt, 
			_stats[tid]->cc_wait_abrt_time / BILLION, 
			_stats[tid]->cc_hold_abrt_time / BILLION, 
			_stats[tid]->cflt_cnt, 
			_stats[tid]->mpq_cnt, 
			_stats[tid]->msg_bytes, 
			_stats[tid]->msg_rcv_cnt, 
			_stats[tid]->msg_sent_cnt, 
			_stats[tid]->qq_full / BILLION,
			_stats[tid]->qq_lat / _stats[tid]->qq_cnt,
			qry_cnt,
			_stats[tid]->rtxn,
			_stats[tid]->rqry_rsp,
			_stats[tid]->rack,
			_stats[tid]->rinit,
			_stats[tid]->rqry,
			_stats[tid]->rprep,
			_stats[tid]->rfin,
			_stats[tid]->spec_abort_cnt, 
			_stats[tid]->spec_commit_cnt, 
			_stats[tid]->time_abort / BILLION,
			_stats[tid]->time_cleanup / BILLION,
			_stats[tid]->time_index / BILLION,
			_stats[tid]->time_lock_man / BILLION,
			_stats[tid]->time_man / BILLION,
			_stats[tid]->time_msg_sent / BILLION,
			_stats[tid]->time_tport_send / BILLION,
			_stats[tid]->time_tport_rcv / BILLION,
			((float)_stats[tid]->time_ts_alloc) / BILLION,
			_stats[tid]->time_clock_wait / BILLION,
			_stats[tid]->time_clock_rwait / BILLION,
			_stats[tid]->time_validate / BILLION,
			_stats[tid]->time_wait / BILLION,
			_stats[tid]->time_wait_lock / BILLION,
			_stats[tid]->time_wait_lock_rem / BILLION,
			_stats[tid]->time_wait_rem / BILLION,
			_stats[tid]->time_work / BILLION,
			_stats[tid]->time_rqry / BILLION,
			_stats[tid]->tport_lat / BILLION / _stats[tid]->msg_rcv_cnt
		);
  print_cnts();
	if (output_file != NULL) 
		fclose(outf);
  else
    fflush(stdout);
}

void Stats::print(bool prog) {

  fflush(stdout);
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	uint64_t total_txn_abort_cnt = 0;
	uint64_t total_rbk_abort_cnt = 0;
	double total_tot_run_time = 0;
	double total_run_time = 0;
	double total_time_work = 0;
	double total_time_man = 0;
	double total_time_rqry = 0;
	double total_time_lock_man = 0;
	double total_time_clock_wait = 0;
	double total_time_clock_rwait = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_qq_full = 0;
	double total_qq_cnt = 0;
	double total_qq_lat = 0;
	double total_aq_full = 0;
	double total_time_index = 0;
	double total_rtime_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_qq = 0;
	double total_time_wait = 0;
	double total_time_wait_lock_rem = 0;
	double total_time_wait_lock = 0;
	double total_time_wait_rem = 0;
	double total_time_tport_send = 0;
	double total_time_tport_rcv = 0;
	double total_time_validate = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_tport_lat = 0;
	double total_time_query = 0;
	double total_rtime_proc = 0;
	double total_time_unpack = 0;
	uint64_t total_cc_wait_cnt = 0;
	double total_cc_wait_time = 0;
	double total_cc_hold_time = 0;
	uint64_t total_cc_wait_abrt_cnt = 0;
	double total_cc_wait_abrt_time = 0;
	double total_cc_hold_abrt_time = 0;
	uint64_t total_cflt_cnt = 0;
	uint64_t total_spec_commit_cnt = 0;
	uint64_t total_spec_abort_cnt = 0;
	uint64_t total_mpq_cnt = 0;
	uint64_t total_msg_bytes = 0;
	uint64_t total_msg_sent_cnt = 0;
	uint64_t total_msg_rcv_cnt = 0;
	double total_time_msg_sent = 0;


  double total_txn_time_idx = 0;
  double total_txn_time_man = 0;
  double total_txn_time_ts = 0;
  double total_txn_time_abrt = 0;
  double total_txn_time_clean = 0;
  double total_txn_time_copy = 0;
  double total_txn_time_wait = 0;
  double total_txn_time_twopc = 0;
  double total_txn_time_q_abrt = 0;
  double total_txn_time_q_work = 0;
  double total_txn_time_net = 0;
  double total_txn_time_misc = 0;


  uint64_t total_rqry = 0;
  uint64_t total_rqry_rsp = 0;
  uint64_t total_rtxn = 0;
  uint64_t total_rinit = 0;
  uint64_t total_rprep = 0;
  uint64_t total_rfin = 0;
  uint64_t total_rack = 0;
  uint64_t total_qry_cnt = 0;

  uint64_t limit;
  if(g_node_id < g_node_cnt)
    limit =  g_thread_cnt + g_rem_thread_cnt;
  else
    limit =  g_client_thread_cnt + g_client_rem_thread_cnt;
	for (uint64_t tid = 0; tid < limit; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		total_txn_abort_cnt += _stats[tid]->txn_abort_cnt;
		total_rbk_abort_cnt += _stats[tid]->rbk_abort_cnt;
    if(!prog)
		  total_tot_run_time += _stats[tid]->tot_run_time;
		total_run_time += _stats[tid]->run_time;
		total_time_work += _stats[tid]->time_work;
		total_time_man += _stats[tid]->time_man;
		total_time_rqry += _stats[tid]->time_rqry;
		total_time_lock_man += _stats[tid]->time_lock_man;
		total_time_clock_wait += _stats[tid]->time_clock_wait;
		total_time_clock_rwait += _stats[tid]->time_clock_rwait;
		total_debug1 += _stats[tid]->debug1;
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_qq_full += _stats[tid]->qq_full;
		total_qq_cnt += _stats[tid]->qq_cnt;
		total_qq_lat += _stats[tid]->qq_lat;
		total_aq_full += _stats[tid]->aq_full;
		total_rtxn += _stats[tid]->rtxn;
		total_rqry_rsp += _stats[tid]->rqry_rsp;
		total_rack += _stats[tid]->rack;
		total_rinit += _stats[tid]->rinit;
		total_rqry += _stats[tid]->rqry;
		total_rprep += _stats[tid]->rprep;
		total_rfin += _stats[tid]->rfin;
		total_time_index += _stats[tid]->time_index;
		total_rtime_index += _stats[tid]->rtime_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_qq += _stats[tid]->time_qq;
		total_time_wait += _stats[tid]->time_wait;
		total_time_wait_lock_rem += _stats[tid]->time_wait_lock_rem;
		total_time_wait_lock += _stats[tid]->time_wait_lock;
		total_time_wait_rem += _stats[tid]->time_wait_rem;
		total_time_tport_send += _stats[tid]->time_tport_send;
		total_time_tport_rcv += _stats[tid]->time_tport_rcv;
		total_time_validate += _stats[tid]->time_validate;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_tport_lat += _stats[tid]->tport_lat;
		total_time_query += _stats[tid]->time_query;
		total_rtime_proc += _stats[tid]->rtime_proc;
		total_time_unpack += _stats[tid]->time_unpack;

		total_cc_wait_cnt += _stats[tid]->cc_wait_cnt;
		total_cc_wait_time += _stats[tid]->cc_wait_time;
		total_cc_hold_time += _stats[tid]->cc_hold_time;
		total_cc_wait_abrt_cnt += _stats[tid]->cc_wait_abrt_cnt;
		total_cc_wait_abrt_time += _stats[tid]->cc_wait_abrt_time;
		total_cc_hold_abrt_time += _stats[tid]->cc_hold_abrt_time;
		total_cflt_cnt += _stats[tid]->cflt_cnt;
		total_spec_commit_cnt += _stats[tid]->spec_commit_cnt;
		total_spec_abort_cnt += _stats[tid]->spec_abort_cnt;
		total_mpq_cnt += _stats[tid]->mpq_cnt;
		total_msg_bytes += _stats[tid]->msg_bytes;
		total_msg_sent_cnt += _stats[tid]->msg_sent_cnt;
		total_msg_rcv_cnt += _stats[tid]->msg_rcv_cnt;
		total_time_msg_sent += _stats[tid]->time_msg_sent;
		
  total_txn_time_idx += _stats[tid]->txn_time_idx;
  total_txn_time_man += _stats[tid]->txn_time_man;
  total_txn_time_ts += _stats[tid]->txn_time_ts;
  total_txn_time_abrt += _stats[tid]->txn_time_abrt;
  total_txn_time_clean += _stats[tid]->txn_time_clean;
  total_txn_time_copy += _stats[tid]->txn_time_copy;
  total_txn_time_wait += _stats[tid]->txn_time_wait;
  total_txn_time_twopc += _stats[tid]->txn_time_twopc;
  total_txn_time_q_abrt += _stats[tid]->txn_time_q_abrt;
  total_txn_time_q_work += _stats[tid]->txn_time_q_work;
  total_txn_time_net += _stats[tid]->txn_time_net;
  total_txn_time_misc += _stats[tid]->txn_time_misc;

  total_qry_cnt += _stats[tid]->rtxn +_stats[tid]->rqry_rsp +_stats[tid]->rack +_stats[tid]->rinit +_stats[tid]->rqry +_stats[tid]->rprep +_stats[tid]->rfin;

		printf("[tid=%ld] txn_cnt=%ld,abort_cnt=%ld\n", 
			tid,
			_stats[tid]->txn_cnt,
			_stats[tid]->abort_cnt
		);
	}

  if(prog)
		total_tot_run_time += _stats[0]->tot_run_time;
  else
		total_tot_run_time = total_tot_run_time / g_thread_cnt;

	FILE * outf;
	if (output_file != NULL) 
		outf = fopen(output_file, "w");
  else
    outf = stdout;
  if(prog)
	  fprintf(outf, "[prog] ");
  else
	  fprintf(outf, "[summary] ");
	fprintf(outf, 
      "txn_cnt=%ld"
			",clock_time=%f"
      ",abort_cnt=%ld"
      ",txn_abort_cnt=%ld"
      ",rbk_abort_cnt=%ld"
      ",latency=%f"
      ",run_time=%f"
      ",aq_full=%f"
			",cc_wait_cnt=%ld"
			",cc_wait_time=%f"
			",cc_hold_time=%f"
			",cc_wait_abrt_cnt=%ld"
			",cc_wait_abrt_time=%f"
			",cc_hold_abrt_time=%f"
      ",cflt_cnt=%ld"
			",mpq_cnt=%ld"
      ",msg_bytes=%ld"
      ",msg_rcv=%ld"
      ",msg_sent=%ld"
      ",qq_full=%f"
      ",qq_lat=%f"
      ",qry_cnt=%ld"
      ",qry_rtxn=%ld"
      ",qry_rqry_rsp=%ld"
      ",qry_rack=%ld"
      ",qry_rinit=%ld"
      ",qry_rqry=%ld"
      ",qry_rprep=%ld"
      ",qry_rfin=%ld"
      ",spec_abort_cnt=%ld"
      ",spec_commit_cnt=%ld"
      ",time_abort=%f"
      ",time_cleanup=%f"
			",time_index=%f"
      ",time_lock_man=%f"
			",time_man=%f"
			",time_msg_sent=%f"
      ",time_qq=%f"
      ",time_tport_send=%f"
      ",time_tport_rcv=%f"
      ",time_ts_alloc=%f"
      ",time_clock_wait=%f"
      ",time_clock_rwait=%f"
      ",time_validate=%f"
      ",time_wait=%f"
      ",time_wait_lock=%f"
      ",time_wait_lock_rem=%f"
      ",time_wait_rem=%f"
      ",time_work=%f"
      ",time_rqry=%f"
      ",tport_lat=%f"
  ",txn_time_idx=%f"
  ",txn_time_man=%f"
  ",txn_time_ts=%f"
  ",txn_time_abrt=%f"
  ",txn_time_clean=%f"
  ",txn_time_copy=%f"
  ",txn_time_wait=%f"
  ",txn_time_twopc=%f"
  ",txn_time_q_abrt=%f"
  ",txn_time_q_work=%f"
  ",txn_time_net=%f"
  ",txn_time_misc=%f"
			"\n",
			total_txn_cnt, 
			total_tot_run_time / BILLION,
			total_abort_cnt,
			total_txn_abort_cnt,
			total_rbk_abort_cnt,
			total_latency / BILLION / total_txn_cnt,
			total_run_time / BILLION,
			total_aq_full / BILLION,
      total_cc_wait_cnt,
      total_cc_wait_time / BILLION,
      total_cc_hold_time / BILLION,
      total_cc_wait_abrt_cnt,
      total_cc_wait_abrt_time / BILLION,
      total_cc_hold_abrt_time / BILLION,
      total_cflt_cnt,
			total_mpq_cnt, 
			total_msg_bytes, 
			total_msg_rcv_cnt, 
			total_msg_sent_cnt, 
			total_qq_full / BILLION,
			total_qq_lat / total_qq_cnt / BILLION,
			total_qry_cnt,
			total_rtxn,
			total_rqry_rsp ,
			total_rack,
			total_rinit,
			total_rqry ,
			total_rprep,
			total_rfin ,
      total_spec_abort_cnt,
      total_spec_commit_cnt,
			total_time_abort / BILLION,
			total_time_cleanup / BILLION,
			total_time_index / BILLION,
			total_time_lock_man / BILLION,
			total_time_man / BILLION,
      total_time_msg_sent / BILLION,
			total_time_qq / BILLION,
			total_time_tport_send / BILLION,
			total_time_tport_rcv / BILLION,
			total_time_ts_alloc / BILLION,
      total_time_clock_wait / BILLION,
      total_time_clock_rwait / BILLION,
			total_time_validate / BILLION,
			total_time_wait / BILLION,
			total_time_wait_lock / BILLION,
			total_time_wait_lock_rem / BILLION,
			total_time_wait_rem / BILLION,
			total_time_work / BILLION,
			total_time_rqry / BILLION,
			total_tport_lat / BILLION / total_msg_rcv_cnt,
  total_txn_time_idx / BILLION,
  total_txn_time_man / BILLION,
  total_txn_time_ts / BILLION,
  total_txn_time_abrt / BILLION,
  total_txn_time_clean / BILLION,
  total_txn_time_copy / BILLION,
  total_txn_time_wait / BILLION,
  total_txn_time_twopc / BILLION,
  total_txn_time_q_abrt / BILLION,
  total_txn_time_q_work / BILLION,
  total_txn_time_net / BILLION,
  total_txn_time_misc / BILLION
		);

  if(!prog) {
    print_cnts();
	  print_lat_distr();
  }
	if (output_file != NULL) {
    fflush(outf);
		fclose(outf);
  }
  fflush(stdout);

}

void Stats::print_cnts() {
  if(g_node_id >= g_node_cnt)
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
    _stats[tid]->all_abort.print(stdout);
#if WORKLOAD == TPCC
  /*
  printf("\n[w_cflt %ld] ",w_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->w_cflt.print(stdout);
    */
  printf("\n[d_cflt %ld] ",d_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->d_cflt.print(stdout);
  /*
  printf("\n[cnp_cflt %ld] ",cnp_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->cnp_cflt.print(stdout);
  printf("\n[c_cflt %ld] ",c_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->c_cflt.print(stdout);
  printf("\n[ol_cflt %ld] ",ol_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->ol_cflt.print(stdout);
    */
  printf("\n[s_cflt %ld] ",s_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->s_cflt.print(stdout);
  /*
  printf("\n[w_abrt %ld] ",w_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->w_abrt.print(stdout);
    */
  printf("\n[d_abrt %ld] ",d_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->d_abrt.print(stdout);
  /*
  printf("\n[cnp_abrt %ld] ",cnp_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->cnp_abrt.print(stdout);
  printf("\n[c_abrt %ld] ",c_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->c_abrt.print(stdout);
  printf("\n[ol_abrt %ld] ",ol_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->ol_abrt.print(stdout);
    */
  printf("\n[s_abrt %ld] ",s_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->s_abrt.print(stdout);
#endif

  printf("\n");

}

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
  /*
#if PRT_LAT_DISTR
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "a");
		for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
			fprintf(outf, "[all_lat thd=%d] ", tid);
			for (UInt32 tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%f,", (double)_stats[tid]->all_lat[tnum] / BILLION);
			fprintf(outf, "\n");
		}
		fclose(outf);
	} 
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
		printf("[all_lat thd=%d] ", tid);
		for (UInt32 tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
			printf("%f,", (double)_stats[tid]->all_lat[tnum] / BILLION);
		printf("\n");
	}
#endif
*/
}

