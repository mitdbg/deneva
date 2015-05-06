#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"

void StatsArr::init() {
	arr = (uint64_t *)
		mem_allocator.alloc(sizeof(uint64_t) * STAT_ARR_SIZE, 0);
  size = STAT_ARR_SIZE;
  cnt = 0;
}

void StatsArr::resize() {
  size = size * 2;
	arr = (uint64_t *)
		mem_allocator.realloc(arr,sizeof(uint64_t) * size, 0);
}

void StatsArr::insert(uint64_t item) {
  /*
  if(cnt == size)
    resize();
  arr[cnt++] = item;
  */
}

void StatsArr::print(FILE * f) {
	for (UInt32 i = 0; i < cnt; i ++) {
	  fprintf(f,"%ld,", arr[i]);
	}
}

void Stats_thd::init(uint64_t thd_id) {
	clear();
//	all_lat = new uint64_t [MAX_TXN_PER_PART]; 
#if PRT_LAT_DISTR
	all_lat = (uint64_t *)
		mem_allocator.alloc(sizeof(uint64_t) * MAX_TXN_PER_PART, thd_id);
#endif

  all_abort.init();
  w_cflt.init();
  d_cflt.init();
  cnp_cflt.init();
  c_cflt.init();
  ol_cflt.init();
  s_cflt.init();
  w_abrt.init();
  d_abrt.init();
  cnp_abrt.init();
  c_abrt.init();
  ol_abrt.init();
  s_abrt.init();

}

void Stats_thd::clear() {
	txn_cnt = 0;
	abort_cnt = 0;
	txn_abort_cnt = 0;
	tot_run_time = 0;
	run_time = 0;
	time_work = 0;
	time_man = 0;
	time_rqry = 0;
	time_lock_man = 0;
	rtime_lock_man = 0;
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
	time_wait = 0;
	rtime_wait_lock = 0;
	time_wait_lock = 0;
	time_wait_rem = 0;
	time_ts_alloc = 0;
	latency = 0;
	tport_lat = 0;
	time_query = 0;
	rtime_proc = 0;
	rtime_unpack = 0;
	rtime_unpack_ndest = 0;
	lock_diff = 0;

	mpq_cnt = 0;
	msg_bytes = 0;
	msg_sent_cnt = 0;
	msg_rcv_cnt = 0;
	time_msg_wait = 0;
	time_rem_req = 0;
	time_rem = 0;

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

void Stats::add_lat(uint64_t thd_id, uint64_t latency) {
#if PRT_LAT_DISTR
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		_stats[thd_id]->all_lat[tnum] = latency;
	}
#endif
}

void Stats::commit(uint64_t thd_id) {
	if (STATS_ENABLE) {
		/*
		_stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
		_stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
		_stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
		_stats[thd_id]->time_wait_lock += tmp_stats[thd_id]->time_wait_lock;
		_stats[thd_id]->time_wait_rem += tmp_stats[thd_id]->time_wait_rem;
		*/
		_stats[thd_id]->mpq_cnt += tmp_stats[thd_id]->mpq_cnt;
		tmp_stats[thd_id]->init();
	}
}

void Stats::abort(uint64_t thd_id) {	
	if (STATS_ENABLE) 
		tmp_stats[thd_id]->init();
}

void Stats::print() {
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	uint64_t total_txn_abort_cnt = 0;
	double total_tot_run_time = 0;
	double total_run_time = 0;
	double total_time_work = 0;
	double total_time_man = 0;
	double total_time_rqry = 0;
	double total_time_lock_man = 0;
	double total_rtime_lock_man = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_lock_diff = 0;
	double total_time_index = 0;
	double total_rtime_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_wait = 0;
	double total_rtime_wait_lock = 0;
	double total_time_wait_lock = 0;
	double total_time_wait_rem = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_tport_lat = 0;
	double total_time_query = 0;
	double total_rtime_proc = 0;
	double total_rtime_unpack = 0;
	double total_rtime_unpack_ndest = 0;
	uint64_t total_mpq_cnt = 0;
	uint64_t total_msg_bytes = 0;
	uint64_t total_msg_sent_cnt = 0;
	uint64_t total_msg_rcv_cnt = 0;
	double total_time_msg_wait = 0;
	double total_time_rem_req = 0;
	double total_time_rem = 0;
	for (uint64_t tid = 0; tid < g_thread_cnt + g_rem_thread_cnt; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		total_txn_abort_cnt += _stats[tid]->txn_abort_cnt;
		total_tot_run_time += _stats[tid]->tot_run_time;
		total_run_time += _stats[tid]->run_time;
		total_time_work += _stats[tid]->time_work;
		total_time_man += _stats[tid]->time_man;
		total_time_rqry += _stats[tid]->time_rqry;
		total_time_lock_man += _stats[tid]->time_lock_man;
		total_rtime_lock_man += _stats[tid]->rtime_lock_man;
		total_debug1 += _stats[tid]->debug1;
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_lock_diff += _stats[tid]->lock_diff;
		total_time_index += _stats[tid]->time_index;
		total_rtime_index += _stats[tid]->rtime_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_wait += _stats[tid]->time_wait;
		total_rtime_wait_lock += _stats[tid]->rtime_wait_lock;
		total_time_wait_lock += _stats[tid]->time_wait_lock;
		total_time_wait_rem += _stats[tid]->time_wait_rem;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_tport_lat += _stats[tid]->tport_lat;
		total_time_query += _stats[tid]->time_query;
		total_rtime_proc += _stats[tid]->rtime_proc;
		total_rtime_unpack += _stats[tid]->rtime_unpack;
		total_rtime_unpack_ndest += _stats[tid]->rtime_unpack_ndest;

		total_mpq_cnt += _stats[tid]->mpq_cnt;
		total_msg_bytes += _stats[tid]->msg_bytes;
		total_msg_sent_cnt += _stats[tid]->msg_sent_cnt;
		total_msg_rcv_cnt += _stats[tid]->msg_rcv_cnt;
		total_time_msg_wait += _stats[tid]->time_msg_wait;
		total_time_rem_req += _stats[tid]->time_rem_req;
		total_time_rem += _stats[tid]->time_rem;
		
		printf("[tid=%ld] txn_cnt=%ld,abort_cnt=%ld\n", 
			tid,
			_stats[tid]->txn_cnt,
			_stats[tid]->abort_cnt
		);
	}
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "w");
		fprintf(outf, "[summary] txn_cnt=%ld,abort_cnt=%ld,txn_abort_cnt=%ld"
			",tot_run_time=%f,run_time=%f,time_wait=%f,time_wait_lock=%f,rtime_wait_lock=%f,time_wait_rem=%f,time_ts_alloc=%f"
      ",time_work=%f"
			",time_man=%f,time_rqry=%f,time_lock_man=%f,rtime_lock_man=%f"
			",time_index=%f,rtime_index=%f,time_abort=%f,time_cleanup=%f,latency=%f,tport_lat=%f"
			",deadlock_cnt=%ld,cycle_detect=%ld,dl_detect_time=%f,dl_wait_time=%f"
			",time_query=%f,rtime_proc=%f,rtime_unpack=%f,rtime_unpack_ndest=%f"
			",mpq_cnt=%ld,msg_bytes=%ld,msg_sent=%ld,msg_rcv=%ld"
			",time_msg_wait=%f,time_req_req=%f,time_rem=%f,lock_diff=%f"
			",debug1=%f,debug2=%f,debug3=%f,debug4=%f,debug5=%f\n",
			total_txn_cnt, 
			total_abort_cnt,
			total_txn_abort_cnt,
			(total_tot_run_time / g_thread_cnt) / BILLION,
			total_run_time / BILLION,
			total_time_wait / BILLION,
			total_time_wait_lock / BILLION,
			total_rtime_wait_lock / BILLION,
			total_time_wait_rem / BILLION,
			total_time_ts_alloc / BILLION,
			total_time_work / BILLION,
			total_time_man / BILLION,
			total_time_rqry / BILLION,
			total_time_lock_man / BILLION,
			total_rtime_lock_man / BILLION,
			//(total_time_man - total_time_wait - total_time_wait_lock) / BILLION,
			total_time_index / BILLION,
			total_rtime_index / BILLION,
			total_time_abort / BILLION,
			total_time_cleanup / BILLION,
			total_latency / BILLION / total_txn_cnt,
			total_tport_lat / BILLION / total_msg_rcv_cnt,
			deadlock,
			cycle_detect,
			dl_detect_time / BILLION,
			dl_wait_time / BILLION,
			total_time_query / BILLION,
			total_rtime_proc / BILLION,
			total_rtime_unpack / BILLION,
			total_rtime_unpack_ndest / BILLION,
			total_mpq_cnt, 
			total_msg_bytes, 
			total_msg_sent_cnt, 
			total_msg_rcv_cnt, 
			total_time_msg_wait / BILLION,
			total_time_rem_req / BILLION,
			total_time_rem / BILLION, 
			total_lock_diff / BILLION,
			total_debug1 / BILLION,
			total_debug2 / BILLION,
			total_debug3 / BILLION,
			total_debug4 / BILLION,
			total_debug5 / BILLION
		);
		fclose(outf);
	}
	printf("[summary] txn_cnt=%ld,abort_cnt=%ld,txn_abort_cnt=%ld"
		",tot_run_time=%f,run_time=%f,time_wait=%f,time_wait_lock=%f,rtime_wait_lock=%f,time_wait_rem=%f,time_ts_alloc=%f"
    ",time_work=%f"
		",time_man=%f,time_rqry=%f,time_lock_man=%f,rtime_lock_man=%f"
		",time_index=%f,rtime_index=%f,time_abort=%f,time_cleanup=%f,latency=%f,tport_lat=%f"
		",deadlock_cnt=%ld,cycle_detect=%ld,dl_detect_time=%f,dl_wait_time=%f"
		",time_query=%f,rtime_proc=%f,rtime_unpack=%f,rtime_unpack_ndest=%f"
		",mpq_cnt=%ld,msg_bytes=%ld,msg_sent=%ld,msg_rcv=%ld"
		",time_msg_wait=%f,time_req_req=%f,time_rem=%f,lock_diff=%f"
		",debug1=%f,debug2=%f,debug3=%f,debug4=%f,debug5=%f\n",
		total_txn_cnt, 
		total_abort_cnt,
		total_txn_abort_cnt,
		(total_tot_run_time / g_thread_cnt) / BILLION,
		total_run_time / BILLION,
		total_time_wait / BILLION,
		total_time_wait_lock / BILLION,
		total_rtime_wait_lock / BILLION,
		total_time_wait_rem / BILLION,
		total_time_ts_alloc / BILLION,
		total_time_work / BILLION,
		total_time_man / BILLION,
		total_time_rqry / BILLION,
		total_time_lock_man / BILLION,
		total_rtime_lock_man / BILLION,
		total_time_index / BILLION,
		total_rtime_index / BILLION,
		total_time_abort / BILLION,
		total_time_cleanup / BILLION,
		total_latency / BILLION / total_txn_cnt,
		total_tport_lat / BILLION / total_msg_rcv_cnt,
		deadlock,
		cycle_detect,
		dl_detect_time / BILLION,
		dl_wait_time / BILLION,
		total_time_query / BILLION,
		total_rtime_proc / BILLION,
		total_rtime_unpack / BILLION,
		total_rtime_unpack_ndest / BILLION,
		total_mpq_cnt, 
		total_msg_bytes, 
		total_msg_sent_cnt, 
		total_msg_rcv_cnt, 
		total_time_msg_wait / BILLION,
		total_time_rem_req / BILLION,
		total_time_rem / BILLION, 
		total_lock_diff / BILLION,
		total_debug1 / BILLION,
		total_debug2 / BILLION,
		total_debug3 / BILLION,
		total_debug4 / BILLION,
		total_debug5 / BILLION 
	);

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
  printf("\n[w_cflt %ld] ",w_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->w_cflt.print(stdout);
  printf("\n[d_cflt %ld] ",d_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->d_cflt.print(stdout);
  printf("\n[cnp_cflt %ld] ",cnp_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->cnp_cflt.print(stdout);
  printf("\n[c_cflt %ld] ",c_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->c_cflt.print(stdout);
  printf("\n[ol_cflt %ld] ",ol_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->ol_cflt.print(stdout);
  printf("\n[s_cflt %ld] ",s_cflt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->s_cflt.print(stdout);
  printf("\n[w_abrt %ld] ",w_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->w_abrt.print(stdout);
  printf("\n[d_abrt %ld] ",d_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->d_abrt.print(stdout);
  printf("\n[cnp_abrt %ld] ",cnp_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->cnp_abrt.print(stdout);
  printf("\n[c_abrt %ld] ",c_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->c_abrt.print(stdout);
  printf("\n[ol_abrt %ld] ",ol_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->ol_abrt.print(stdout);
  printf("\n[s_abrt %ld] ",s_abrt_cnt);
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) 
    _stats[tid]->s_abrt.print(stdout);

	if (g_prt_lat_distr)
		print_lat_distr();
}

void Stats::print_lat_distr() {
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
}

