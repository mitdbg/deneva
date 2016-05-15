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
#include "ycsb.h"
#include "tpcc.h"
#include "thread.h"
#include "worker_thread.h"
#include "calvin_thread.h"
#include "abort_thread.h"
#include "io_thread.h"
#include "log_thread.h"
#include "manager.h"
#include "math.h"
#include "query.h"
#include "occ.h"
#include "transport.h"
#include "msg_queue.h"
#include "ycsb_query.h"
#include "sequencer.h"
#include "logger.h"
#include "sim_manager.h"
#include "abort_queue.h"
#include "work_queue.h"
#include "maat.h"
#include "client_query.h"

void * f(void *);
void * g(void *);
void * worker(void *);
void * nn_worker(void *);
void * log_worker(void *);
void * send_worker(void *);
void * abort_worker(void *);
void * calvin_lock_worker(void * id); 
void * calvin_seq_worker(void * id); 
void * calvin_worker(void * id); 
void network_test();
void network_test_recv();
void * run_thread(void *);


WorkerThread * worker_thds;
InputThread * input_thds;
OutputThread * output_thds;
AbortThread * abort_thds;
LogThread * log_thds;
#if CC_ALG == CALVIN
CalvinThread * calvin_thds;
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	// 0. initialize global data structure
	parser(argc, argv);
#if SEED != 0
  uint64_t seed = SEED + g_node_id;
#else
	uint64_t seed = get_sys_clock();
#endif
	srand(seed);
	printf("Random seed: %ld\n",seed);

#if ISOLATION_LEVEL == READ_COMMITTED
  assert(CC_ALG == NO_WAIT);
#endif

	int64_t starttime;
	int64_t endtime;
  starttime = get_server_clock();
  printf("Initializing stats... ");
  fflush(stdout);
	stats.init(g_total_thread_cnt);
  printf("Done\n");
  printf("Initializing global manager... ");
  fflush(stdout);
	glob_manager.init();
  printf("Done\n");
	Workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new YCSBWorkload; break;
		case TPCC :
			m_wl = new TPCCWorkload; break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("Workload initialized!\n");
  fflush(stdout);
  printf("Initializing simulation... ");
  fflush(stdout);
  simulation = new SimManager;
  simulation->init();
  printf("Done\n");
#if NETWORK_TEST
	tport_man.init(g_node_id,m_wl);
	sleep(3);
	if(g_node_id == 0)
		network_test();
	else if(g_node_id == 1)
		network_test_recv();

	return 0;
#endif


  printf("Initializing transport manager... ");
  fflush(stdout);
	tport_man.init(m_wl);
  printf("Done\n");
  fflush(stdout);
  printf("Initializing work queue... ");
  fflush(stdout);
  work_queue.init();
  printf("Done\n");
  printf("Initializing abort queue... ");
  fflush(stdout);
  abort_queue.init();
  printf("Done\n");
  printf("Initializing message queue... ");
  fflush(stdout);
  msg_queue.init();
  printf("Done\n");
  printf("Initializing transaction manager pool... ");
  fflush(stdout);
  txn_man_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing transaction pool... ");
  fflush(stdout);
  txn_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing row pool... ");
  fflush(stdout);
  row_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing access pool... ");
  fflush(stdout);
  access_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing txn node table pool... ");
  fflush(stdout);
  txn_table_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing query pool... ");
  fflush(stdout);
  qry_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing msg pool... ");
  fflush(stdout);
  msg_pool.init(m_wl,0);
  printf("Done\n");
  printf("Initializing transaction table... ");
  fflush(stdout);
  txn_table.init();
  printf("Done\n");
#if CC_ALG == CALVIN
  printf("Initializing sequencer... ");
  fflush(stdout);
  seq_man.init(m_wl);
  printf("Done\n");
#endif
#if CC_ALG == MAAT
  printf("Initializing Time Table... ");
  fflush(stdout);
  time_table.init();
  printf("Done\n");
  printf("Initializing MaaT manager... ");
  fflush(stdout);
	maat_man.init();
  printf("Done\n");
#endif
#if LOGGING
  printf("Initializing logger... ");
  fflush(stdout);
  logger.init("logfile.log");
  printf("Done\n");
#endif

#if SERVER_GENERATE_QUERIES
  printf("Initializing client query queue... ");
  fflush(stdout);
  client_query_queue.init(m_wl);
  printf("Done\n");
  fflush(stdout);
#endif

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	uint64_t wthd_cnt = thd_cnt;
	uint64_t rthd_cnt = g_rem_thread_cnt;
	uint64_t sthd_cnt = g_send_thread_cnt;
  uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt + 1;
#if LOGGING
  all_thd_cnt += 1; // logger thread
#endif
#if CC_ALG == CALVIN
  assert(thd_cnt >= 3);
  wthd_cnt -= 2;
#endif
	
	pthread_t * p_thds = 
		(pthread_t *) malloc(sizeof(pthread_t) * (all_thd_cnt));
	pthread_attr_t attr;
	pthread_attr_init(&attr);

  worker_thds = new WorkerThread[wthd_cnt];
  input_thds = new InputThread[rthd_cnt];
  output_thds = new OutputThread[sthd_cnt];
  abort_thds = new AbortThread[1];
  log_thds = new LogThread[1];
#if CC_ALG == CALVIN
  calvin_thds = new CalvinThread[wthd_cnt];
  calvin_lock_thds = new CalvinLockThread[1];
  calvin_seq_thds = new CalvinSequencerThread[1];
#endif
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	//if (WORKLOAD != TEST) {
	//	query_queue.init(m_wl);
	//}
#if CC_ALG == OCC
  printf("Initializing occ lock manager... ");
	occ_man.init();
  printf("Done\n");
#endif

  /*
  printf("Initializing threads... ");
  fflush(stdout);
	for (uint32_t i = 0; i < all_thd_cnt; i++) 
		m_thds[i].init(i, g_node_id, m_wl);
  printf("Done\n");
  fflush(stdout);
  */

  endtime = get_server_clock();
  printf("Initialization Time = %ld\n", endtime - starttime);
  fflush(stdout);
	warmup_done = true;
	pthread_barrier_init( &warmup_bar, NULL, all_thd_cnt);

#if SET_AFFINITY
	uint64_t cpu_cnt = 0;
  cpu_set_t cpus;
#endif
	// spawn and run txns again.
	starttime = get_server_clock();

  uint64_t id = 0;
	for (uint64_t i = 0; i < wthd_cnt; i++) {
#if SET_AFFINITY
		CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		cpu_cnt++;
#endif
    assert(id >= 0 && id < wthd_cnt);
#if CC_ALG == CALVIN
		//pthread_create(&p_thds[i], &attr, calvin_worker, (void *)vid);
    calvin_thds[i].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_thds[i]);
#else
    worker_thds[i].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
#endif
  }

#if CC_ALG == CALVIN
  calvin_lock_thds[0].init(id,g_node_id,m_wl);
  pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
  calvin_seq_thds[0].init(id,g_node_id,m_wl);
  pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
#endif

	for (uint64_t j = 0; j < rthd_cnt ; j++) {
		//uint64_t vid = i;
		//pthread_create(&p_thds[i], NULL, nn_worker, (void *)vid);
    assert(id >= wthd_cnt && id < wthd_cnt + rthd_cnt);
    input_thds[j].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&input_thds[j]);
  }


	for (uint64_t j = 0; j < sthd_cnt; j++) {
		//uint64_t vid = i;
    assert(id >= wthd_cnt + rthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt);
    output_thds[j].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&output_thds[j]);
		//pthread_create(&p_thds[i], &attr, send_worker, (void *)vid);
  }
#if LOGGING
		//pthread_create(&p_thds[i], NULL, log_worker, (void *)i);
    log_thds[0].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&log_thds[0]);
#endif

  abort_thds[0].init(id,g_node_id,m_wl);
  pthread_create(&p_thds[id++], NULL, run_thread, (void *)&abort_thds[0]);
  //abort_worker((void *)(i));

	for (uint64_t i = 0; i < all_thd_cnt ; i++) 
		pthread_join(p_thds[i], NULL);

	endtime = get_server_clock();
	
  printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
  if (STATS_ENABLE)
    stats.print(false);
  //malloc_stats_print(NULL, NULL, NULL);
  printf("\n");
  fflush(stdout);
  // Free things
	//tport_man.shutdown();

  /*
  txn_table.delete_all();
  txn_pool.free_all();
  access_pool.free_all();
  txn_table_pool.free_all();
  msg_pool.free_all();
  qry_pool.free_all();
  */
	return 0;
}

/*
void * worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run();
	return NULL;
}

void * nn_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_remote();
	return NULL;
}

void * log_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_logger();
	return NULL;
}
void * abort_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_abort();
	return NULL;
}


void * send_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_send();
	return NULL;
}

void * calvin_seq_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_calvin_seq();
	return NULL;
}

void * calvin_lock_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_calvin_lock();
	return NULL;
}

void * calvin_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_calvin();
	return NULL;
}
*/

void * run_thread(void * id) {
  Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}

void network_test() {

      /*
	ts_t start;
	ts_t end;
	ts_t time;
	int bytes;
  float total = 0;
	for (int i = 0; i < 4; ++i) {
		time = 0;
		int num_bytes = (int) pow(10,i);
		printf("Network Bytes: %d\nns: ", num_bytes);
		for(int j = 0;j < 1000; j++) {
			start = get_sys_clock();
			tport_man.simple_send_msg(num_bytes);
			while((bytes = tport_man.simple_recv_msg()) == 0) {}
			end = get_sys_clock();
			assert(bytes == num_bytes);
			time = end-start;
      total += time;
			//printf("%lu\n",time);
		}
		printf("Avg(s): %f\n",total/BILLION/1000);
    fflush(stdout);
		//time = time/1000;
		//printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
		//printf("Network Bytes: %d, ns: %.3f\n",i,time);
		
	}
      */

}

void network_test_recv() {
  /*
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
  */
}
