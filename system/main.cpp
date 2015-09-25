#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "calvin_thread.h"
#include "manager.h"
#include "math.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "transport.h"
#include "msg_queue.h"
//#include <jemalloc.h>

void * f(void *);
void * g(void *);
void * worker(void *);
void * nn_worker(void *);
void * send_worker(void *);
void network_test();
void network_test_recv();


// TODO the following global variables are HACK
#if CC_ALG == CALVIN
calvin_thread_t * m_thds;
#else
thread_t * m_thds;
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

#if NETWORK_TEST
	tport_man.init(g_node_id);
	sleep(3);
	if(g_node_id == 0)
		network_test();
	else if(g_node_id == 1)
		network_test_recv();

	return 0;
#endif


	int64_t starttime;
	int64_t endtime;
  starttime = get_server_clock();
	// per-partition malloc
  printf("Initializing memory allocator... ");
  fflush(stdout);
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
  printf("Done\n");
  printf("Initializing stats... ");
  fflush(stdout);
	stats.init();
  printf("Done\n");
  printf("Initializing global manager... ");
  fflush(stdout);
	glob_manager.init();
  printf("Done\n");
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("Workload initialized!\n");
  fflush(stdout);

  printf("Initializing remote query manager... ");
  fflush(stdout);
	rem_qry_man.init(g_node_id,m_wl);
  printf("Done\n");
  printf("Initializing transport manager... ");
  fflush(stdout);
	tport_man.init(g_node_id);
  printf("Done\n");
  fflush(stdout);
  printf("Initializing work queue... ");
  work_queue.init();
  printf("Done\n");
  printf("Initializing abort queue... ");
  abort_queue.init();
  printf("Done\n");
  printf("Initializing message queue... ");
  msg_queue.init();
  printf("Done\n");
  printf("Initializing transaction pool... ");
  txn_pool.init(m_wl,g_inflight_max);
  printf("Done\n");
  printf("Initializing query pool... ");
  qry_pool.init(m_wl,g_inflight_max);
  printf("Done\n");
  printf("Initializing transaction table... ");
  txn_table.init();
  printf("Done\n");

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	uint64_t rthd_cnt = g_rem_thread_cnt;
	uint64_t sthd_cnt = g_send_thread_cnt;
  uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt;
	
	pthread_t * p_thds = 
		(pthread_t *) malloc(sizeof(pthread_t) * (all_thd_cnt));
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	cpu_set_t cpus;
#if CC_ALG == CALVIN
	m_thds = new calvin_thread_t[all_thd_cnt];
#else
	m_thds = new thread_t[all_thd_cnt];
#endif
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	//if (WORKLOAD != TEST) {
	//	query_queue.init(m_wl);
	//}
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  printf("Initializing partition lock manager... ");
	part_lock_man.init(g_node_id);
  printf("Done\n");
#elif CC_ALG == OCC
  printf("Initializing occ lock manager... ");
	occ_man.init();
  printf("Done\n");
#elif CC_ALG == VLL
  printf("Initializing vll lock manager... ");
	vll_man.init();
  printf("Done\n");
#endif

  printf("Initializing threads... ");
  fflush(stdout);
	for (uint32_t i = 0; i < all_thd_cnt; i++) 
		m_thds[i].init(i, g_node_id, m_wl);
  printf("Done\n");
  fflush(stdout);

  endtime = get_server_clock();
  printf("Initialization Time = %ld\n", endtime - starttime);
  fflush(stdout);
	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			CPU_ZERO(&cpus);
      CPU_SET(i, &cpus);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
			pthread_create(&p_thds[i], &attr, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, all_thd_cnt);

	uint64_t cpu_cnt = 0;
	// spawn and run txns again.
	starttime = get_server_clock();

  uint64_t i = 0;
	for (i = 0; i < thd_cnt; i++) {
		uint64_t vid = i;
		CPU_ZERO(&cpus);
#if TPORT_TYPE_IPC
    CPU_SET(g_node_id * (g_thread_cnt) + cpu_cnt, &cpus);
#else
    CPU_SET(cpu_cnt, &cpus);
#endif
		cpu_cnt++;
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		pthread_create(&p_thds[i], &attr, worker, (void *)vid);
  }

	for (; i < thd_cnt + sthd_cnt; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], &attr, send_worker, (void *)vid);
  }
	for (; i < thd_cnt + sthd_cnt + rthd_cnt -1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], &attr, nn_worker, (void *)vid);
  }


  nn_worker((void *)(i));

	for (i = 0; i < all_thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);

	endtime = get_server_clock();
	
	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print(false);
    //malloc_stats_print(NULL, NULL, NULL);
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

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

void * send_worker(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_send();
	return NULL;
}

void network_test() {

	ts_t start;
	ts_t end;
	ts_t time;
	int bytes;
	//for(int i=4; i < 257; i+=4) {
	//	time = 0;
	//	for(int j=0;j < 1000; j++) {
	//		start = get_sys_clock();
	//		tport_man.simple_send_msg(i);
	//		while((bytes = tport_man.simple_recv_msg()) == 0) {}
	//		end = get_sys_clock();
	//		assert(bytes == i);
	//		time += end-start;
	//	}
	//	time = time/1000;
	//	printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
	//}
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
			printf("%lu ",time);
		}
		printf("\n");
		//time = time/1000;
		//printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
		//printf("Network Bytes: %d, ns: %.3f\n",i,time);
		
	}

}

void network_test_recv() {
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
}
