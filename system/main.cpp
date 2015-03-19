#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "transport.h"

void * f(void *);
void * g(void *);
void network_test();
void network_test_recv();


// TODO the following global variables are HACK
thread_t * m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	// 0. initialize global data structure
	parser(argc, argv);

	uint64_t seed = get_sys_clock();
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


	// per-partition malloc
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager.init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");
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
	printf("workload initialized!\n");

	rem_qry_man.init(g_node_id,m_wl);
	tport_man.init(g_node_id);

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	uint64_t rthd_cnt = g_rem_thread_cnt;
	
	pthread_t * p_thds = 
		(pthread_t *) malloc(sizeof(pthread_t) * (thd_cnt + rthd_cnt));
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	cpu_set_t cpus;
	m_thds = new thread_t[thd_cnt + rthd_cnt];
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	if (WORKLOAD != TEST) {
		query_queue.init(m_wl);
	}
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init(g_node_id);
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt + rthd_cnt; i++) 
		m_thds[i].init(i, g_node_id, m_wl);

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
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt + g_rem_thread_cnt);
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt+ g_rem_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt + g_rem_thread_cnt);

	uint64_t cpu_cnt = 0;
	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt; i++) {
		uint64_t vid = i;
		CPU_ZERO(&cpus);
#if TPORT_TYPE_IPC
    CPU_SET(g_node_id * (g_thread_cnt) + cpu_cnt, &cpus);
    //CPU_SET(g_node_id * (g_thread_cnt + g_rem_thread_cnt) + cpu_cnt, &cpus);
#else
    CPU_SET(cpu_cnt, &cpus);
#endif
		cpu_cnt++;
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		pthread_create(&p_thds[i], &attr, f, (void *)vid);
	}


	for (uint32_t i = 0; i < rthd_cnt; i++) {
		CPU_ZERO(&cpus);
#if TPORT_TYPE_IPC
    //CPU_SET(g_node_id * (g_thread_cnt + g_rem_thread_cnt) + cpu_cnt, &cpus);
#else
    CPU_SET(cpu_cnt, &cpus);
  	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
#endif
		cpu_cnt++;
		pthread_create(&p_thds[thd_cnt+i], &attr, g, (void *)(thd_cnt + i));
	//g((void *)(thd_cnt));
	}


	for (uint32_t i = 0; i < thd_cnt + rthd_cnt; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();
	
	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run();
	return NULL;
}

void * g(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid].run_remote();
	return NULL;
}

void network_test() {

	ts_t start;
	ts_t end;
	double time;
	int bytes;
	for(int i=4; i < 257; i+=4) {
		time = 0;
		for(int j=0;j < 1000; j++) {
			start = get_sys_clock();
			tport_man.simple_send_msg(i);
			while((bytes = tport_man.simple_recv_msg()) == 0) {}
			end = get_sys_clock();
			assert(bytes == i);
			time += end-start;
		}
		time = time/1000;
		printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
	}
}

void network_test_recv() {
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
}
