#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "client_thread.h"
#include "mem_alloc.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
//#include <jemallloc.h>

void * f(void *);
void * g(void *);
void * worker(void *);
void * nn_worker(void *);
void network_test();
void network_test_recv();


// TODO the following global variables are HACK
Client_thread_t * m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
    printf("Running client...\n\n");
	// 0. initialize global data structure
	parser(argc, argv);
    assert(g_node_id >= g_node_cnt);
    assert(g_client_node_cnt <= g_node_cnt);

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
	m_wl->workload::init();
	printf("workload initialized!\n");

  printf("Initializing remote query manager... ");
  fflush(stdout);
	rem_qry_man.init(g_node_id,m_wl);
  printf("Done\n");
  printf("Initializing transport manager... ");
  fflush(stdout);
	tport_man.init(g_node_id);
  printf("Done\n");
  printf("Initializing client manager... ");
  client_man.init();
  printf("Done\n");
  fflush(stdout);

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_client_thread_cnt;
	uint64_t rthd_cnt = g_client_rem_thread_cnt;

	pthread_t * p_thds = 
		(pthread_t *) malloc(sizeof(pthread_t) * (thd_cnt + rthd_cnt));
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	cpu_set_t cpus;
	m_thds = new Client_thread_t[thd_cnt + rthd_cnt];
	//// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
  printf("Initializing client query queue... ");
  fflush(stdout);
	if (WORKLOAD != TEST) {
		client_query_queue.init(m_wl);
	}
  printf("Done\n");

	pthread_barrier_init( &warmup_bar, NULL, thd_cnt );
  printf("Initializing threads... ");
  fflush(stdout);
	for (uint32_t i = 0; i < thd_cnt + rthd_cnt; i++) 
		m_thds[i].init(i, g_node_id, m_wl);
  printf("Done\n");
#if CREATE_TXN_FILE
  return(0);
#endif

  endtime = get_server_clock();
  printf("Initialization Time = %ld\n", endtime - starttime);
  fflush(stdout);
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, thd_cnt + rthd_cnt);
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, thd_cnt+ rthd_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, thd_cnt + rthd_cnt);

	uint64_t cpu_cnt = 0;
	// spawn and run txns again.
	starttime = get_server_clock();

  uint64_t i = 0;
	for (i = 0; i < thd_cnt; i++) {
		uint64_t vid = i;
		CPU_ZERO(&cpus);
#if TPORT_TYPE_IPC
        CPU_SET(g_node_id * thd_cnt + cpu_cnt, &cpus);
#else
        CPU_SET(cpu_cnt, &cpus);
#endif
		cpu_cnt++;
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		pthread_create(&p_thds[i], &attr, worker, (void *)vid);
    }

	for (; i < thd_cnt + rthd_cnt -1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], &attr, nn_worker, (void *)vid);
  }

    nn_worker((void *)(i));

	for (i = 0; i < thd_cnt + rthd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);

	endtime = get_server_clock();
	
	if (WORKLOAD != TEST) {
		printf("CLIENT PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print_client(false);
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

void * worker(void * id) {
	uint64_t tid = (uint64_t)id;
    printf("Starting client worker: %lu\n", tid);
    fflush(stdout);
	m_thds[tid].run();
	return NULL;
}

void * nn_worker(void * id) {
	uint64_t tid = (uint64_t)id;
    printf("Starting client nn_worker: %lu\n", tid);
    fflush(stdout);
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
        fflush(stdout);
	}
}

void network_test_recv() {
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
}
