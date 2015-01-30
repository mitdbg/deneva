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

// TODO the following global variables are HACK
thread_t * m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	// 0. initialize global data structure
	parser(argc, argv);
	// per-partition malloc
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager.init();

	/*
	// Purely for testing transport layer: TODO remove this
	tport_man.init(g_node_id);
	const char * arg1 = "Hello ";
	const char * arg2 = "World! ";
	uint64_t arg3 = 25;
	uint32_t arg4 = 101;
	const char * arg5 = "Oh say can you see by the dawn's early light what so proudly we hailed at the twilight's last gleaming? With broad stripes and bright stars through the perilous fight, o'er the ramparts we watched were so gallantly gleaming. And the rockets red glare, the bombs bursting in air, gave proof to the night that our flag was still there. Oh say, does that star spangled banner yet wave o'er the land of the free and the home of the brave?";
	const void * data[5];
	data[0] = arg1;
	data[1] = arg2;
	data[2] = &arg3;
	data[3] = &arg4;
	data[4] = arg5;
	int sizes[5]; 
	sizes[0] = sizeof(char) * 6;
	sizes[1] = sizeof(char) * 7;
	sizes[2] = sizeof(uint64_t);
	sizes[3] = sizeof(uint64_t);
	sizes[4] = sizeof(char) * 434;

	if(g_node_id == 0) {
		sleep(10);
		tport_man.send_msg(1,(void**)data,sizes,5);
	}
	//tport_man.send_msg(buf);

	tport_man.recv_msg();

	return 0;
	*/

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
	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	
	pthread_t * p_thds = 
		(pthread_t *) malloc(sizeof(pthread_t) * (thd_cnt - 1));
	m_thds = new thread_t[thd_cnt];
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	if (WORKLOAD != TEST)
		query_queue.init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i].init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));
	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
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
