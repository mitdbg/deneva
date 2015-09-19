#include "client_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/

void 
Client_query_queue::init(workload * h_wl) {
	_wl = h_wl;
#if CREATE_TXN_FILE
	all_queries = new Client_query_thd * [PART_CNT];
	for (UInt32 tid = 0; tid < PART_CNT; tid ++) {
		init(tid);
    }
#else
	all_queries = new Client_query_thd * [g_servers_per_client];
	for (UInt32 tid = 0; tid < g_servers_per_client; tid ++) {
		init(tid);
  }
#endif
}

void 
Client_query_queue::init(int thread_id) {	
	all_queries[thread_id] = (Client_query_thd *) 
		mem_allocator.alloc(sizeof(Client_query_thd), thread_id);
	all_queries[thread_id]->init(_wl, thread_id);

#if CREATE_TXN_FILE
  char output_file[50];
  sprintf(output_file,"p%d_%d_skew%d.txt",thread_id,g_part_cnt,(int)(g_zipf_theta*10));
  FILE * outf;
  outf = fopen(output_file,"w");
  //printf("%ld %ld %ld ",request_cnt,part_to_access[0],part_num);

  for(uint64_t j = 0; j < g_client_thread_cnt; j++) {
	ycsb_client_query * query = (ycsb_client_query*)all_queries[thread_id]->get_next_query(j);
  while(query) {
    for(uint64_t i = 0; i < query->request_cnt; i++) {
      fprintf(outf,"%d,%ld,",query->requests[i].acctype,query->requests[i].key);
    }
    fprintf(outf,"\n");
	  query = (ycsb_client_query*)all_queries[thread_id]->get_next_query(j);
    //printf("%d %d %ld | ",requests[i].acctype,((ycsb_wl*)h_wl)->key_to_part(requests[i].key),requests[i].key);
  }
  }
  fclose(outf);
#endif


}

bool
Client_query_queue::done() {
  bool done = true;
  for(uint32_t n = 0; n < g_servers_per_client; n++) {
    done = all_queries[n]->done();
    if(!done)
      break;
  }
  return done;
}

base_client_query * 
Client_query_queue::get_next_query(uint64_t nid, uint64_t tid) { 	
  uint64_t starttime = get_sys_clock();
	base_client_query * query = all_queries[nid]->get_next_query(tid);
  INC_STATS(tid,time_getqry,get_sys_clock() - starttime);
	return query;
}

void 
Client_query_thd::init(workload * h_wl, int thread_id) {
  this->thread_id = thread_id;
  this->h_wl = h_wl;
  next_tid = 0;
	q_idx = 0;
#if CLIENT_RUNTIME
#if WORKLOAD == YCSB	
	queries = (ycsb_client_query *) 
		mem_allocator.alloc(sizeof(ycsb_client_query) * g_client_thread_cnt, thread_id);
  for(uint64_t i=0;i<g_client_thread_cnt;i++)
	  new(&queries[i]) ycsb_client_query();
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) 
		mem_allocator.alloc(sizeof(tpcc_query) * g_client_thread_cnt, thread_id);
  for(uint64_t i=0;i<g_client_thread_cnt;i++)
    new(&queries[i]) tpcc_query();
#endif

#else
	uint64_t request_cnt;
	//request_cnt = WARMUP / g_client_thread_cnt + MAX_TXN_PER_PART + 4;
	request_cnt = MAX_TXN_PER_PART + 4;

#if WORKLOAD == YCSB	
	queries = (ycsb_client_query *) 
		mem_allocator.alloc(sizeof(ycsb_client_query) * request_cnt, thread_id);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) 
		mem_allocator.alloc(sizeof(tpcc_query) * request_cnt, thread_id);
#endif

#if LOAD_TXN_FILE

  char input_file[50];
	char * cpath = getenv("SCHEMA_PATH");
	if (cpath == NULL) 
    sprintf(input_file,"./input_txn_files/p%d_%d_skew%d.txt",thread_id+g_server_start_node,g_part_cnt,(int)(g_zipf_theta*10));
	else { 
    sprintf(input_file,"%sp%d_%d_skew%d.txt",cpath,thread_id+g_server_start_node,g_part_cnt,(int)(g_zipf_theta*10));
	}
  printf("%s\n",input_file);
  init_txns_file( input_file );
  return;
#endif

	pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		pthread_create(&p_thds[i], NULL, threadInitQuery, this);
  }
  threadInitQuery(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		//printf("thread %d complete\n", i);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}

#endif
}

void 
Client_query_thd::init_txns_file(const char * txn_file) {
	string line;
  uint64_t qid = 0;
  uint64_t request_cnt;
	request_cnt = MAX_TXN_PER_PART + 4;
	ifstream fin(txn_file);
  while (getline(fin, line) && qid < request_cnt) {

#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_client_query();
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
#endif
    queries[qid].client_init();
    int num = 0;
    int idx = 0;
    int part_cnt = 0;
		size_t pos = 0;
		string token;
    while(line.length() != 0) {
      pos = line.find(",");
		  if (pos == string::npos)
				pos = line.length();
	    token = line.substr(0, pos);
		 	line.erase(0, pos + 1);
      switch(num) {
        case 0: queries[qid].requests[idx].acctype = (access_t)atoi(token.c_str()); break;// Rd/Wr
        case 1: queries[qid].requests[idx].key = atoi(token.c_str());break;// key
      }
      if(num == 1) {
        uint64_t part = queries[qid].requests[idx].key % g_part_cnt;
        int i = 0;
        for(i = 0; i < part_cnt; i++) {
          if(part == queries[qid].part_to_access[i])
            break;
        }
        if(i == part_cnt)
          queries[qid].part_to_access[part_cnt++] = part;
        idx++;
      }
      else {
        if(g_read_perc == 1.0)
          queries[qid].requests[idx].acctype = RD;
        else if(g_write_perc == 1.0)
          queries[qid].requests[idx].acctype = WR;

      }
      num = (num + 1) % 2;

    }
    queries[qid].request_cnt = idx;
    queries[qid].part_num = part_cnt;
    queries[qid].pid = queries[qid].part_to_access[0];
    qid++;
  }

	fin.close();

}

bool
Client_query_thd::done(){
  return q_idx >= MAX_TXN_PER_PART;
}

base_client_query * 
Client_query_thd::get_next_query(uint64_t tid) {
	if (q_idx >= MAX_TXN_PER_PART)
		return NULL;
#if CLIENT_RUNTIME
  /*
#if WORKLOAD == YCSB	
		new(&queries[q_idx]) ycsb_query();
#elif WORKLOAD == TPCC
		new(&queries[q_idx]) tpcc_query();
#endif
*/
		queries[tid].init(thread_id, h_wl, thread_id);
		// Setup
		queries[tid].txn_id = UINT64_MAX;
		queries[tid].rtype = RTXN;
	base_client_query * query = &queries[tid];
#else
    int cur_q_idx = ATOM_FETCH_ADD(q_idx, 1);
	//base_query * query = &queries[q_idx++];
	base_client_query * query = &queries[cur_q_idx];

#endif

	return query;
}

void * Client_query_thd::threadInitQuery(void * This) {
	((Client_query_thd *)This)->init_query();
  return NULL;
}

void Client_query_thd::init_query() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
  uint64_t request_cnt;
	request_cnt = MAX_TXN_PER_PART + 4;
	
    uint32_t final_request;
    if (tid == g_init_parallelism-1) {
        final_request = MAX_TXN_PER_PART+4;
    } else {
        final_request = request_cnt / g_init_parallelism * (tid+1);
    }
	//for (UInt32 qid = request_cnt / g_init_parallelism * tid; qid < request_cnt / g_init_parallelism * (tid+1); qid ++) {
	for (UInt32 qid = request_cnt / g_init_parallelism * tid; qid < final_request; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_client_query();
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
#endif
		//queries[qid].init(thread_id, h_wl, qid % g_node_cnt);
		queries[qid].client_init(thread_id, h_wl, thread_id+g_server_start_node);
		// Setup
    /*
		queries[qid].txn_id = UINT64_MAX;
		queries[qid].rtype = RTXN;
    */
		//queries[qid].client_id = g_node_id;
	}
}
