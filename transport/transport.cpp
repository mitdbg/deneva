#include "global.h"
#include "manager.h"
#include "transport.h"
#include "nn.hpp"
#include "mem_alloc.h"
#include "remote_query.h"
#include "tpcc_query.h"
#include "query.h"

#define MAX_IFADDR_LEN 20

/*
	 HEADER FORMAT:
	 32b destination ID
	 32b source ID
	 32b ??? [unused] ??? transaction ID
	 ===> 32b message type <=== First 4 bytes of data
	 N?	data

	 */
Transport::Transport() {
    _node_cnt = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
	_node_cnt++;	// account for the sequencer
#endif
  uint64_t s_thd;
  if(g_client_send_thread_cnt > g_send_thread_cnt)
    s_thd = g_client_send_thread_cnt;
  else
    s_thd = g_send_thread_cnt;
  _sock_cnt = s_thd * (_node_cnt) + g_node_cnt * g_send_thread_cnt + g_client_node_cnt * g_client_send_thread_cnt;
	//s = new Socket[_node_cnt];
	s = new Socket[_sock_cnt];

    delay_queue = new DelayQueue();
    delay_queue->init();
}


Transport::~Transport() {
	/*
	for(uint64_t i=0;i<_node_cnt;i++) {
		delete &s[i];
	}
	*/
}

void Transport::read_ifconfig(const char * ifaddr_file) {
	uint64_t cnt = 0;
	ifstream fin(ifaddr_file);
	string line;
  while (getline(fin, line)) {
		//memcpy(ifaddr[cnt],&line[0],12);
    strcpy(ifaddr[cnt],&line[0]);
		cnt++;
	}
	for(uint64_t i=0;i<_node_cnt;i++) {
		printf("%ld: %s\n",i,ifaddr[i]);
	}
}

void Transport::init(uint64_t node_id) {
	printf("Init %ld\n",node_id);
	_node_id = node_id;
  if(ISCLIENT)
    _sock_cnt = g_client_send_thread_cnt * (_node_cnt-1) + g_node_cnt * g_send_thread_cnt + (g_client_node_cnt-1) * g_client_send_thread_cnt;
  else
    _sock_cnt = g_send_thread_cnt  * (_node_cnt-1) + (g_node_cnt-1) * g_send_thread_cnt + g_client_node_cnt * g_client_send_thread_cnt;
  rr = 0;
#if CC_ALG == CALVIN
	// TODO: fix me. Calvin does not have separate remote and local threads
	// so the stat updates seg fault if the sequencer thread count is 1
	_thd_id = 0;
#else
	_thd_id = 1;
#endif

#if !TPORT_TYPE_IPC
	// Read ifconfig file
	ifaddr = new char *[_node_cnt];
	for(uint64_t i=0;i<_node_cnt;i++) {
		ifaddr[i] = new char[MAX_IFADDR_LEN];
	}
	string path;
#if SHMEM_ENV
  path = "/dev/shm/";
#else
	char * cpath
  cpath = getenv("SCHEMA_PATH");
	if(cpath == NULL)
		path = "./";
	else
		path = string(cpath);
#endif
	path += "ifconfig.txt";
	cout << "reading ifconfig file: " << path << endl;
	read_ifconfig(path.c_str());
#endif

	int rc = 0;

	int timeo = 1000; // timeout in ms
	//int stimeo = 60000; // timeout in ms
  int opt = 0;
	for(uint64_t i=0;i<_sock_cnt;i++) {
		s[i].sock.setsockopt(NN_SOL_SOCKET,NN_RCVTIMEO,&timeo,sizeof(timeo));
		//s[i].sock.setsockopt(NN_SOL_SOCKET,NN_SNDTIMEO,&stimeo,sizeof(stimeo));
    // NN_TCP_NODELAY doesn't cause TCP_NODELAY to be set -- nanomsg issue #118
		s[i].sock.setsockopt(NN_SOL_SOCKET,NN_TCP_NODELAY,&opt,sizeof(opt));
	}

	printf("Node ID: %d/%lu\n",g_node_id,_node_cnt);

  /*
	for(uint64_t i=0;i<_node_cnt-1;i++) {
		for(uint64_t j=i+1;j<_node_cnt;j++) {
			if(i != g_node_id && j != g_node_id) {
				continue;
			}
			char socket_name[MAX_TPORT_NAME];
			// Socket name format: transport://addr
#if TPORT_TYPE_IPC
			sprintf(socket_name,"%s://node_%ld_%ld_%s",TPORT_TYPE,i,j,TPORT_PORT);
#else
			int port = TPORT_PORT + (i * _node_cnt) + j;
			if(i == g_node_id)
				sprintf(socket_name,"%s://eth0:%d",TPORT_TYPE,port);
			else
				sprintf(socket_name,"%s://eth0;%s:%d",TPORT_TYPE,ifaddr[i],port);
#endif
			if(i == g_node_id) {
				printf("Binding to %s\n",socket_name);
				rc = s[j].sock.bind(socket_name);
			} else {
				printf("Connecting to %s\n",socket_name);
				rc = s[i].sock.connect(socket_name);
			}
      s_cnt++;
		}
	}
	fflush(stdout);
  */

  uint64_t s_cnt = 0;
  uint64_t s_thd;
  if(ISCLIENT)
    s_thd = g_client_send_thread_cnt;
  else
    s_thd = g_send_thread_cnt;

  // Ports for sending
  for(uint64_t i = 0; i < s_thd; i++) {
    for(uint64_t j = 0; j < _node_cnt; j++) {
      if(j == g_node_id)
        continue;
			char socket_name[MAX_TPORT_NAME];
			int port = g_node_id + j * _node_cnt + i * _node_cnt * _node_cnt;
#if TPORT_TYPE_IPC
			sprintf(socket_name,"%s://node_%d%s",TPORT_TYPE,port,TPORT_PORT);
#else
      port+= TPORT_PORT;
      sprintf(socket_name,"%s://eth0:%d",TPORT_TYPE,port);
#endif
      printf("Sock[%ld] Binding to %s %d -> %ld\n",s_cnt,socket_name,g_node_id,j);
      rc = s[s_cnt].sock.bind(socket_name);
      s_cnt++;
    }
  }

  // Ports for receiving
  for(uint64_t i = 0; i < _node_cnt; i++) {
    if(i == g_node_id)
      continue;
    if(ISCLIENTN(i))
      s_thd = g_client_send_thread_cnt;
    else
      s_thd = g_send_thread_cnt;
    for(uint64_t j = 0; j < s_thd; j++) {
			char socket_name[MAX_TPORT_NAME];
			int port = i + (g_node_id * _node_cnt) + j * _node_cnt * _node_cnt;
#if TPORT_TYPE_IPC
			sprintf(socket_name,"%s://node_%d%s",TPORT_TYPE,port,TPORT_PORT);
#else
      port+= TPORT_PORT;
			sprintf(socket_name,"%s://eth0;%s:%d",TPORT_TYPE,ifaddr[i],port);
#endif
      printf("Sock[%ld] Connecting to %s %ld -> %d\n",s_cnt,socket_name,i,g_node_id);
      rc = s[s_cnt].sock.connect(socket_name);
      s_cnt++;

    }
  }
	fflush(stdout);

	if(rc < 0) {
		printf("Bind Error: %d %s\n",errno,strerror(errno));
	}
}

uint64_t Transport::get_node_id() {
	return _node_id;
}

void Transport::send_msg(uint64_t sid, uint64_t dest_id, void * sbuf,int size) {
  uint64_t starttime = get_sys_clock();
  uint64_t id;
  if(ISCLIENT) {
    id = sid - g_client_thread_cnt;
  }
  else {
    id = sid - g_thread_cnt;
  }
  uint64_t idx = id * (_node_cnt-1) + dest_id;
  if(g_node_id < dest_id)
    idx--;
  /*
  printf("d:%ld idx:%ld -- %ld * %ld + %ld -- %ld\n",dest_id,idx,id,_node_cnt-1,dest_id,sid);
  fflush(stdout);
  */

	void * buf = nn_allocmsg(size,0);
	memcpy(buf,sbuf,size);
  int rc = -1;

  rc= s[idx].sock.send(&buf,NN_MSG,0);
  //assert(rc == size);
	//int rc= s[idx].sock.send(&sbuf,NN_MSG,0);
  //nn_freemsg(sbuf);

	// Check for a send error
	if(rc < 0 || rc != size) {
		printf("send Error: %d %s\n",errno,strerror(errno));
		//assert(false);
	}

  INC_STATS(_thd_id,time_tport_send,get_sys_clock() - starttime);
	INC_STATS(_thd_id,msg_sent_cnt,1);
	INC_STATS(_thd_id,msg_bytes,size);
}

void Transport::send_msg(uint64_t dest_id, void ** data, int * sizes, int num) {
    uint64_t starttime = get_sys_clock();
#if CC_ALG == CALVIN
	//RemReqType * rtype = (RemReqType *) data[rtype_offset];
	RemReqType rtype = *((RemReqType *)data[1]);
	//memcpy(&rtype, data[rtype_offset],sizeof(RemReqType));
	if (rtype != INIT_DONE) {
		// Sequencer is last node id
		uint64_t seq_node_id = _node_cnt - 1;
		if (g_node_id != seq_node_id)
			dest_id = seq_node_id;
	} else {
		printf("Node %lu sending INIT_DONE to %lu\n",get_node_id(),dest_id);
	}
#endif

	// 1: Scrape data pointers for actual data
	// Profile data for size
	uint64_t size = HEADER_SIZE;
	for(int i = 0; i < num; i++) {
		size += sizes[i];
	}
	// For step 3
	size += sizeof(ts_t);

	void * sbuf = nn_allocmsg(size,0);
	memset(sbuf,0,size);

	// Copy all data into sbuf
	uint64_t dsize = HEADER_SIZE;
	for(int i = 0; i < num; i++) {
		memcpy(&((char*)sbuf)[dsize],data[i],sizes[i]);
		dsize += sizes[i];
	}


	// 2: Create message header
  // dest_id
	((uint32_t*)sbuf)[0] = dest_id;
  // return_id
	((uint32_t*)sbuf)[1] = get_node_id();

	//DEBUG("Sending %ld -> %ld: %ld bytes\n",get_node_id(),dest_id,size);

	// 3: Add time of message sending for stats purposes
	ts_t time = get_sys_clock();
	memcpy(&((char*)sbuf)[dsize],&time,sizeof(ts_t));
	dsize += sizeof(ts_t);

	assert(size == dsize);

	int rc;
	
	// 4: send message
  if(*(RemReqType*)data[1] == EXP_DONE) {
	  rc = s[dest_id].sock.send(&sbuf,NN_MSG,NN_DONTWAIT);
    return;
  }

#if NETWORK_DELAY > 0
    RemReqType rem_req_type = *(RemReqType*)data[1];
    if (rem_req_type != INIT_DONE) { // && rem_req_type != RTXN) {
       DelayMessage * msg = new DelayMessage(dest_id, sbuf,size);
       delay_queue->add_entry(msg);
       INC_STATS(_thd_id,time_tport_send,get_sys_clock() - starttime);
       return;
    }
#endif

	rc= s[dest_id].sock.send(&sbuf,NN_MSG,0);

	// Check for a send error
	if(rc < 0) {
		printf("send Error: %d %s\n",errno,strerror(errno));
		assert(false);
	}

  INC_STATS(_thd_id,time_tport_send,get_sys_clock() - starttime);
	INC_STATS(_thd_id,msg_sent_cnt,1);
	INC_STATS(_thd_id,msg_bytes,size);
}

void Transport::check_delayed_messages() {
    assert(NETWORK_DELAY > 0);
    DelayMessage * dmsg = NULL;
    while ((dmsg = (DelayMessage *) delay_queue->get_next_entry()) != NULL) {
        DEBUG("In check_delayed_messages : sending message on the delay queue\n");
        send_msg_no_delay(dmsg);
    }
}

void Transport::send_msg_no_delay(DelayMessage * msg) {
    assert(NETWORK_DELAY > 0);

    uint64_t starttime = get_sys_clock();
    // dest_id
	uint64_t dest_id = ((uint32_t*)msg->_sbuf)[0];
    // return_id
	uint64_t return_id = ((uint32_t*)msg->_sbuf)[1];
    DEBUG("In send_msg_no_delay: dest_id = %lu, return_id = %lu\n",dest_id, return_id);
    assert (dest_id == msg->_dest_id);
    assert (return_id == get_node_id());

    int rc;
	rc= s[msg->_dest_id].sock.send(&msg->_sbuf,NN_MSG,0);

	// Check for a send error
	if(rc < 0) {
		printf("send Error: %d %s\n",errno,strerror(errno));
		assert(false);
	}

	INC_STATS(_thd_id,msg_sent_cnt,1);
  INC_STATS(_thd_id,time_tport_send,get_sys_clock() - starttime);
	INC_STATS(_thd_id,msg_bytes,msg->_size);
}

// Listens to socket for messages from other nodes
bool Transport::recv_msg() {
	int bytes = 0;
	void * buf;
  uint64_t starttime = get_sys_clock();
	
	for(uint64_t i=0;i<_sock_cnt;i++) {
		bytes = s[rr++ % _sock_cnt].sock.recv(&buf, NN_MSG, NN_DONTWAIT);

    if(rr == UINT64_MAX)
      rr = 0;

		if(bytes <= 0 && errno != 11) {
		  printf("Recv Error %d %s\n",errno,strerror(errno));
			nn::freemsg(buf);	
		}

		if(bytes>0)
			break;
	}

	if(bytes < 0 && errno != 11) {
		printf("Recv Error %d %s\n",errno,strerror(errno));
	}
	// Discard any messages not intended for this node
	if(bytes <= 0 ) {
    return false;
	}

	// Calculate time of message delay
  /*
	ts_t time;
	memcpy(&time,&((char*)buf)[bytes-sizeof(ts_t)],sizeof(ts_t));
  */

	ts_t time2 = get_sys_clock();
	//INC_STATS(_thd_id,tport_lat,time2 - time);

  INC_STATS(_thd_id,time_tport_rcv, time2 - starttime);

	starttime = get_sys_clock();
	INC_STATS(_thd_id,msg_rcv_cnt,1);

  /*
	uint32_t return_id __attribute__ ((unused)); // for debug only
	uint32_t dest_id;
#if CC_ALG == CALVIN
	if (get_node_id() == _node_cnt - 1) {
		query = (void *) rem_qry_man.unpack_client_query(buf,bytes);
		return_id = ((base_client_query *)query)->return_id;
		memcpy(&dest_id,buf,sizeof(uint32_t));
	} else {
		query = (void *) rem_qry_man.unpack(buf,bytes);
		return_id = ((base_query *)query)->return_id;
		dest_id = ((base_query *)query)->dest_id;
	}
#else
	query = (void *) rem_qry_man.unpack(buf,bytes);
	return_id = ((base_query *)query)->return_id;
	dest_id = ((base_query *)query)->dest_id;
#endif
*/
  if(CC_ALG == CALVIN && ISSEQUENCER) {
    rem_qry_man.unpack_client_query(buf,bytes);
  } else {
    rem_qry_man.unpack(buf,bytes);
  }

	//DEBUG("Msg delay: %d->%d, %d bytes, %f s\n",return_id,
  //          dest_id,bytes,((float)(time2-time))/BILLION);
	nn::freemsg(buf);	
  //assert(dest_id == get_node_id());

	INC_STATS(_thd_id,time_unpack,get_sys_clock()-starttime);
  return true;
}

void Transport::simple_send_msg(int size) {
	void * sbuf = nn_allocmsg(size,0);

	ts_t time = get_sys_clock();
	memcpy(&((char*)sbuf)[0],&time,sizeof(ts_t));

	int rc = s[(g_node_id + 1) % _node_cnt].sock.send(&sbuf,NN_MSG,0);
	if(rc < 0) {
		printf("send Error: %d %s\n",errno,strerror(errno));
		assert(false);
	}
}

uint64_t Transport::simple_recv_msg() {
	int bytes;
	void * buf;
	bytes = s[(g_node_id + 1) % _node_cnt].sock.recv(&buf, NN_MSG, NN_DONTWAIT);
	if(bytes <= 0 ) {
		if(errno != 11)
			nn::freemsg(buf);	
		return 0;
	}

	ts_t time;
	memcpy(&time,&((char*)buf)[0],sizeof(ts_t));
	//printf("%d bytes, %f s\n",bytes,((float)(get_sys_clock()-time)) / BILLION);

	nn::freemsg(buf);	
	return bytes;
}

void * DelayQueue::get_next_entry() {
  assert (NETWORK_DELAY > 0);
  q_entry_t next_entry = NULL;
  DelayMessage * data = NULL;

  pthread_mutex_lock(&mtx);

  assert( ( (cnt == 0) && head == NULL && tail == NULL) || ( (cnt > 0) && head != NULL && tail !=NULL) );

  if(cnt > 0) {
    next_entry = head;
	data = (DelayMessage *) next_entry->entry;
	assert(data != NULL);
    uint64_t nowtime = get_sys_clock();

    DEBUG("DelayQueue: current delay time for head query: %lu\n",nowtime - data->_start_ts);
    // Check whether delay ns have passed
    if (nowtime - data->_start_ts < NETWORK_DELAY) {
        pthread_mutex_unlock(&mtx);
        return NULL;
    }
    head = head->next;
	free(next_entry);
    cnt--;

    if(cnt == 0) {
      tail = NULL;
    }
  }

  if(cnt == 0 && last_add_time != 0) {
    //INC_STATS(0,qq_full,nowtime - last_add_time);
    last_add_time = 0;
  }

  pthread_mutex_unlock(&mtx);
  return data;
}

