#include "global.h"
#include "manager.h"
#include "transport.h"
#include "nn.hpp"
#include "mem_alloc.h"
#include "remote_query.h"
#include "tpcc_query.h"
#include "query.h"

/*
	 HEADER FORMAT:
	 32b destination ID
	 32b source ID
	 32b ??? [unused] ??? transaction ID
	 ===> 32b message type <=== First 4 bytes of data
	 N?	data

	 */
Transport::Transport() {
	s = new Socket[g_node_cnt];
}


Transport::~Transport() {
	/*
	for(uint64_t i=0;i<g_node_cnt;i++) {
		delete &s[i];
	}
	*/
}

void Transport::read_ifconfig(const char * ifaddr_file) {
	uint64_t cnt = 0;
	ifstream fin(ifaddr_file);
	string line;
  while (getline(fin, line)) {
		memcpy(ifaddr[cnt],&line[0],12);
		cnt++;
	}
	for(uint64_t i=0;i<g_node_cnt;i++) {
		printf("%ld: %s\n",i,ifaddr[i]);
	}
}

void Transport::init(uint64_t node_id) {
	printf("Init %ld\n",node_id);
	_node_id = node_id;

#if !TPORT_TYPE_IPC
	// Read ifconfig file
	ifaddr = new char *[g_node_cnt];
	for(uint64_t i=0;i<g_node_cnt;i++) {
		ifaddr[i] = new char[12];
	}
	char * cpath = getenv("SCHEMA_PATH");
	string path;
	if(cpath == NULL)
		path = "./";
	else
		path = string(cpath);
	path += "ifconfig.txt";
	cout << "reading ifconfig file: " << path << endl;
	read_ifconfig(path.c_str());
#endif

	int rc;

	int timeo = 1000; // timeout in ms
	for(uint64_t i=0;i<g_node_cnt;i++) {
		s[i].sock.setsockopt(NN_SOL_SOCKET,NN_RCVTIMEO,&timeo,sizeof(timeo));
	}

	printf("Node ID: %d/%d\n",g_node_id,g_node_cnt);

	for(uint64_t i=0;i<g_node_cnt-1;i++) {
		for(uint64_t j=i+1;j<g_node_cnt;j++) {
			if(i != g_node_id && j != g_node_id) {
				continue;
			}
			char socket_name[MAX_TPORT_NAME];
			// Socket name format: transport://addr
#if TPORT_TYPE_IPC
			sprintf(socket_name,"%s://node_%ld_%ld_%s",TPORT_TYPE,i,j,TPORT_PORT);
#else
			int port = TPORT_PORT + (i * g_node_cnt) + j;
			if(i == g_node_id)
				sprintf(socket_name,"%s://eth0:%d",TPORT_TYPE,port);
			else
				sprintf(socket_name,"%s://eth0;%s:%d",TPORT_TYPE,ifaddr[i],port);
#endif
			if(i == g_node_id) {
				printf("Binding to %s\n",socket_name);
				rc = s[j].sock.bind(socket_name);
			}
			else {
				printf("Connecting to %s\n",socket_name);
				rc = s[i].sock.connect(socket_name);
			}
		}

	}
	fflush(stdout);
	/*
#if !TPORT_TYPE_IPC
	for(uint64_t i=0;i<g_node_cnt;i++) {
			int port = TPORT_PORT + (i * g_node_cnt) + j;
			if(i == g_node_id) {
				sprintf(socket_name,"%s://eth0:%d",TPORT_TYPE,port);
				printf("Binding to %s",socket_name);
				rc = s[i].sock.bind(socket_name);
			}
			else {
				sprintf(socket_name,"%s://eth0;%s:%d",TPORT_TYPE,ifaddr[j],port);
				printf("Connecting to %s",socket_name);
				rc = s[i].sock.connect(socket_name);
			}
	}

#endif
*/


	if(rc < 0) {
		printf("Bind Error: %d %s\n",errno,strerror(errno));
	}
}

uint64_t Transport::get_node_id() {
	return _node_id;
}

void Transport::send_msg(uint64_t dest_id, void ** data, int * sizes, int num) {

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

#if DEBUG_DISTR
	printf("Sending %ld -> %ld: %ld bytes\n",get_node_id(),dest_id,size);
#endif

	// 3: Add time of message sending for stats purposes
	ts_t time = get_sys_clock();
	memcpy(&((char*)sbuf)[dsize],&time,sizeof(ts_t));
	dsize += sizeof(ts_t);

	assert(size == dsize);

	int rc;
	
	// 4: send message
	rc= s[dest_id].sock.send(&sbuf,NN_MSG,0);

	// Check for a send error
	if(rc < 0) {
		printf("send Error: %d %s\n",errno,strerror(errno));
		assert(false);
	}

	INC_STATS(1,msg_sent_cnt,1);
	INC_STATS(1,msg_bytes,size);

}

// Listens to socket for messages from other nodes
base_query * Transport::recv_msg() {
	int bytes = 0;
	void * buf;
  base_query * query = NULL;
	
	// FIXME: Currently unfair round robin; prioritizes nodes with low node_id
	for(uint64_t i=0;i<g_node_cnt;i++) {
		bytes = s[i].sock.recv(&buf, NN_MSG, NN_DONTWAIT);

		if(bytes <= 0 && errno != 11) {
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
		return 0;
	}

	// Queue request for thread to execute
	// Unpack request


	// Calculate time of message delay
	ts_t time;
	memcpy(&time,&((char*)buf)[bytes-sizeof(ts_t)],sizeof(ts_t));

  // Insert artificial network delay
#if NETWORK_DELAY > 0 && TPORT_TYPE_IPC
  ts_t starttime = time;
  //ts_t starttime = get_sys_clock();
  while( (get_sys_clock() - starttime) < NETWORK_DELAY) {}
#endif
	ts_t time2 = get_sys_clock();
	INC_STATS(1,tport_lat,time2 - time);

	ts_t start = get_sys_clock();
	INC_STATS(1,msg_rcv_cnt,1);
  
	query = rem_qry_man.unpack(buf,bytes);
#if DEBUG_DISTR
	printf("Msg delay: %d->%d, %d bytes, %f s\n",query->return_id,query->dest_id,bytes,((float)(time2-time))/BILLION);
#endif
	nn::freemsg(buf);	
  assert(query->dest_id == get_node_id());

	INC_STATS(1,rtime_unpack,get_sys_clock()-start);
	return query;

}

void Transport::simple_send_msg(int size) {
	void * sbuf = nn_allocmsg(size,0);

	ts_t time = get_sys_clock();
	memcpy(&((char*)sbuf)[0],&time,sizeof(ts_t));

	int rc = s[(g_node_id + 1) % g_node_cnt].sock.send(&sbuf,NN_MSG,0);
	if(rc < 0) {
		printf("send Error: %d %s\n",errno,strerror(errno));
		assert(false);
	}
}

uint64_t Transport::simple_recv_msg() {
	int bytes;
	void * buf;
	bytes = s[(g_node_id + 1) % g_node_cnt].sock.recv(&buf, NN_MSG, NN_DONTWAIT);
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


