#include "global.h"
#include "manager.h"
#include "transport.h"
#include "nn.hpp"
#include "mem_alloc.h"


void Transport::init(uint64_t node_id) {
	printf("Init %ld\n",node_id);
	_node_id = node_id;
//	s.socket(AF_SP, NN_BUS);
	// Format: transport://addr
	int rc;
	if(node_id == 0)
		//rc = s.bind("inproc://a");
		rc = s.bind("ipc://a.ipc");
	else
		//rc = s.connect("inproc://a");
		rc = s.connect("ipc://a.ipc");
	if(rc < 0) {
		printf("Bind Error: %d\n",errno);
	}
}

uint64_t Transport::get_node_id() {
	return _node_id;
}

void Transport::send_msg(void * buf) {
	// Send message packets
	int rc = s.send(buf,MSG_SIZE,0);
	//int rc = s.send("ABC",3,0);
	if (rc < 0) {
		// Error; resend?
		printf("send Error: %d\n",errno);
	}
}

void Transport::send_msg(uint64_t dest_id, void ** data, int * sizes, int num) {

	// 1: Scrape data pointers for actual data
	// Profile data for size
	uint64_t size = HEADER_SIZE;
	for(int i = 0; i < num; i++) {
		size += sizes[i];
	}
	if(size % MSG_SIZE != 0) {
		size = size + MSG_SIZE - (size % MSG_SIZE);
	}
	int packets = size / MSG_SIZE;

	char * sbuf = (char *) mem_allocator.alloc(sizeof(char) * size,get_node_id());

	// Copy all data into sbuf
	uint64_t dsize = HEADER_SIZE;
	for(int i = 0; i < num; i++) {
		memcpy(&sbuf[dsize],data[i],sizes[i]);
		dsize += sizes[i];
	}

	// 2: Create message header
	// TODO: Separate header for each packet?
	encode_header(&sbuf,dest_id,size,0);

	printf("Sending %ld -> %ld: %d pkts, %ld bytes\n",get_node_id(),dest_id,packets,size);
	// 3: Send message packets (may be multiple)
	// Send data in packets
	for(int p = 0; p < packets; p++) {
		send_msg(&sbuf[p*MSG_SIZE]);
	}

}

// Listens to socket for messages from other nodes
void Transport::recv_msg() {
	printf("recv_msg\n");
	int rc;
	int c = 0;
	char * buf = (char *) mem_allocator.alloc(sizeof(char) * MSG_SIZE,get_node_id());
	while(1) {
		rc = s.recv(buf,MSG_SIZE,0);
		if(rc < 0) {
			printf("Recv Error %d\n",errno);
			continue;
		}
		printf("RECV: %s\n",buf);
		c++;
		if(c >= 4)
			return;
		// Discard any messages not intended for this node
		/*
		decode_msg();
		if(msg_recv_id != get_node_id())
			continue;
		if(msg_size > MSG_SIZE) {
			printf("Message Size: %d\n",msg_size);
		}
		// Queue request for thread to execute
		add_query(msg_send_id,&buf[HEADER_SIZE]);
		*/
	}

}

/*
void Transport::decode_msg_hdr() {
	msg_recv_id = ((uint32_t * ) buf)[0];
	msg_send_id = ((uint32_t * ) buf)[1];
	msg_size = ((uint32_t * ) buf)[2];
}
*/

void Transport::encode_header(char ** sbuf, uint64_t dest_id, uint64_t size, uint64_t n) {
	((uint32_t*) *sbuf) [0] = dest_id;
	((uint32_t*) *sbuf) [1] = get_node_id();
	((uint32_t*) *sbuf) [2] = size;
	((uint32_t*) *sbuf) [2] = n;
	//memcpy(&sbuf[HEADER_SIZE],data,size);

}
