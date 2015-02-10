#include "global.h"
#include "manager.h"
#include "transport.h"
#include "nn.hpp"
#include "mem_alloc.h"
#include "remote_query.h"

/*
	 HEADER FORMAT:
	 32b destination ID
	 32b source ID
	 32b message type
	 32b transaction ID
	 N?	data

	 */
void Transport::init(uint64_t node_id) {
	printf("Init %ld\n",node_id);
	_node_id = node_id;
//	s.socket(AF_SP, NN_BUS);
	// Format: transport://addr
	int rc;
	char socket_name[MAX_TPORT_NAME];
	strcpy(socket_name,TPORT_TYPE);
	strcat(socket_name,"://");
	strcat(socket_name,TPORT_PORT);
	if(node_id == 0)
		rc = s.bind(socket_name);
	else
		rc = s.connect(socket_name);
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
	encode_header(&sbuf,dest_id);

	printf("Sending %ld -> %ld: %d pkts, %ld bytes\n",get_node_id(),dest_id,packets,size);
	// 3: Send message packets (may be multiple)
	// TOOD: Send data in packets
	/*
	for(int p = 0; p < packets; p++) {
		send_msg(&sbuf[p*MSG_SIZE]);
	}
	*/
	send_msg(sbuf);

}

// Listens to socket for messages from other nodes
uint64_t Transport::recv_msg(r_query * query) {
	printf("recv_msg\n");
	int bytes;
	//char * buf = (char *) mem_allocator.alloc(sizeof(char) * MSG_SIZE,get_node_id());
	char * buf = NULL;
	bytes = s.recv(buf,MSG_SIZE, 0 | (int)NN_MSG);
	if(bytes < 0) {
		printf("Recv Error %d\n",errno);
	}
	// Discard any messages not intended for this node
	if(bytes <= 0 || GET_RCV_NODE_ID(buf) != get_node_id()) {
		nn::freemsg((void*)buf);	
		return 0;
	}
	// Queue request for thread to execute
	// Unpack request
	rem_qry_man.unpack(query,buf);
	nn::freemsg((void*)buf);	
	return 1;

}

/*
void Transport::decode_msg_hdr() {
	msg_recv_id = ((uint32_t * ) buf)[0];
	msg_send_id = ((uint32_t * ) buf)[1];
	msg_size = ((uint32_t * ) buf)[2];
}
*/

void Transport::encode_header(char ** sbuf, uint64_t dest_id) {
	((uint32_t*) *sbuf) [0] = dest_id;
	((uint32_t*) *sbuf) [1] = get_node_id();
	//((uint32_t*) *sbuf) [2] = size;
	//((uint32_t*) *sbuf) [2] = n;
	//memcpy(&sbuf[HEADER_SIZE],data,size);

}
