#include "global.h"
#include "manager.h"
#include "transport.h"
#include "nn.hpp"


void transport_man::init(uint64_t node_id) {
	_node_id = node_id;
	s.socket(AF_SP, NN_BUS);
}

uint64_t transport_man::get_node_id() {
	return _node_id;
}

void transport_man::send_msg(uint64_t dest_id, const char * data, size_t len) {
	uint64_t size = len;
	if(len > MSG_SIZE) {
		printf("Send msg size: %d\n",len);
	}
	char sbuf[MSG_SIZE];

	while ( size > 0) {
		size -= encode_msg(&sbuf,dest_id,size,data);
		int rc = s.send((void*) sbuf,MSG_SIZE,0)
		if (rc < 0) {
			// Error; resend?
			printf("Error: %d\n",rc);
		}
	}

}

// Listens to socket for messages from other nodes
void transport_man::recv_msg() {
	int rc;
	while(1) {
		rc = s.recv(buf,sizeof(buf),0);
		if(rc < 0) {
			printf("Error %d\n",errno);
			continue;
		}
		// Discard any messages not intended for this node
		decode_msg();
		if(msg_recv_id != get_node_id())
			continue;
		if(msg_size > MSG_SIZE) {
			printf("Message Size: %d\n",msg_size);
		}
		// Queue request for thread to execute
		add_query(msg_send_id,&buf[HEADER_SIZE]);
	}

}

void transport_man::decode_msg_hdr() {
	msg_recv_id = ((uint32_t * ) buf)[0];
	msg_send_id = ((uint32_t * ) buf)[1];
	msg_size = ((uint32_t * ) buf)[2];
}

void transport_man::encode_msg(char * sbuf, uint64_t dest_id, uint64_t size, const char * data) {
	((uint32_t*) sbuf) [0] = dest_id;
	((uint32_t*) sbuf) [1] = get_node_id();
	((uint32_t*) sbuf) [2] = size;
	memcpy(&sbuf[HEADER_SIZE],data,size);
	return size;

}
