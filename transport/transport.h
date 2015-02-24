#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_
#include "global.h"
#include "nn.hpp"
#include <nanomsg/bus.h>
#include <nanomsg/pair.h>
#include "remote_query.h"
#include "query.h"


/*
	 // TODO: Add checksum to data, in case network is faulty?
	 Message format:
Header: 4 Byte receiver ID
				4 Byte sender ID
				4 Byte # of bytes in msg
Data:	MSG_SIZE - HDR_SIZE bytes
	 */

#define GET_RCV_NODE_ID(b)  ((uint32_t*)b)[0]
class Transport {
	public:
		Transport() : s(AF_SP, NN_BUS) {}
		void init(uint64_t node_id);
		uint64_t get_node_id();
		void send_msg(uint64_t dest_id, void ** data, int * sizes, int num); 
		uint64_t recv_msg(base_query * query);
		void simple_send_msg(int size); 
		uint64_t simple_recv_msg();
	private:
		nn::socket s;

		uint64_t _node_id;
		uint32_t msg_recv_id;
		uint32_t msg_send_id;
		uint32_t msg_size;

};
#endif
