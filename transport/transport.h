#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_
#include "global.h"
#include "nn.hpp"
#include <nanomsg/bus.h>
#include <nanomsg/pair.h>

#define MSG_SIZE 128 // in bytes
#define HEADER_SIZE sizeof(uint32_t)*3 // in bytes

/*
	 // TODO: Add checksum to data, in case network is faulty?
	 Message format:
Header: 4 Byte receiver ID
				4 Byte sender ID
				4 Byte # of bytes in msg
Data:	MSG_SIZE - HDR_SIZE bytes
	 */

class Transport {
	public:
		Transport() : s(AF_SP, NN_BUS) {}
		void init(uint64_t node_id);
		uint64_t get_node_id();
		void send_msg(void * buf);
		void send_msg(uint64_t dest_id, void ** data, int * sizes, int num); 
		void recv_msg();
		void decode_msg_hdr();
		void encode_header(char ** sbuf, uint64_t dest_id, uint64_t size, uint64_t n);
	private:
		nn::socket s;

		uint64_t _node_id;
		uint32_t msg_recv_id;
		uint32_t msg_send_id;
		uint32_t msg_size;

};
#endif
