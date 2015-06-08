#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_
#include "global.h"
#include "nn.hpp"
#include <nanomsg/bus.h>
#include <nanomsg/pair.h>
#include "remote_query.h"
#include "query.h"


/*
	 Message format:
Header: 4 Byte receiver ID
				4 Byte sender ID
				4 Byte # of bytes in msg
Data:	MSG_SIZE - HDR_SIZE bytes
	 */

#define GET_RCV_NODE_ID(b)  ((uint32_t*)b)[0]

class Socket {
	public:
		Socket () : sock(AF_SP,NN_PAIR) {}
		~Socket () { delete &sock;}
        char _pad1[CL_SIZE];
		nn::socket sock;
        char _pad[CL_SIZE - sizeof(nn::socket)];
};

class Transport {
	public:
		Transport();
		~Transport();
		//Transport() : s(AF_SP,NN_PAIR) {}
		void read_ifconfig(const char * ifaddr_file);
		void init(uint64_t node_id);
		uint64_t get_node_id();
		void send_msg(uint64_t dest_id, void ** data, int * sizes, int num); 
		base_query * recv_msg();
		void simple_send_msg(int size); 
		uint64_t simple_recv_msg();
		//void set_ifaddr(const char * ifaddr, uint64_t n) { this.ifaddr[n] = ifaddr; }
	private:
		Socket * s;

		uint64_t _node_id;
    uint64_t _node_cnt;
		char ** ifaddr;

};

#endif
