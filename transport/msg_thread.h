#ifndef _MSG_THREAD_H_
#define _MSG_THREAD_H_

#include "global.h"
#include "helper.h"
#include "nn.hpp"

struct mbuf {
  char * buffer;
  uint64_t starttime;
  uint64_t ptr;
  uint64_t cnt;
  bool wait;

  void init(uint64_t dest_id) {
    buffer = (char*)nn_allocmsg(g_msg_size,0);
  }
  void reset(uint64_t dest_id) {
    //buffer = (char*)nn_allocmsg(g_msg_size,0);
    memset(buffer,0,g_msg_size);
    starttime = 0;
    cnt = 0;
    wait = false;
	  ((uint32_t*)buffer)[0] = dest_id;
	  ((uint32_t*)buffer)[1] = g_node_id;
    ptr = sizeof(uint32_t) * 3;
  }
  void copy(char * p, uint64_t s) {
    assert(ptr + s <= g_msg_size);
    if(cnt == 0)
      starttime = get_sys_clock();
    COPY_BUF_SIZE(buffer,p,ptr,s);
    //memcpy(&((char*)buffer)[size],p,s);
    //size += s;
  }
  bool fits(uint64_t s) {
    return (ptr + s) <= g_msg_size;
  }
  bool ready() {
    if(starttime == 0)
      return false;
    if( (get_sys_clock() - starttime) >= g_msg_time_limit )
      return true;
    return false;
  }
};

class MessageThread {
public:
  void init(uint64_t thd_id);
  void run();
  void copy_to_buffer(mbuf * sbuf, RemReqType type, base_query * qry); 
  uint64_t get_msg_size(RemReqType type, base_query * qry); 
  void rack( mbuf * sbuf,base_query * qry);
  void rprepare( mbuf * sbuf,base_query * qry);
  void rfin( mbuf * sbuf,base_query * qry);
  void cl_rsp(mbuf * sbuf, base_query *qry);
  void rinit(mbuf * sbuf,base_query * qry);
  void rqry( mbuf * sbuf, base_query *qry);
  void rfwd( mbuf * sbuf, base_query *qry);
  void rqry_rsp( mbuf * sbuf, base_query *qry);
  void rtxn(mbuf * sbuf, base_query *qry);
  void rtxn_seq(mbuf * sbuf, base_query *qry);
private:
  mbuf ** buffer;
  uint64_t buffer_cnt;
  uint64_t _thd_id;
  base_query * head_qry;
  RemReqType head_type;
  uint64_t head_dest;

};

#endif
