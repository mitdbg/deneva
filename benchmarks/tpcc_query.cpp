#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

void tpcc_query::init(uint64_t thd_id, workload * h_wl) {
	//double x = (double)(rand() % 100) / 100.0;
	//part_to_access = (uint64_t *) 
	//	mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	//pid = GET_PART_ID(0,g_node_id);
	//// TODO
	//if (x < g_perc_payment)
	//	gen_payment(pid);
	//else 
	//	gen_new_order(pid);
    init(thd_id, h_wl, g_node_id);
}

void tpcc_query::init(uint64_t thd_id, workload * h_wl, uint64_t node_id) {
	double x = (double)(rand() % 100) / 100.0;
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	pid = GET_PART_ID(0,node_id);
	// TODO
	if (x < g_perc_payment)
		gen_payment(pid);
	else 
		gen_new_order(pid);
}

void tpcc_query::reset() {
  switch(txn_type) {
    case TPCC_PAYMENT:
      txn_rtype = TPCC_PAYMENT0;
      break;
    case TPCC_NEW_ORDER:
      txn_rtype = TPCC_NEWORDER0;
      break;
    default:
      assert(false);
      break;
  }
}

/*
uint64_t tpcc_query::get_keys(uint64_t *& arr) {
  uint64_t num_keys = txn_type == TPCC_PAYMENT ? 4 : 3 + ol_cnt;
  uint64_t keys[num_keys];
  uint64_t n = 0;

  uint64_t w_key = w_id;
  keys[n++] = w_key;
  uint64_t d_key = txn_type == TPCC_PAYMENT ? distKey(d_id, d_w_id) : distKey(d_id, w_id);
  keys[n++] = d_key;
  uint64_t cnp_key = txn_type == TPCC_PAYMENT ? custNPKey(c_last, c_d_id, c_w_id) : UINT64_MAX;
  if(txn_type == TPCC_PAYMENT)
    keys[n++] = cnp_key;
  uint64_t c_key =  txn_type == TPCC_PAYMENT ? custKey(c_id, c_d_id, c_w_id) : custKey(c_id, d_id, w_id);
  keys[n++] = c_key;

  if(txn_type == TPCC_NEWORDER) {
    for(uint64_t i = 0; i < ol_cnt; i++) {
      // item table is replicated at every node
      //uint64_t ol_key = items[i].ol_i_id;
      uint64_t s_key = stockKey(items[i].ol_i_id, items[i].ol_supply_w_id);
      keys[n++] = s_key;
    }
  }

  arr = keys;
  return size;
}
*/

// Note: If you ever change the number of parameters sent, change "total"
void tpcc_query::remote_qry(base_query * query, int type, int dest_id) {

#if DEBUG_DISTR
  printf("Sending RQRY %ld\n",query->txn_id);
#endif
	tpcc_query * m_query = (tpcc_query *) query;
	TPCCRemTxnType t = (TPCCRemTxnType) type;

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
	int total = 13;

#if CC_ALG == WAIT_DIE | CC_ALG == TIMESTAMP || CC_ALG == MVCC
  total ++;   // For timestamp
#endif
#if CC_ALG == MVCC
  total ++;
#endif
#if CC_ALG == OCC
  total ++; // For start_ts
#endif

	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;
	RemReqType rtype = RQRY;
	uint64_t _pid = m_query->pid;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &t;
	sizes[num++] = sizeof(TPCCRemTxnType); 
	// The requester's PID
	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t); 

  data[num] = &m_query->txn_id;
  sizes[num++] = sizeof(txnid_t);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
  data[num] = &m_query->ts;
  sizes[num++] = sizeof(uint64_t);   // sizeof ts_t
#endif
#if CC_ALG == MVCC 
  data[num] = &m_query->thd_id;
  sizes[num++] = sizeof(uint64_t);
#endif
#if CC_ALG == OCC
  data[num] = &m_query->start_ts;
  sizes[num++] = sizeof(uint64_t);   // sizeof ts_t
#endif
	switch(t) {
		case TPCC_PAYMENT0 :
			data[num] = &m_query->w_id;
			sizes[num++] = sizeof(m_query->w_id);
			data[num] = &m_query->d_id;
			sizes[num++] = sizeof(m_query->d_id);
			data[num] = &m_query->d_w_id;
			sizes[num++] = sizeof(m_query->d_w_id);
			data[num] = &m_query->h_amount;
			sizes[num++] = sizeof(m_query->h_amount);
			break;
		case TPCC_PAYMENT4 :
			data[num] = &m_query->w_id;
			sizes[num++] = sizeof(m_query->w_id);
			data[num] = &m_query->d_id;
			sizes[num++] = sizeof(m_query->d_id);
			data[num] = &m_query->c_id;
			sizes[num++] = sizeof(m_query->c_id);
			data[num] = &m_query->c_w_id;
			sizes[num++] = sizeof(m_query->c_w_id);
			data[num] = &m_query->c_d_id;
			sizes[num++] = sizeof(m_query->c_d_id);
			data[num] = &m_query->c_last;
			sizes[num++] = sizeof(m_query->c_last);
			data[num] = &m_query->h_amount;
			sizes[num++] = sizeof(m_query->h_amount);
			data[num] = &m_query->by_last_name;
			sizes[num++] = sizeof(m_query->by_last_name);
			break;
		case TPCC_NEWORDER0 :
			data[num] = &m_query->w_id;
			sizes[num++] = sizeof(m_query->w_id);
			data[num] = &m_query->d_id;
			sizes[num++] = sizeof(m_query->d_id);
			data[num] = &m_query->c_id;
			sizes[num++] = sizeof(m_query->c_id);
			data[num] = &m_query->remote;
			sizes[num++] = sizeof(m_query->remote);
			data[num] = &m_query->ol_cnt;
			sizes[num++] = sizeof(m_query->ol_cnt);
			break;
		case TPCC_NEWORDER6 :
			data[num] = &m_query->ol_i_id;
			sizes[num++] = sizeof(m_query->ol_i_id);
			break;
		case TPCC_NEWORDER8 :
			data[num] = &m_query->w_id;
			sizes[num++] = sizeof(m_query->w_id);
			data[num] = &m_query->d_id;
			sizes[num++] = sizeof(m_query->d_id);
			data[num] = &m_query->remote;
			sizes[num++] = sizeof(m_query->remote);
			data[num] = &m_query->ol_i_id;
			sizes[num++] = sizeof(m_query->ol_i_id);
			data[num] = &m_query->ol_supply_w_id;
			sizes[num++] = sizeof(m_query->ol_supply_w_id);
			data[num] = &m_query->ol_quantity;
			sizes[num++] = sizeof(m_query->ol_quantity);
			data[num] = &m_query->ol_number;
			sizes[num++] = sizeof(m_query->ol_number);
			data[num] = &m_query->o_id;
			sizes[num++] = sizeof(m_query->o_id);
			break;

		default:
			assert(false);
	}
	// FIXME: Use tid as param
	rem_qry_man.send_remote_query(dest_id, data, sizes, num);
}

// Note: If you ever change the number of parameters sent, change "total"
void tpcc_query::remote_rsp(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
  printf("Sending RQRY_RSP %ld\n",query->txn_id);
#endif
	int total = 7;

	void ** data = new void *[total];
	int * sizes = new int [total];

	int num = 0;
	uint64_t _pid = m_query->pid;
	RemReqType rtype = RQRY_RSP;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &m_query->txn_rtype;
	sizes[num++] = sizeof(m_query->txn_rtype);
	data[num] = &m_query->rc;
	sizes[num++] = sizeof(RC);
	// The original requester's pid
	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t);
    data[num] = &m_query->txn_id;
    sizes[num++] = sizeof(txnid_t);
    // TODO: only need to send this after first set of neworder subqueries
	data[num] = &m_query->o_id;
	sizes[num++] = sizeof(m_query->o_id);
	rem_qry_man.send_remote_rsp(m_query->return_id, data, sizes, num);
}

void tpcc_query::unpack_rsp(base_query * query, void * d) {
	char * data = (char *) d;
    RC rc;

	tpcc_query * m_query = (tpcc_query *) query;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
	memcpy(&m_query->txn_rtype,&data[ptr],sizeof(TPCCRemTxnType));
	ptr += sizeof(TPCCRemTxnType);
	memcpy(&rc,&data[ptr],sizeof(RC));
	ptr += sizeof(RC);
	memcpy(&m_query->pid,&data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);
  //memcpy(&m_query->txn_id, &data[ptr], sizeof(txnid_t));
  ptr += sizeof(txnid_t);
	memcpy(&m_query->o_id,&data[ptr],sizeof(m_query->o_id));
	ptr += sizeof(m_query->o_id);

  if(rc == Abort || m_query->rc == WAIT || m_query->rc == WAIT_REM) {
    m_query->rc = rc;
    m_query->txn_rtype = TPCC_FIN;
  }
}

void tpcc_query::unpack(base_query * query, void * d) {
	tpcc_query * m_query = (tpcc_query *) query;
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
	memcpy(&m_query->txn_rtype,&data[ptr],sizeof(TPCCRemTxnType));
	ptr += sizeof(TPCCRemTxnType);
	memcpy(&m_query->pid,&data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);
    //memcpy(&m_query->txn_id, &data[ptr], sizeof(txnid_t));
    ptr += sizeof(txnid_t);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
    memcpy(&m_query->ts, &data[ptr], sizeof(uint64_t));
    ptr += sizeof(uint64_t);
#endif
#if CC_ALG == MVCC 
    memcpy(&m_query->thd_id,&data[ptr], sizeof(uint64_t));
    ptr += sizeof(uint64_t);
#endif
#if CC_ALG == OCC
    memcpy(&m_query->start_ts, &data[ptr], sizeof(uint64_t));
    ptr += sizeof(uint64_t);
#endif
	switch(m_query->txn_rtype) {
		case TPCC_PAYMENT0 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->d_w_id,&data[ptr],sizeof(m_query->d_w_id));
			ptr += sizeof(m_query->d_w_id);
			memcpy(&m_query->h_amount,&data[ptr],sizeof(m_query->h_amount));
			ptr += sizeof(m_query->h_amount);
			break;
		case TPCC_PAYMENT4 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->c_id,&data[ptr],sizeof(m_query->c_id));
			ptr += sizeof(m_query->c_id);
			memcpy(&m_query->c_w_id,&data[ptr],sizeof(m_query->c_w_id));
			ptr += sizeof(m_query->c_w_id);
			memcpy(&m_query->c_d_id,&data[ptr],sizeof(m_query->c_d_id));
			ptr += sizeof(m_query->c_d_id);
			memcpy(&m_query->c_last,&data[ptr],sizeof(m_query->c_last));
			ptr += sizeof(m_query->c_last);
			memcpy(&m_query->h_amount,&data[ptr],sizeof(m_query->h_amount));
			ptr += sizeof(m_query->h_amount);
			memcpy(&m_query->by_last_name,&data[ptr],sizeof(m_query->by_last_name));
			ptr += sizeof(m_query->by_last_name);
			break;
		case TPCC_NEWORDER0 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->c_id,&data[ptr],sizeof(m_query->c_id));
			ptr += sizeof(m_query->c_id);
			memcpy(&m_query->remote,&data[ptr],sizeof(m_query->remote));
			ptr += sizeof(m_query->remote);
			memcpy(&m_query->ol_cnt,&data[ptr],sizeof(m_query->ol_cnt));
			ptr += sizeof(m_query->ol_cnt);
			break;
		case TPCC_NEWORDER6 :
			memcpy(&m_query->ol_i_id,&data[ptr],sizeof(m_query->ol_i_id));
			ptr += sizeof(m_query->ol_i_id);
			break;
		case TPCC_NEWORDER8 :
			memcpy(&m_query->w_id,&data[ptr],sizeof(m_query->w_id));
			ptr += sizeof(m_query->w_id);
			memcpy(&m_query->d_id,&data[ptr],sizeof(m_query->d_id));
			ptr += sizeof(m_query->d_id);
			memcpy(&m_query->remote,&data[ptr],sizeof(m_query->remote));
			ptr += sizeof(m_query->remote);
			memcpy(&m_query->ol_i_id,&data[ptr],sizeof(m_query->ol_i_id));
			ptr += sizeof(m_query->ol_i_id);
			memcpy(&m_query->ol_supply_w_id,&data[ptr],sizeof(m_query->ol_supply_w_id));
			ptr += sizeof(m_query->ol_supply_w_id);
			memcpy(&m_query->ol_quantity,&data[ptr],sizeof(m_query->ol_quantity));
			ptr += sizeof(m_query->ol_quantity);
			memcpy(&m_query->ol_number,&data[ptr],sizeof(m_query->ol_number));
			ptr += sizeof(m_query->ol_number);
			memcpy(&m_query->o_id,&data[ptr],sizeof(m_query->o_id));
			ptr += sizeof(m_query->o_id);
			break;
		default:
			assert(false);
	}
}

void tpcc_query::client_query(base_query * query, uint64_t dest_id) {
#if DEBUG_DISTR
    printf("Sending RTXN %lu\n",query->txn_id);
#endif
	tpcc_query * m_query = (tpcc_query *) query;

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
	int total = 14 + m_query->part_num;
    if (m_query->txn_type == TPCC_NEW_ORDER)
        total += m_query->ol_cnt;

	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

	RemReqType rtype = RTXN;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &m_query->txn_type;
    sizes[num++] = sizeof(TPCCTxnType);
    data[num] = &m_query->pid;
    sizes[num++] = sizeof(uint64_t);
    data[num] = &m_query->part_num;
    sizes[num++] = sizeof(uint64_t);

    for (uint64_t i = 0; i < m_query->part_num; ++i) {
        data[num] = &m_query->part_to_access[i];
        sizes[num++] = sizeof(uint64_t);
    }
    data[num] = &m_query->w_id;
    sizes[num++] = sizeof(uint64_t);
    data[num] = &m_query->d_id;
    sizes[num++] = sizeof(uint64_t);
    data[num] = &m_query->c_id;
    sizes[num++] = sizeof(uint64_t);

    switch (m_query->txn_type) {
        case TPCC_PAYMENT:
            data[num] = &m_query->d_w_id;
            sizes[num++] = sizeof(uint64_t);
            data[num] = &m_query->c_w_id;
            sizes[num++] = sizeof(uint64_t);
            data[num] = &m_query->c_d_id;
            sizes[num++] = sizeof(uint64_t);
            data[num] = &m_query->c_last;
            sizes[num++] = LASTNAME_LEN;
            data[num] = &m_query->h_amount;
            sizes[num++] = sizeof(double);
            data[num] = &m_query->by_last_name;
            sizes[num++] = sizeof(bool);
            break;
        case TPCC_NEW_ORDER:
            data[num] = &m_query->ol_cnt;
            sizes[num++] = sizeof(uint64_t);
            for (uint64_t j = 0; j < m_query->ol_cnt; ++j) {
                data[num] = &m_query->items[j];
                sizes[num++] = sizeof(Item_no);
            }
            data[num] = &m_query->rbk;
            sizes[num++] = sizeof(bool);
            data[num] = &m_query->remote;
            sizes[num++] = sizeof(bool);
            data[num] = &m_query->o_entry_d;
            sizes[num++] = sizeof(uint64_t);
            data[num] = &m_query->o_carrier_id;
            sizes[num++] = sizeof(uint64_t);
            data[num] = &m_query->ol_delivery_d;
            sizes[num++] = sizeof(uint64_t);
            break;
        default:
            assert(false);
    }
	rem_qry_man.send_remote_rsp(dest_id, data, sizes, num);
}

void tpcc_query::unpack_client(base_query * query, void * d) {
	tpcc_query * m_query = (tpcc_query *) query;
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
    m_query->client_id = query->return_id;
    memcpy(&m_query->txn_type, &data[ptr], sizeof(TPCCTxnType));
    ptr += sizeof(TPCCTxnType);
    memcpy(&m_query->pid, &data[ptr], sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    memcpy(&m_query->part_num, &data[ptr], sizeof(uint64_t));
    ptr += sizeof(uint64_t);
	m_query->part_to_access = (uint64_t *) 
		    mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
    for (uint64_t i = 0; i < m_query->part_num; ++i) {
        memcpy(&m_query->part_to_access[i], &data[ptr], sizeof(uint64_t));
        ptr += sizeof(uint64_t);
    }

    if (m_query->txn_type == TPCC_PAYMENT || m_query->txn_type == TPCC_NEW_ORDER) {
        memcpy(&m_query->w_id, &data[ptr], sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        memcpy(&m_query->d_id, &data[ptr], sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        memcpy(&m_query->c_id, &data[ptr], sizeof(uint64_t));
        ptr += sizeof(uint64_t);
    }

    switch(m_query->txn_type) {
        case TPCC_PAYMENT:
            m_query->txn_rtype = TPCC_PAYMENT0;
            memcpy(&m_query->d_w_id, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            memcpy(&m_query->c_w_id, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            memcpy(&m_query->c_d_id, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            memcpy(&m_query->c_last, &data[ptr], LASTNAME_LEN);
            ptr += LASTNAME_LEN;
            memcpy(&m_query->h_amount, &data[ptr], sizeof(double));
            ptr += sizeof(double);
            memcpy(&m_query->by_last_name, &data[ptr], sizeof(bool));
            ptr += sizeof(bool);
            break;
        case TPCC_NEW_ORDER:
            m_query->txn_rtype = TPCC_NEWORDER0;
            memcpy(&m_query->ol_cnt, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
	        m_query->items = (Item_no *) mem_allocator.alloc(
                    sizeof(Item_no) * m_query->ol_cnt, thd_id);
            for (uint64_t j = 0; j < m_query->ol_cnt; ++j) {
                memcpy(&m_query->items[j], &data[ptr], sizeof(Item_no));
                ptr += sizeof(Item_no);
            }
            memcpy(&m_query->rbk, &data[ptr], sizeof(bool));
            ptr += sizeof(bool);
            memcpy(&m_query->remote, &data[ptr], sizeof(bool));
            ptr += sizeof(bool);
            memcpy(&m_query->o_entry_d, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            memcpy(&m_query->o_carrier_id, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            memcpy(&m_query->ol_delivery_d, &data[ptr], sizeof(uint64_t));
            ptr += sizeof(uint64_t);
            break;
        default:
            assert(false);
    }
}

void tpcc_query::gen_payment(uint64_t thd_id) {
	txn_type = TPCC_PAYMENT;
	txn_rtype = TPCC_PAYMENT0;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_w_id = w_id;
	uint64_t part_id = wh_to_part(w_id);
	part_to_access[0] = part_id;
	part_num = 1;

	d_id = URand(1, DIST_PER_WARE);
	h_amount = URand(1, 5000);
	int x = URand(1, 100);
	int y = URand(1, 100);


	if(x > MPR) { 
		// home warehouse
		c_d_id = d_id;
		c_w_id = w_id;
	} else {	
		// remote warehouse
		c_d_id = URand(1, DIST_PER_WARE);
		if(g_num_wh > 1) {
			while((c_w_id = URand(1, g_num_wh)) == w_id) {}
			if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
				part_to_access[1] = wh_to_part(c_w_id);
				part_num = 2;
			}
		} else 
			c_w_id = w_id;
	}
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist);
	}
}

void tpcc_query::gen_new_order(uint64_t thd_id) {
	txn_type = TPCC_NEW_ORDER;
	txn_rtype = TPCC_NEWORDER0;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_id = URand(1, DIST_PER_WARE);
	c_id = NURand(1023, 1, g_cust_per_dist);
	rbk = URand(1, 100);
	ol_cnt = URand(5, 15);
	o_entry_d = 2013;
	items = (Item_no *) mem_allocator.alloc(sizeof(Item_no) * ol_cnt, thd_id);
	remote = false;
	part_to_access[0] = wh_to_part(w_id);
	part_num = 1;

	UInt32 y = URand(1, 100);
	for (UInt32 oid = 0; oid < ol_cnt; oid ++) {

		while(1) {
			UInt32 i;
#if CONTENTION
			items[oid].ol_i_id = 2000 + oid;
#else
			items[oid].ol_i_id = NURand(8191, 1, g_max_items);
#endif
			for (i = 0; i < oid; i++) {
				if (items[i].ol_i_id == items[oid].ol_i_id) {
					break;
				}
			}
			if(i == oid)
				break;
		} 

		//UInt32 x = URand(1, 100);

		if (y > MPR || remote || g_num_wh == 1) {
			// home warehouse
			items[oid].ol_supply_w_id = w_id;
		}
		else  {
			// remote warehouse
			while((items[oid].ol_supply_w_id = URand(1, g_num_wh)) == w_id) {}
#if STRICT_MPR
			remote = true;
#endif
		}
		items[oid].ol_quantity = URand(1, 10);
	}
	// Remove duplicate items
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		for (UInt32 j = 0; j < i; j++) {
			if (items[i].ol_i_id == items[j].ol_i_id) {
				for (UInt32 k = i; k < ol_cnt - 1; k++)
					items[k] = items[k + 1];
				ol_cnt --;
				i--;
			}
		}
	}
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		for (UInt32 j = 0; j < i; j++) 
			assert(items[i].ol_i_id != items[j].ol_i_id);
	}

	// update part_to_access
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		UInt32 j;
		for (j = 0; j < part_num; j++ ) 
			if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
				break;
		if (j == part_num) // not found! add to it.
		part_to_access[part_num ++] = wh_to_part( items[i].ol_supply_w_id );
	}

}

void 
tpcc_query::gen_order_status(uint64_t thd_id) {
	txn_type = TPCC_ORDER_STATUS;
	//txn_rtype = TPCC_ORDER_STATUS0;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_id = URand(1, DIST_PER_WARE);
	c_w_id = w_id;
	c_d_id = d_id;
	int y = URand(1, 100);
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist);
	}
}

/*
void 
tpcc_query::gen_delivery(uint64_t thd_id) {
/	type = TPCC_DELIVERY;
//	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
//	else
//		w_id = URand(1, g_num_wh);
	o_carrier_id = URand(1, 10);
	ol_delivery_d = 2014;
}
*/
//uint64_t tpcc_query::wh_to_part(uint64_t wid) {
//	uint64_t part_id;
//	assert(g_part_cnt <= g_num_wh);
//	part_id = wid % g_part_cnt;
//	return part_id;
//}
