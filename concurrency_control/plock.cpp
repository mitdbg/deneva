#include "global.h"
#include "helper.h"
#include "plock.h"
#include "mem_alloc.h"
#include "txn.h"
#include "remote_query.h"

/************************************************/
// per-partition Manager
/************************************************/
void PartMan::init() {
	uint64_t part_id = get_part_id(this);
	waiter_cnt = 0;
	owner = NULL;
	waiters = (txn_man **)
		mem_allocator.alloc(sizeof(txn_man *) * g_thread_cnt, part_id);
	pthread_mutex_init( &latch, NULL );
}



RC PartMan::lock(txn_man * txn) {
	RC rc;

	pthread_mutex_lock( &latch );
	if (owner == NULL) {
		owner = txn;
		rc = RCOK;
	} else if (owner->get_ts() < txn->get_ts()) {
		int i;
		assert(waiter_cnt < g_thread_cnt);
		for (i = waiter_cnt; i > 0; i--) {
			if (txn->get_ts() > waiters[i - 1]->get_ts()) {
				waiters[i] = txn;
				break;
			} else 
				waiters[i] = waiters[i - 1];
		}
		if (i == 0)
			waiters[i] = txn;
		waiter_cnt ++;
		ATOM_ADD(txn->ready_part, 1);
		rc = WAIT;
	} else
		rc = Abort;
	pthread_mutex_unlock( &latch );
	return rc;
}

void PartMan::unlock(txn_man * txn) {
	pthread_mutex_lock( &latch );
	if (txn == owner) {		
		if (waiter_cnt == 0) 
			owner = NULL;
		else {
			owner = waiters[0];			
			for (UInt32 i = 0; i < waiter_cnt - 1; i++) {
				assert( waiters[i]->get_ts() < waiters[i + 1]->get_ts() );
				waiters[i] = waiters[i + 1];
			}
			waiter_cnt --;
			ATOM_SUB(owner->ready_part, 1);
		} 
	} else {
		bool find = false;
		for (UInt32 i = 0; i < waiter_cnt; i++) {
			if (waiters[i] == txn) 
				find = true;
			if (find && i < waiter_cnt - 1) 
				waiters[i] = waiters[i + 1];
		}
		ATOM_SUB(txn->ready_part, 1);
		assert(find);
		waiter_cnt --;
	}
	pthread_mutex_unlock( &latch );
}

/************************************************/
// Partition Lock
/************************************************/

void Plock::init(uint64_t node_id) {
	_node_id = node_id;
	ARR_PTR(PartMan, part_mans, g_part_cnt);
	for (UInt32 i = 0; i < g_part_cnt; i++)
		part_mans[i]->init();
}

RC Plock::lock(txn_man * txn, uint64_t * parts, uint64_t part_cnt) {
	RC rc;
	ts_t starttime = get_sys_clock();
	UInt32 i;
	for (i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		if(GET_NODE_ID(part_id) == get_node_id()) 
			rc = part_mans[part_id]->lock(txn);
		else
			rc = remote_qry(true,&part_id);
		if (rc == Abort)
			break;
	}
	if (rc == Abort) {
		for (UInt32 j = 0; j < i; j++) {
			uint64_t part_id = parts[j];
			if(GET_NODE_ID(part_id) == get_node_id()) 
				part_mans[part_id]->unlock(txn);
			else
				remote_qry(false,&part_id);
		}
		assert(txn->ready_part == 0);
		INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
		return Abort;
	}
	if (txn->ready_part > 0) {
		ts_t t = get_sys_clock();
		while (txn->ready_part > 0) {}
		INC_TMP_STATS(txn->get_thd_id(), time_wait, get_sys_clock() - t);
	}
	assert(txn->ready_part == 0);
	INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
	return RCOK;
}

void Plock::unlock(txn_man * txn, uint64_t * parts, uint64_t part_cnt) {
	ts_t starttime = get_sys_clock();
	for (UInt32 i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		if(GET_NODE_ID(part_id) == get_node_id()) 
			part_mans[part_id]->unlock(txn);
		else
			remote_qry(false,&part_id);
	}
	INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
}

RC Plock::unpack_rsp(void * d) {
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(RemReqType);
	RC rc;
	memcpy(&rc,&data[ptr],sizeof(RC));
	return rc;
}

void Plock::unpack(base_query * query, char * data) {
	uint64_t ptr = HEADER_SIZE + sizeof(RemReqType);
	assert(query->rtype == RLK || query->rtype == RULK);
		
	memcpy(&query->part_cnt,&data[ptr],sizeof(query->part_cnt));
	ptr += sizeof(query->part_cnt);
	query->parts = new uint64_t[query->part_cnt];
	for (uint64_t i = 0; i < query->part_cnt; i++) {
		memcpy(&query->parts[i],&data[ptr],sizeof(query->parts[i]));
		ptr += sizeof(query->parts[i]);
	}
}

RC Plock::remote_qry(bool l, uint64_t * part_id) {
	int num = 0;
	int max_num = 3;
	uint64_t part_cnt = 1;
	RemReqType rtype = l ? RLK : RULK;
	void ** data = new void *[max_num];
	int * sizes = new int [max_num];
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &part_cnt;
	sizes[num++] = sizeof(uint64_t);
	data[num] = part_id;
	sizes[num++] = sizeof(uint64_t);

	RC rc;
	void * buf;
	buf = rem_qry_man.send_remote_query(GET_NODE_ID(*part_id),data,sizes,num,0);
	rc = unpack_rsp(buf);
	return rc;
}

void Plock::remote_rsp(bool l, RC * rc, uint64_t node_id) {
	int max_num = 4;
	void ** data = new void *[max_num];
	int * sizes = new int [max_num];
	int num = 0;
	RemReqType rtype = l ? RLK_RSP : RULK_RSP;
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &rc;
	sizes[num++] = sizeof(RC);

	rem_qry_man.send_remote_rsp(node_id,data,sizes,num,0);
}
