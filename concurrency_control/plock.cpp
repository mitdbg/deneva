#include "global.h"
#include "helper.h"
#include "plock.h"
#include "mem_alloc.h"
#include "txn.h"
#include "remote_query.h"

/************************************************/
// per-partition Manager
/************************************************/
void PartMan::init(uint64_t node_id) {
	uint64_t part_id = get_part_id(this);
	_node_id = node_id; 
	waiter_cnt = 0;
	owner = UINT64_MAX;
	owner_ts = 0;
	waiters = (uint64_t *)
		mem_allocator.alloc(sizeof(uint64_t) * g_thread_cnt * g_node_cnt, part_id);
	waiters_ts = (uint64_t *)
		mem_allocator.alloc(sizeof(uint64_t) * g_thread_cnt * g_node_cnt, part_id);
	waiters_rp = (uint64_t **)
		mem_allocator.alloc(sizeof(uint64_t*) * g_thread_cnt * g_node_cnt, part_id);
	pthread_mutex_init( &latch, NULL );
}



RC PartMan::lock(uint64_t pid,  uint64_t * rp, uint64_t ts) {
	RC rc;

	pthread_mutex_lock( &latch );
	if (owner == UINT64_MAX) {
		owner = pid;
		owner_ts = ts;
		owner_rp = rp;
		// If not local, send remote response
		if(GET_NODE_ID(owner) != _node_id)
			remote_rsp(true,RCOK,GET_NODE_ID(owner),owner);
		rc = RCOK;
	} else if (owner_ts < ts) {
		int i;
		assert(waiter_cnt < (g_thread_cnt * g_node_cnt -1 ));
		for (i = waiter_cnt; i > 0; i--) {
			if (ts > waiters_ts[i - 1]) {
				waiters[i] = pid;
				waiters_ts[i] = ts;
				waiters_rp[i] = rp;
				break;
			} else {
				waiters[i] = waiters[i - 1];
				waiters_ts[i] = waiters_ts[i - 1];
				waiters_rp[i] = waiters_rp[i - 1];
			}
		}
		if (i == 0) {
			waiters[i] = pid;
			waiters_ts[i] = ts;
			waiters_rp[i] = rp;
		}
		
		waiter_cnt ++;
		// if local request, atom add, else do nothing
		if(GET_NODE_ID(pid) == _node_id)
			ATOM_ADD(*rp, 1);
		//ATOM_ADD(txn->ready_part, 1);
		rc = WAIT;
	} else {
		rc = Abort;
		// if we abort, need to send abort to remote node
		if(GET_NODE_ID(pid) != _node_id)
			remote_rsp(true,rc,GET_NODE_ID(pid),pid);
	}
	pthread_mutex_unlock( &latch );
	return rc;
}

void PartMan::unlock(uint64_t pid,  uint64_t * rp) {
	pthread_mutex_lock( &latch );
	if (pid == owner) {		
		if (waiter_cnt == 0) 
			owner = UINT64_MAX;
		else {
			// TODO: if waiter[0] is remote, send a RULK_RSP
			owner = waiters[0];			
			owner_ts = waiters_ts[0];
			owner_rp = waiters_rp[0];
			for (UInt32 i = 0; i < waiter_cnt - 1; i++) {
				assert( waiters_ts[i] < waiters_ts[i + 1] );
				waiters[i] = waiters[i + 1];
				waiters_ts[i] = waiters_ts[i + 1];
				waiters_rp[i] = waiters_rp[i + 1];
			}
			waiter_cnt --;
			// if local, decr, else send response to owner
			if(GET_NODE_ID(owner) == _node_id)
				ATOM_SUB(*owner_rp, 1);
			else {
				remote_rsp(true,RCOK,GET_NODE_ID(owner),owner);
			}
		} 
	} else {
		bool find = false;
		for (UInt32 i = 0; i < waiter_cnt; i++) {
			if (waiters[i] == pid) 
				find = true;
			if (find && i < waiter_cnt - 1)  {
				waiters[i] = waiters[i + 1];
				waiters_ts[i] = waiters_ts[i + 1];
				waiters_rp[i] = waiters_rp[i + 1];
			}
		}
		// if local, decr. shared variable
		// We don't send response for rulk
		if(GET_NODE_ID(pid) == _node_id)
			ATOM_SUB(*rp, 1);
		// We may not find a remote request among the waiters; ignore
		//assert(find);
		if(find)
			waiter_cnt --;
	}
	pthread_mutex_unlock( &latch );
}

void PartMan::remote_rsp(bool l, RC rc, uint64_t node_id, uint64_t pid) {
	int max_num = 4;
	void ** data = new void *[max_num];
	int * sizes = new int [max_num];
	int num = 0;
	uint64_t _pid = pid;
	RC _rc = rc;
	RemReqType rtype = l ? RLK_RSP : RULK_RSP;
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &_rc;
	sizes[num++] = sizeof(RC);
	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t);

	rem_qry_man.send_remote_rsp(node_id,data,sizes,num,0);
}


/************************************************/
// Partition Lock
/************************************************/

void Plock::init(uint64_t node_id) {
	_node_id = node_id;
	ARR_PTR(PartMan, part_mans, g_part_cnt);
	_ready_parts = new  uint64_t[g_thread_cnt];
	_rcs = new RC[g_thread_cnt];
	for (UInt32 i = 0; i < g_part_cnt; i++)
		part_mans[i]->init(node_id);
}

// TODO: send remote requests in parallel
// TODO: make non-blocking remote requests
RC Plock::lock(txn_man * txn, uint64_t * parts, uint64_t part_cnt) {
	RC rc;
	uint64_t tid = txn->get_thd_id();
	uint64_t nid = txn->get_node_id();
	_rcs[tid] = RCOK;
	_ready_parts[tid] = 0;
	ts_t starttime = get_sys_clock();
	UInt32 i;
	for (i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		if(GET_NODE_ID(part_id) == get_node_id()) 
			rc = part_mans[part_id]->lock(GET_PART_ID(tid,nid), &_ready_parts[tid], txn->get_ts());
		else {
			// Increment txn->ready_part; Pass txn to remote thr somehow?
			// Have some Plock shared object and spin on that instead of txn object?
			ATOM_ADD(_ready_parts[tid],1);
			remote_qry(true,GET_PART_ID(tid,nid),part_id,txn->get_ts());
		}
		if (rc == Abort || _rcs[tid] == Abort)
			break;
	}
	if (_ready_parts[tid] > 0 && !(rc == Abort || _rcs[tid] == Abort)) {
		ts_t t = get_sys_clock();
		while (_ready_parts[tid] > 0) {
			if(_rcs[tid] == Abort)
				break;
		}
		INC_TMP_STATS(tid, time_wait, get_sys_clock() - t);
	}
	// Abort and send unlock requests as necessary
	if (rc == Abort || _rcs[tid] == Abort) {
		for (UInt32 j = 0; j < i; j++) {
			uint64_t part_id = parts[j];
			if(GET_NODE_ID(part_id) == get_node_id()) 
				part_mans[part_id]->unlock(GET_PART_ID(tid,nid),&_ready_parts[tid]);
			else {
				remote_qry(false,GET_PART_ID(tid,nid),part_id,txn->get_ts());
				ATOM_SUB(_ready_parts[tid],1);
			}
		}
		INC_TMP_STATS(tid, time_man, get_sys_clock() - starttime);
		return Abort;
	}
	assert(_ready_parts[tid] == 0);
	INC_TMP_STATS(tid, time_man, get_sys_clock() - starttime);
	return RCOK;
}

void Plock::unlock(txn_man * txn, uint64_t * parts, uint64_t part_cnt) {
	ts_t starttime = get_sys_clock();
	for (UInt32 i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		if(GET_NODE_ID(part_id) == get_node_id()) 
			part_mans[part_id]->unlock(GET_PART_ID(txn->get_thd_id(),txn->get_node_id()),&_ready_parts[txn->get_thd_id()]);
		else
			remote_qry(false,GET_PART_ID(txn->get_thd_id(),txn->get_node_id()),part_id,txn->get_ts());
	}
	INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
}

void Plock::unpack_rsp(base_query * query, void * d) {
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(RemReqType);
	memcpy(&query->rc,&data[ptr],sizeof(RC));
	ptr += sizeof(query->rc);
	memcpy(&query->pid,&data[ptr],sizeof(query->pid));
	ptr += sizeof(query->pid);
}

void Plock::unpack(base_query * query, char * data) {
	uint64_t ptr = HEADER_SIZE + sizeof(RemReqType);
	assert(query->rtype == RLK || query->rtype == RULK);
		
	memcpy(&query->pid,&data[ptr],sizeof(query->pid));
	ptr += sizeof(query->pid);
	memcpy(&query->ts,&data[ptr],sizeof(query->ts));
	ptr += sizeof(query->ts);
	memcpy(&query->part_cnt,&data[ptr],sizeof(query->part_cnt));
	ptr += sizeof(query->part_cnt);
	query->parts = new uint64_t[query->part_cnt];
	for (uint64_t i = 0; i < query->part_cnt; i++) {
		memcpy(&query->parts[i],&data[ptr],sizeof(query->parts[i]));
		ptr += sizeof(query->parts[i]);
	}
}

void Plock::remote_qry(bool l, uint64_t pid, uint64_t lid, uint64_t ts) {
	assert(GET_NODE_ID(lid) != _node_id);
	int num = 0;
	int max_num = 5;
	uint64_t part_cnt = 1;
	uint64_t _ts = ts;
	uint64_t _pid = pid;
	uint64_t _lid = lid;
	RemReqType rtype = l ? RLK : RULK;
	void ** data = new void *[max_num];
	int * sizes = new int [max_num];
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &_ts;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &part_cnt;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &_lid;
	sizes[num++] = sizeof(uint64_t);

	rem_qry_man.send_remote_query(GET_NODE_ID(lid),data,sizes,num,0);
}

void Plock::rem_unlock(uint64_t pid, uint64_t * parts, uint64_t part_cnt) {
	ts_t starttime = get_sys_clock();
	for (UInt32 i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		assert(GET_NODE_ID(part_id) == get_node_id());
		part_mans[part_id]->unlock(pid,NULL);
	}
	INC_TMP_STATS(1, time_man, get_sys_clock() - starttime);
}

void Plock::rem_lock(uint64_t pid, uint64_t ts, uint64_t * parts, uint64_t part_cnt) {
	ts_t starttime = get_sys_clock();
	for (UInt32 i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		assert(GET_NODE_ID(part_id) == get_node_id());
		part_mans[part_id]->lock(pid,NULL,ts);
	}
	INC_TMP_STATS(1, time_man, get_sys_clock() - starttime);
}

void Plock::rem_lock_rsp(uint64_t pid, RC rc) {
	ts_t starttime = get_sys_clock();
	if(rc != RCOK)
		_rcs[GET_THREAD_ID(pid)] = rc;
	if(rc == RCOK)
		ATOM_SUB(_ready_parts[GET_THREAD_ID(pid)], 1);
	INC_TMP_STATS(1, time_man, get_sys_clock() - starttime);
}
