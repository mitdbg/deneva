/*
   Copyright 2015 Rachael Harding

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "concurrentqueue.h"
#include "wl.h"


void QWorkQueue::init(Workload * wl) {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  q_len = g_part_cnt / g_node_cnt;
#else
  q_len = 1;
#endif
	_wl = wl;
  hash = (QHash *) mem_allocator.alloc(sizeof(QHash));
  hash->init();
  uint64_t limit = g_thread_cnt + g_send_thread_cnt + g_rem_thread_cnt + 1;
  active_txns = (uint64_t*) mem_allocator.alloc(sizeof(uint64_t) *limit);
  for(uint64_t i = 0; i < limit; i++) {
    active_txns[i] = UINT64_MAX;
  }
  pthread_mutex_init(&mtx,NULL);
  pthread_cond_init(&cond,NULL);
  wq_cnt = 0;
  rem_wq_cnt = 0;
  new_wq_cnt = 0;
  sched_wq_cnt = 0;
  abrt_cnt = 0;
  aq_head = NULL;
  //_wl->curr_epoch = 1;
  new_epoch = false;
  last_sched_dq = NULL;
  sched_ptr = 0;
  sched_head = (wq_entry_t*) mem_allocator.alloc(sizeof(wq_entry_t*)*g_node_cnt);
  sched_tail = (wq_entry_t*) mem_allocator.alloc(sizeof(wq_entry_t*)*g_node_cnt);
  for(uint64_t i = 0; i < g_node_cnt; i++) {
    sched_head[i] = NULL;
    sched_tail[i] = NULL;
  }
}

int inline QWorkQueue::get_idx(int input) {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
    int idx = input;
#else
    int idx = 0;
#endif
    return idx;
}

void QWorkQueue::enqueue_new(uint64_t thd_id, BaseQuery * qry) {
    new_wq.enqueue(qry);
}

void QWorkQueue::sched_enqueue(BaseQuery * qry) {
  assert(CC_ALG == CALVIN);
  uint64_t id = qry->return_id;
  assert(ISSERVERN(id));
  assert(qry);
  wq_entry_t en = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry));
  pthread_mutex_lock(&mtx);
  en->qry = qry;
  //DEBUG("SEnq %ld %ld,%ld\n",id,qry->txn_id,qry->batch_id);
  LIST_PUT_TAIL(sched_head[id],sched_tail[id],en)
  pthread_mutex_unlock(&mtx);
  ATOM_ADD(sched_wq_cnt,1);
}

bool QWorkQueue::sched_dequeue(BaseQuery *& qry) {

  bool result = false;
  assert(CC_ALG == CALVIN);
  pthread_mutex_lock(&mtx);
  while(true) {
    if(last_sched_dq != NULL) {
      qry = last_sched_dq;
      result = true;
      break;
    }
    wq_entry_t en = sched_head[sched_ptr];
    if(!en || simulation->get_worker_epoch() > simulation->get_sched_epoch() || (new_epoch && simulation->epoch_txn_cnt > 0)) {
      qry = NULL;
      break;
      //return false;
    }
    new_epoch = false;

    if(en->qry->rtype == RDONE) {
      ATOM_SUB(sched_wq_cnt,1);
      LIST_REMOVE_HT(en,sched_head[sched_ptr],sched_tail[sched_ptr])
      qry_pool.put(en->qry);
      mem_allocator.free(en,sizeof(struct wq_entry));
      DEBUG("RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
      sched_ptr++;
      if(sched_ptr == g_node_cnt) {
        simulation->next_worker_epoch();
        sched_ptr = 0;
        new_epoch = true;
      }
      continue;
    }

    ATOM_SUB(sched_wq_cnt,1);
    simulation->inc_epoch_txn_cnt();
    LIST_REMOVE_HT(en,sched_head[sched_ptr],sched_tail[sched_ptr])
    qry = en->qry;
    DEBUG("SDeq %ld (%ld,%ld) %ld\n",sched_ptr,qry->txn_id,qry->batch_id,simulation->get_worker_epoch());
    assert(qry->batch_id == simulation->get_worker_epoch());
    mem_allocator.free(en,sizeof(struct wq_entry));
    result = true;
    break;

  }
  pthread_mutex_unlock(&mtx);
  return result;

}


// types:
// 9: N/A
// 0: wq
// 1: sched_wq
// 2: new_wq
void QWorkQueue::enqueue(uint64_t thd_id, BaseQuery * qry,bool busy) {
  uint64_t starttime = get_sys_clock();
  qry->q_starttime = starttime;
  int q_type __attribute__((unused));
  q_type = 9;
  RemReqType rtype = NO_MSG;
  uint64_t batch_id = UINT64_MAX;
  uint64_t return_id = UINT64_MAX;
  uint64_t txn_id = UINT64_MAX;
  if(qry) {
    rtype = qry->rtype;
    batch_id = qry->batch_id;
    return_id =qry->return_id;
    txn_id =qry->txn_id;
  }
#if CC_ALG == CALVIN
  if(ISCLIENT) {
    ATOM_ADD(wq_cnt,1);
    wq.enqueue(qry);
    q_type = 0;
  } else {
    if(busy) { //re-enqueue
      if(thd_id < g_thread_cnt-2) { //worker
          ATOM_ADD(wq_cnt,1);
          wq.enqueue(qry);
          q_type = 0;
      } else if(thd_id == g_thread_cnt-2) { // lock thread / scheduler
        sched_enqueue(qry);
        q_type = 1;
      } else if(thd_id == g_thread_cnt-1) { // sequencer
          ATOM_ADD(new_wq_cnt,1);
          new_wq.enqueue(qry);
          q_type = 2;
      } else {
        assert(false);
      }
    } else {
    if(thd_id < g_thread_cnt-2) { //worker
      if(qry->rtype == RACK) {
        ATOM_ADD(new_wq_cnt,1);
        new_wq.enqueue(qry);
        q_type = 2;
      } else {
        ATOM_ADD(wq_cnt,1);
        wq.enqueue(qry);
        q_type = 0;
      }
    } else if(thd_id == g_thread_cnt-2) { // lock thread / scheduler
      ATOM_ADD(wq_cnt,1);
      wq.enqueue(qry);
      q_type = 0;
    } else if(thd_id == g_thread_cnt-1) { // sequencer
      sched_enqueue(qry);
      q_type = 1;
    } else { // receiver
      if(ISCLIENTN(qry->return_id)) {
        ATOM_ADD(new_wq_cnt,1);
        new_wq.enqueue(qry);
        q_type = 2;
      } else {
        if(qry->rtype == RFWD || qry->rtype == RFIN) {
          ATOM_ADD(wq_cnt,1);
          wq.enqueue(qry);
          q_type = 0;
        } else if (qry->rtype == RTXN || qry->rtype == RDONE){
          // To lock thread
          sched_enqueue(qry);
          q_type = 1;
        } else {
          // To sequencer
          ATOM_ADD(new_wq_cnt,1);
          new_wq.enqueue(qry);
          q_type = 2;
        }
      }
    }
    }
  }
  assert(ISCLIENT || rtype == CL_RSP || (q_type == 2 && rtype == RTXN) || batch_id != UINT64_MAX);
  assert(
      (q_type == 0  && (rtype == CL_RSP || rtype == RTXN || rtype == RFWD || rtype == RQRY || rtype == RFIN))
      || (q_type == 1  && (rtype == RTXN || rtype == RDONE))
      || (q_type == 2  && (rtype == RTXN || rtype == RACK))
      );
#else
  switch(PRIORITY) {
    case PRIORITY_FCFS:
      ATOM_ADD(wq_cnt,1);
      wq.enqueue(qry);
      INC_STATS(thd_id,wq_enqueue,get_sys_clock() - starttime);
      break;
    case PRIORITY_ACTIVE:
      if(qry->txn_id == UINT64_MAX) {
        ATOM_ADD(new_wq_cnt,1);
        new_wq.enqueue(qry);
        INC_STATS(thd_id,new_wq_enqueue,get_sys_clock() - starttime);
      }
      else {
        ATOM_ADD(wq_cnt,1);
        wq.enqueue(qry);
        INC_STATS(thd_id,wq_enqueue,get_sys_clock() - starttime);
        }
      break;
    case PRIORITY_HOME:
      if(qry->txn_id == UINT64_MAX) {
        ATOM_ADD(new_wq_cnt,1);
        new_wq.enqueue(qry);
        INC_STATS(thd_id,new_wq_enqueue,get_sys_clock() - starttime);
      }
      else if (IS_REMOTE(qry->txn_id)) {
        ATOM_ADD(rem_wq_cnt,1);
        rem_wq.enqueue(qry);
        INC_STATS(thd_id,rem_wq_enqueue,get_sys_clock() - starttime);
      }
      else {
        ATOM_ADD(wq_cnt,1);
        wq.enqueue(qry);
        INC_STATS(thd_id,wq_enqueue,get_sys_clock() - starttime);
      }
      break;
    default: assert(false);
  }
#endif
  DEBUG("%ld ENQUEUE (%ld,%ld); %ld; %d, %d,0x%lx\n",thd_id,txn_id,batch_id,return_id,q_type,rtype,(uint64_t)qry);
  INC_STATS(thd_id,all_wq_enqueue,get_sys_clock() - starttime);
}
//TODO: do we need to has qry id here?
// If we do, maybe add in check as an atomic var in BaseQuery
// Current hash implementation requires an expensive mutex
Message QWorkQueue::dequeue() {
  bool valid;
  uint64_t prof_starttime = get_sys_clock();
  int q_type __attribute__((unused));
  q_type = 9;
#if CC_ALG == CALVIN
  if(ISCLIENT) {
    valid = wq.try_dequeue(qry);
    if(valid) {
      ATOM_SUB(wq_cnt,1);
    }
    q_type = 0;
  } else {
    if(thd_id < g_thread_cnt-2) {
      valid = wq.try_dequeue(qry);
      if(valid) {
        ATOM_SUB(wq_cnt,1);
      }
      q_type = 0;
    } else if(thd_id == g_thread_cnt-2) { // lock thread / scheduler
      //valid = sched_wq.try_dequeue(qry);
      valid = sched_dequeue(qry);
      q_type = 1;
    } else if(thd_id == g_thread_cnt-1) { // sequencer
      valid = new_wq.try_dequeue(qry);
      if(valid) {
        ATOM_SUB(new_wq_cnt,1);
      }
      q_type = 2;
    } else {
      assert(false);
    }

  }
  assert(!valid || ISCLIENT || qry->rtype == CL_RSP || (q_type == 2 && qry->rtype == RTXN) || qry->batch_id != UINT64_MAX);
  assert(
      !valid
      || (q_type == 0  && (qry->rtype == CL_RSP || qry->rtype == RTXN || qry->rtype == RFWD || qry->rtype == RQRY || qry->rtype == RFIN))
      || (q_type == 1  && (qry->rtype == RTXN))
      || (q_type == 2  && (qry->rtype == RTXN || qry->rtype == RACK))
      );
#else
  uint64_t starttime = prof_starttime;
  valid = wq.try_dequeue(qry);
  INC_STATS(thd_id,wq_dequeue,get_sys_clock() - starttime);
  if(valid) {
    ATOM_SUB(wq_cnt,1);
  }
  if(PRIORITY == PRIORITY_HOME) {
    if(!valid) {
      starttime = get_sys_clock();
      valid = rem_wq.try_dequeue(qry);
      INC_STATS(thd_id,rem_wq_dequeue,get_sys_clock() - starttime);
      if(valid) {
        ATOM_SUB(rem_wq_cnt,1);
      }
    }
  }
  if(PRIORITY == PRIORITY_HOME || PRIORITY == PRIORITY_ACTIVE) {
    if(!valid) {
      starttime = get_sys_clock();
      valid = new_wq.try_dequeue(qry);
      INC_STATS(thd_id,new_wq_dequeue,get_sys_clock() - starttime);
      if(valid) {
        ATOM_SUB(new_wq_cnt,1);
      }
    }
  }
#endif


  if(!ISCLIENT && valid && qry->txn_id != UINT64_MAX && !(CC_ALG == CALVIN && thd_id == g_thread_cnt-1)) {
    if(set_active(thd_id, qry->txn_id)) {
      //DEBUG("%ld BUSY %ld %ld %ld %ld\n",thd_id,qry->txn_id,wq_cnt,rem_wq_cnt,new_wq_cnt);
      if(CC_ALG == CALVIN && thd_id == g_thread_cnt -2) {
        last_sched_dq = qry;
      } else {
        enqueue(thd_id,qry,true);
      }
      qry = NULL;
      valid = false;
    }
  }
  if(valid) {
    uint64_t t = get_sys_clock() - qry->q_starttime;
    if(CC_ALG == CALVIN && thd_id == g_thread_cnt -2) {
      last_sched_dq = NULL;
    }
    qry->time_q_work += t;
    INC_STATS(0,qq_cnt,1);
    INC_STATS(0,qq_lat,t);
    DEBUG("%ld DEQUEUE (%ld,%ld) %ld; %d, %d, 0x%lx\n",thd_id,qry->txn_id,qry->batch_id,qry->return_id,q_type,qry->rtype,(uint64_t)qry);
  }
  INC_STATS(thd_id,all_wq_dequeue,get_sys_clock() - prof_starttime);
  return valid;
}

bool QWorkQueue::set_active(uint64_t thd_id, uint64_t txn_id) {
  bool result = false;
  uint64_t limit = g_thread_cnt + g_send_thread_cnt + g_rem_thread_cnt + 1;
  pthread_mutex_lock(&mtx);
  while(true) {
    uint64_t i = 0;
    for(i = 0; i < limit; i++) {
      if(i == thd_id)
        continue;
      if(active_txns[i] == txn_id) {
        result = true;
        goto end;
      }
    }
    if(i==limit)
      break;
    //pthread_cond_wait(&cond,&mtx);
  }
  active_txns[thd_id] = txn_id;
  //pthread_cond_broadcast(&cond);
end:
  pthread_mutex_unlock(&mtx);
  return result;
} 
void QWorkQueue::delete_active(uint64_t thd_id, uint64_t txn_id) {
  pthread_mutex_lock(&mtx);
  //assert(active_txns[thd_id] == txn_id);
  active_txns[thd_id] = UINT64_MAX;
  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mtx);
}

void QWorkQueue::update_hash(int tid, uint64_t id) {
  set_active(tid,id);
  //hash->update_qhash(id);
}

void QWorkQueue::done(int tid, uint64_t id) {
  delete_active(tid,id);
  /*
  int idx = get_idx(tid);
    return queue[idx].done(id);
    */
}


// FIXME: Rewrite abort queue
void AbortQueue::enqueue(uint64_t txn_id, uint64_t abort_cnt) {
  uint64_t penalty = g_abort_penalty;
#if BACKOFF
  penalty = max(penalty * 2^abort_cnt,g_abort_penalty_max);
#endif
  penalty += get_sys_clock();
  abort_entry * entry = new abort_entry(penalty,txn_id);
  INSERT_TAIL(head,tail,entry);
  
  INC_STATS(tid,aq_enqueue,get_sys_clock() - starttime);
}

void AbortQueue::process_aborts() {
  uint64_t starttime = get_sys_clock();
  abort_entry * entry;
  while(head && head->penalty_end < starttime) {
    LIST_GET_HEAD(head,tail,entry);
    // restart entry
  }
  abort_entry * entry_to_insert;
  while(entry_to_insert != NULL) {
  }

  INC_STATS(0,aq_dequeue,get_sys_clock() - starttime);

}

BaseQuery * AbortQueue::get_next_abort_query(int tid) {
  uint64_t starttime = get_sys_clock();
  //BaseQuery * rtn_qry = queue[idx].get_next_abort_query();
  BaseQuery * rtn_qry = NULL;
  if(aq_head && aq_head->penalty_end < starttime) {
    ATOM_SUB(abrt_cnt,1);
    rtn_qry = aq_head;
    aq_head = NULL;
  }
  INC_STATS(0,aq_dequeue,get_sys_clock() - starttime);
  return rtn_qry;
}

/**********************
  Hash table
  *********************/
void QHash::init() {
  id_hash_size = 1069;
  //id_hash_size = g_inflight_max*g_node_cnt;
  id_hash = new id_entry_t[id_hash_size];
  for(uint64_t i = 0; i < id_hash_size; i++) {
    id_hash[i] = NULL;
  }
  pthread_mutex_init(&mtx,NULL);
}

void QHash::lock() {
  pthread_mutex_lock(&mtx);
}

void QHash::unlock() {
  pthread_mutex_unlock(&mtx);
}

bool QHash::in_qhash(uint64_t id) {
  if( id == UINT64_MAX)
    return true;
  id_entry_t bin = id_hash[id % id_hash_size];
  while(bin && bin->id != id) {
    bin = bin->next;
  }
  if(bin)
    return true;
  return false;

}

void QHash::add_qhash(uint64_t id) {
  if(id == UINT64_MAX)
    return;
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
}

void QHash::update_qhash(uint64_t id) {
  pthread_mutex_lock(&mtx);
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
  pthread_mutex_unlock(&mtx);
}

void QHash::remove_qhash(uint64_t id) {
  if(id ==UINT64_MAX)
    return;
  pthread_mutex_lock(&mtx);

  id_entry_t bin = id_hash[id % id_hash_size];
  id_entry_t prev = NULL;
  while(bin && bin->id != id) {
    prev = bin;
    bin = bin->next;
  }
  assert(bin);

  if(!prev)
    id_hash[id % id_hash_size] = bin->next;
  else
    prev->next = bin->next;

  pthread_mutex_unlock(&mtx);

}


