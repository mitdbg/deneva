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
  hash = (QHash *) mem_allocator.alloc(sizeof(QHash), 0);
  hash->init();
  queue = (QWorkQueueHelper *) mem_allocator.alloc(sizeof(QWorkQueueHelper) * q_len, 0);
  for(int i = 0;i<q_len;i++) {
    queue[i].init(hash); 
  }
  uint64_t limit = g_thread_cnt + g_send_thread_cnt + g_rem_thread_cnt + 1;
  active_txns = (uint64_t*) mem_allocator.alloc(sizeof(uint64_t) *limit,0);
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
  sched_head = (wq_entry_t*) mem_allocator.alloc(sizeof(wq_entry_t*)*g_node_cnt,0);
  sched_tail = (wq_entry_t*) mem_allocator.alloc(sizeof(wq_entry_t*)*g_node_cnt,0);
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

void QWorkQueue::abort_finish(uint64_t time) {
  /*
    for(int idx=0;idx<q_len;idx++)
      queue[idx].abort_finish(time);
      */
} 

void QWorkQueue::enqueue_new(uint64_t thd_id, BaseQuery * qry) {
    new_wq.enqueue(qry);
}

void QWorkQueue::sched_enqueue(BaseQuery * qry) {
  assert(CC_ALG == CALVIN);
  uint64_t id = qry->return_id;
  assert(ISSERVERN(id));
  assert(qry);
  wq_entry_t en = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);
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
    if(!en || _wl->curr_epoch > _wl->epoch || (new_epoch && _wl->epoch_txn_cnt > 0)) {
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
      DEBUG("RDONE %ld %ld\n",sched_ptr,_wl->curr_epoch);
      sched_ptr++;
      if(sched_ptr == g_node_cnt) {
        _wl->curr_epoch++;
        sched_ptr = 0;
        new_epoch = true;
      }
      continue;
    }

    ATOM_SUB(sched_wq_cnt,1);
    ATOM_ADD(_wl->epoch_txn_cnt,1);
    LIST_REMOVE_HT(en,sched_head[sched_ptr],sched_tail[sched_ptr])
    qry = en->qry;
    DEBUG("SDeq %ld (%ld,%ld) %ld\n",sched_ptr,qry->txn_id,qry->batch_id,_wl->curr_epoch);
    if(qry->batch_id == UINT64_MAX)
      assert(false);
    if(qry->batch_id < _wl->curr_epoch)
      assert(false);
    if(qry->batch_id > _wl->curr_epoch)
      assert(false);
    assert(qry->batch_id == _wl->curr_epoch);
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
bool QWorkQueue::dequeue(uint64_t thd_id, BaseQuery *& qry) {
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

void QWorkQueue::add_abort_query(int tid, BaseQuery * qry) {
  uint64_t starttime = get_sys_clock();
  if(qry->penalty == 0)
    qry->penalty = g_abort_penalty;
#if BACKOFF
  else {
    if(qry->penalty * 2 < g_abort_penalty_max)
      qry->penalty = qry->penalty * 2;
  }
#endif
  qry->penalty_end = starttime + qry->penalty;
  aq.enqueue(qry);
  ATOM_ADD(abrt_cnt,1);
  //queue[idx].add_abort_query(qry);
  INC_STATS(tid,aq_enqueue,get_sys_clock() - starttime);
}

bool QWorkQueue::poll_abort(int tid) {
  uint64_t starttime = get_sys_clock();
  //bool result = queue[idx].poll_abort();
  bool result = false;
  if(!aq_head && aq.size_approx() > 0) {
    aq.try_dequeue(aq_head);
  }
  if(aq_head && aq_head->penalty_end < starttime)
    result = true;
  INC_STATS(0,aq_poll,get_sys_clock() - starttime);
  return result;
} 

void QWorkQueue::process_aborts() {
    BaseQuery * qry = NULL;
    uint64_t starttime = get_sys_clock();
    bool valid = aq.try_dequeue(qry);
    int restarts = 0;
    if(valid) 
      restarts = queue[0].add_abort_query(qry);
    else
      restarts = queue[0].check_abort_query();
    ATOM_SUB(abrt_cnt,restarts);
    INC_STATS(0,aq_dequeue,get_sys_clock() - starttime);

}

BaseQuery * QWorkQueue::get_next_abort_query(int tid) {
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
  /*
  if(rtn_qry) {
    ATOM_SUB(abrt_cnt,1);
  }
  return rtn_qry;
  */
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

/*************************
  Query Work Queue Helper
  ************************/
void QWorkQueueHelper::init(QHash * hash) {
  cnt = 0;
  head = NULL;
  tail = NULL;
  this->hash = hash;
  last_add_time = 0;
  pthread_mutex_init(&mtx,NULL);

}

bool QWorkQueueHelper::poll_next_query() {
  return cnt > 0;
}

void QWorkQueueHelper::finish(uint64_t time) {
  if(last_add_time != 0)
    INC_STATS(0,qq_full,time - last_add_time);
}

void QWorkQueueHelper::abort_finish(uint64_t time) {
  if(last_add_time != 0)
    INC_STATS(0,aq_full,time - last_add_time);
}

void QWorkQueueHelper::add_query(BaseQuery * qry) {

  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);
  entry->qry = qry;
  entry->next  = NULL;
  entry->starttime = get_sys_clock();
  assert(qry->rtype <= NO_MSG);

  pthread_mutex_lock(&mtx);

  //printf("Add Query %ld %d\n",qry->txn_id,qry->rtype);
  wq_entry_t n = head;
#if !PRIORITY_WORK_QUEUE
  n = NULL;
#endif

  while(n) {
    assert(CC_ALG==HSTORE_SPEC || n->qry != qry);
#if CC_ALG == HSTORE_SPEC
    if(n->qry == qry)
      goto final;
#endif
    
#if PRIORITY_WORK_QUEUE
    /*
#if CC_ALG == HSTORE_SPEC
    if(qry->spec)
      break;
#else
*/
  if(qry->txn_id == UINT64_MAX || qry->abort_restart) {
    n = NULL;
    break;
  }
    if(n->qry->txn_id > entry->qry->txn_id)
      break;
//#endif
#endif
    n = n->next;
  }

  if(n) {
    LIST_INSERT_BEFORE(n,entry,head);
  }
  else {
    LIST_PUT_TAIL(head,tail,entry);
  }
  cnt++;

#if CC_ALG == HSTORE_SPEC
final:
#endif
  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
}

void QWorkQueueHelper::remove_query(BaseQuery* qry) {

  pthread_mutex_lock(&mtx);

  if(cnt > 0) {
    wq_entry_t next = head;
    while(next) {
      if(next->qry == qry) {
        LIST_REMOVE_HT(next,head,tail);
        cnt--;
        mem_allocator.free(next,sizeof(struct wq_entry));
        break;
      }
      next = next->next;
    }

  }

  pthread_mutex_unlock(&mtx);

}

BaseQuery * QWorkQueueHelper::get_next_query_client() {
  BaseQuery * next_qry = NULL;

  pthread_mutex_lock(&mtx);
  if(cnt > 0) {
    wq_entry_t next = head;
    next_qry = next->qry;
    LIST_REMOVE_HT(next,head,tail);
    cnt--;

    mem_allocator.free(next,sizeof(struct wq_entry));
  }
  pthread_mutex_unlock(&mtx);
  return next_qry;
}

BaseQuery * QWorkQueueHelper::get_next_query(int id) {
  BaseQuery * next_qry = NULL;

  uint64_t thd_prof_start = get_sys_clock();
  pthread_mutex_lock(&mtx);
    INC_STATS(id,thd_prof_wq1,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
  hash->lock();
    INC_STATS(id,thd_prof_wq2,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

  uint64_t starttime = get_sys_clock();

  assert( ( (cnt == 0) && head == NULL && tail == NULL) || ( (cnt > 0) && head != NULL && tail !=NULL) );

  wq_entry_t next = NULL;
  if(cnt > 0) {
    next = head;
    while(next) {
      if(next->qry->txn_id == UINT64_MAX || !hash->in_qhash(next->qry->txn_id)) {
        next_qry = next->qry;
        if(next_qry->txn_id != UINT64_MAX) {
          hash->add_qhash(next_qry->txn_id);
          assert(hash->in_qhash(next_qry->txn_id));
        }
        LIST_REMOVE_HT(next,head,tail);
        cnt--;

      uint64_t t = get_sys_clock() - next->starttime;
      next_qry->time_q_work += t;
      INC_STATS(0,qq_cnt,1);
      INC_STATS(0,qq_lat,t);

        mem_allocator.free(next,sizeof(struct wq_entry));
        break;
      }
      next = next->next;
    }

  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,qq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  /*
  wq_entry_t n = head;
  while(n) {
    assert(n->qry != next_qry);
    n = n->next;
  }
  */

    INC_STATS(id,thd_prof_wq3,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

  INC_STATS(0,time_qq,get_sys_clock() - starttime);
  hash->unlock();
//  if(next_qry)
//    printf("Get Query %ld %d\n",next_qry->txn_id,next_qry->rtype);
  pthread_mutex_unlock(&mtx);
    INC_STATS(id,thd_prof_wq4,get_sys_clock() - thd_prof_start);
  return next_qry;
}


// Remove hash
void QWorkQueueHelper::done(uint64_t id) {
  //hash->remove_qhash(id);
}

bool QWorkQueueHelper::poll_abort() {
  bool ready = false;
  pthread_mutex_lock(&mtx);
  wq_entry_t elem = head;
  if(elem)
    ready = (get_sys_clock() >= elem->qry->penalty_end);
  pthread_mutex_unlock(&mtx);
    //return (get_sys_clock() - elem->qry->penalty_start) >= g_abort_penalty;
  return ready;
}

int QWorkQueueHelper::add_abort_query(BaseQuery * qry) {

  int restarts = 0;
  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);

  /*
  if(qry->penalty == 0)
    qry->penalty = g_abort_penalty;
#if BACKOFF
  else {
    if(qry->penalty * 2 < g_abort_penalty_max)
      qry->penalty = qry->penalty * 2;
  }
#endif

  qry->penalty_end = get_sys_clock() + qry->penalty;
*/
  entry->qry = qry;
  entry->next  = NULL;
  entry->starttime = get_sys_clock();
  assert(qry->rtype <= NO_MSG);
  uint64_t starttime = get_sys_clock();

  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
    if(n->qry->penalty_end < starttime) {
      LIST_REMOVE_HT(n,head,tail);
      INC_STATS(0,txn_time_q_abrt,starttime - n->starttime);
      work_queue.enqueue(0,n->qry,false);
      mem_allocator.free(n,sizeof(struct wq_entry));
      restarts ++;
      n = head;
      continue;
    }
    if(n->qry->penalty_end >= entry->qry->penalty_end)
      break;
    n = n->next;
  }

  if(n) {
    LIST_INSERT_BEFORE(n,entry,head);
  }
  else {
    LIST_PUT_TAIL(head,tail,entry);
  }

  cnt++;

  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
  return restarts;
}

int QWorkQueueHelper::check_abort_query() {

  int restarts = 0;
  uint64_t starttime = get_sys_clock();
  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
    if(n->qry->penalty_end < starttime) {
      LIST_REMOVE_HT(n,head,tail);
      INC_STATS(0,txn_time_q_abrt,starttime - n->starttime);
      work_queue.enqueue(0,n->qry,false);
      restarts++;
      mem_allocator.free(n,sizeof(struct wq_entry));
      n = head;
    } else
      break;
  }
 
  pthread_mutex_unlock(&mtx);
  return restarts;
}

BaseQuery * QWorkQueueHelper::get_next_abort_query() {
  BaseQuery * next_qry = NULL;
  wq_entry_t elem = NULL;

  pthread_mutex_lock(&mtx);

  if(cnt > 0) {
    elem = head;
    assert(elem);
    if(get_sys_clock() > elem->qry->penalty_end) {
      LIST_REMOVE_HT(elem,head,tail);
      //LIST_GET_HEAD(head,tail,elem);
      next_qry = elem->qry;
      if(next_qry)
        next_qry->time_q_abrt += get_sys_clock() - elem->starttime;
      cnt--;

      mem_allocator.free(elem,sizeof(struct wq_entry));
    }

  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,aq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  assert((head && tail && cnt > 0) || (!head && !tail && cnt ==0));


  pthread_mutex_unlock(&mtx);
  return next_qry;
}
