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

#include "global.h"
#include "helper.h"
#include "txn.h"
#include "occ.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"


set_ent::set_ent() {
	set_size = 0;
	txn = NULL;
	rows = NULL;
	next = NULL;
}

void OptCC::init() {
  sem_init(&_semaphore, 0, 1);
	tnc = 0;
	his_len = 0;
	active_len = 0;
	active = NULL;
	lock_all = false;
}

RC OptCC::validate(txn_man * txn) {
	RC rc;
  uint64_t starttime = get_sys_clock();
#if PER_ROW_VALID
	rc = per_row_validate(txn);
#else
	rc = central_validate(txn);
#endif
  INC_STATS(txn->get_thd_id(),thd_prof_occ_val2,get_sys_clock() - starttime);
	return rc;
}

void OptCC::finish(RC rc, txn_man * txn) {
#if PER_ROW_VALID
  per_row_finish(rc,txn);
#else
	central_finish(rc,txn);
#endif
}

RC 
OptCC::per_row_validate(txn_man * txn) {
	RC rc = RCOK;
#if CC_ALG == OCC
	// sort all rows accessed in primary key order.
	// TODO for migration, should first sort by partition id
	for (int i = txn->row_cnt - 1; i > 0; i--) {
		for (int j = 0; j < i; j ++) {
			int tabcmp = strcmp(txn->accesses[j]->orig_row->get_table_name(), 
				txn->accesses[j+1]->orig_row->get_table_name());
			if (tabcmp > 0 || (tabcmp == 0 && txn->accesses[j]->orig_row->get_primary_key() > txn->accesses[j+1]->orig_row->get_primary_key())) {
					Access * tmp = txn->accesses[j]; 
					txn->accesses[j] = txn->accesses[j+1];
					txn->accesses[j+1] = tmp;
			}
		}
	}
#if DEBUG_ASSERT
	for (int i = txn->row_cnt - 1; i > 0; i--) {
		int tabcmp = strcmp(txn->accesses[i-1]->orig_row->get_table_name(), 
			txn->accesses[i]->orig_row->get_table_name());
		assert(tabcmp < 0 || tabcmp == 0 && txn->accesses[i]->orig_row->get_primary_key() > 
			txn->accesses[i-1]->orig_row->get_primary_key());
	}
#endif
	// lock all rows in the readset and writeset.
	// Validate each access
	bool ok = true;
	int lock_cnt = 0;
	for (int i = 0; i < txn->row_cnt && ok; i++) {
		lock_cnt ++;
		txn->accesses[i]->orig_row->manager->latch();
		ok = txn->accesses[i]->orig_row->manager->validate( txn->start_ts );
	}
  rc = ok ? RCOK : Abort;
  /*
	if (ok) {
		// Validation passed.
		// advance the global timestamp and get the end_ts
		txn->end_ts = glob_manager.get_ts( txn->get_thd_id() );
		// write to each row and update wts
		txn->cleanup(RCOK);
		rc = RCOK;
	} else {
		txn->cleanup(Abort);
		rc = Abort;
	}

    */
  /*
	for (int i = 0; i < lock_cnt; i++) 
		txn->accesses[i]->orig_row->manager->release();
    */
#endif
	return rc;
}

RC OptCC::central_validate(txn_man * txn) {
	RC rc;
  uint64_t starttime = get_sys_clock();
  uint64_t total_starttime = starttime;
	uint64_t start_tn = txn->start_ts;
	uint64_t finish_tn;
	//set_ent ** finish_active;
	//set_ent * finish_active[f_active_len];
	uint64_t f_active_len;
	bool valid = true;
	// OptCC is centralized. No need to do per partition malloc.
	set_ent * wset;
	set_ent * rset;
	get_rw_set(txn, rset, wset);
	bool readonly = (wset->set_size == 0);
	set_ent * his;
	set_ent * ent;
	int n = 0;
  int stop __attribute__((unused));

	//pthread_mutex_lock( &latch );
  sem_wait(&_semaphore);
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc1,get_sys_clock() - starttime);
  starttime = get_sys_clock();
	finish_tn = tnc;
	ent = active;
	f_active_len = active_len;
	set_ent * finish_active[f_active_len];
	//finish_active = (set_ent**) mem_allocator.alloc(sizeof(set_ent *) * f_active_len, 0);
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc2,get_sys_clock() - starttime);
  starttime = get_sys_clock();
	while (ent != NULL) {
		finish_active[n++] = ent;
		ent = ent->next;
	}
	if ( !readonly ) {
		active_len ++;
		STACK_PUSH(active, wset);
	}
	his = history;
	//pthread_mutex_unlock( &latch );
  sem_post(&_semaphore);
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc3,get_sys_clock() - starttime);
  starttime = get_sys_clock();

  uint64_t checked = 0;
  stop = 0;
	if (finish_tn > start_tn) {
		while (his && his->tn > finish_tn) 
			his = his->next;
		while (his && his->tn > start_tn) {
      checked++;
			valid = test_valid(his, rset);
			if (!valid) 
				goto final;
			his = his->next;
		}
	}

  INC_STATS(txn->get_thd_id(),thd_prof_mvcc4,get_sys_clock() - starttime);
  starttime = get_sys_clock();
  stop = 1;
	for (UInt32 i = 0; i < f_active_len; i++) {
		set_ent * wact = finish_active[i];
    checked++;
		valid = test_valid(wact, rset);
		if (valid) {
      checked++;
			valid = test_valid(wact, wset);
		} 
    if (!valid)
			goto final;
	}
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc5,get_sys_clock() - starttime);
  starttime = get_sys_clock();
final:
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc6,get_sys_clock() - starttime);
  starttime = get_sys_clock();
  /*
	if (valid) 
		txn->cleanup(RCOK);
    */
	mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
	mem_allocator.free(rset, sizeof(set_ent));
	//mem_allocator.free(finish_active, sizeof(set_ent*)* f_active_len);


	if (valid) {
		rc = RCOK;
    INC_STATS(txn->get_thd_id(),occ_check_cnt,checked);
	} else {
		//txn->cleanup(Abort);
    INC_STATS(txn->get_thd_id(),occ_abort_check_cnt,checked);
		rc = Abort;
    // Optimization: If this is aborting, remove from active set now
      sem_wait(&_semaphore);
        set_ent * act = active;
        set_ent * prev = NULL;
        while (act != NULL && act->txn != txn) {
          prev = act;
          act = act->next;
        }
        if(act != NULL && act->txn == txn) {
          if (prev != NULL)
            prev->next = act->next;
          else
            active = act->next;
          active_len --;
        }
      sem_post(&_semaphore);
	}
  INC_STATS(txn->get_thd_id(),thd_prof_occ_val1,get_sys_clock() - total_starttime);
	return rc;
}

void OptCC::per_row_finish(RC rc, txn_man * txn) {
  if(rc == RCOK) {
		// advance the global timestamp and get the end_ts
		txn->end_ts = glob_manager.get_ts( txn->get_thd_id() );
  }
}

void OptCC::central_finish(RC rc, txn_man * txn) {
	set_ent * wset;
	set_ent * rset;
	get_rw_set(txn, rset, wset);
	bool readonly = (wset->set_size == 0);

	if (!readonly) {
		// only update active & tnc for non-readonly transactions
  uint64_t starttime = get_sys_clock();
//		pthread_mutex_lock( &latch );
  sem_wait(&_semaphore);
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc7,get_sys_clock() - starttime);
  starttime = get_sys_clock();
		set_ent * act = active;
		set_ent * prev = NULL;
		while (act != NULL && act->txn != txn) {
			prev = act;
			act = act->next;
		}
    if(act == NULL) {
      assert(rc == Abort);
		  //pthread_mutex_unlock( &latch );
      sem_post(&_semaphore);
      INC_STATS(txn->get_thd_id(),thd_prof_mvcc8,get_sys_clock() - starttime);
      return;
    }
		assert(act->txn == txn);
		if (prev != NULL)
			prev->next = act->next;
		else
			active = act->next;
		active_len --;
		if (rc == RCOK) {
			// TODO remove the assert for performance
			if (history)
				assert(history->tn == tnc);
			tnc ++;
			wset->tn = tnc;
			STACK_PUSH(history, wset);
			his_len ++;
      //mem_allocator.free(wset->rows, sizeof(row_t *) * wset->set_size);
      //mem_allocator.free(wset, sizeof(set_ent));
		}
	//	pthread_mutex_unlock( &latch );
  sem_post(&_semaphore);
  INC_STATS(txn->get_thd_id(),thd_prof_mvcc9,get_sys_clock() - starttime);
	}
}

RC OptCC::get_rw_set(txn_man * txn, set_ent * &rset, set_ent *& wset) {
	wset = (set_ent*) mem_allocator.alloc(sizeof(set_ent), 0);
	rset = (set_ent*) mem_allocator.alloc(sizeof(set_ent), 0);
	wset->set_size = txn->wr_cnt;
	rset->set_size = txn->row_cnt - txn->wr_cnt;
	wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size, 0);
	rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size, 0);
	wset->txn = txn;
	rset->txn = txn;

	UInt32 n = 0, m = 0;
	for (int i = 0; i < txn->row_cnt; i++) {
		if (txn->accesses[i]->type == WR)
			wset->rows[n ++] = txn->accesses[i]->orig_row;
		else 
			rset->rows[m ++] = txn->accesses[i]->orig_row;
	}

	assert(n == wset->set_size);
	assert(m == rset->set_size);
	return RCOK;
}

bool OptCC::test_valid(set_ent * set1, set_ent * set2) {
	for (UInt32 i = 0; i < set1->set_size; i++)
		for (UInt32 j = 0; j < set2->set_size; j++) {
			if (set1->rows[i] == set2->rows[j]) {
				return false;
			}
		}
	return true;
}
