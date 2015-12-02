#include "global.h"
#include "helper.h"
#include "txn.h"
#include "occ.h"
#include "specex.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_specex.h"


void SpecEx::init() {
	tnc = 0;
	his_len = 0;
	active_len = 0;
	active = NULL;
	lock_all = false;
}

void SpecEx::clear() {
  his_len = 0;
  history = NULL;
}

RC SpecEx::validate(txn_man * txn) {
	RC rc;
	bool valid = true;
	// SpecEx is centralized. No need to do per partition malloc.
	set_ent * wset;
	set_ent * rset;
	get_rw_set(txn, rset, wset);
	bool readonly = (wset->set_size == 0);
	set_ent * his;

	his = history;
		while (his) {
			valid = test_valid(his, rset);
			if (!valid) 
				goto final;
			his = his->next;
		}

final:
	mem_allocator.free(rset, sizeof(set_ent));

  // Only add write set of aborting txns to history
	if (!readonly && !valid) {
		// only update active & tnc for non-readonly transactions
		pthread_mutex_lock( &latch );
			STACK_PUSH(history, wset);
			his_len ++;
		pthread_mutex_unlock( &latch );
	}
	if (valid) {
		rc = RCOK;
	} else {
		rc = Abort;
	}
	return rc;
}

RC SpecEx::get_rw_set(txn_man * txn, set_ent * &rset, set_ent *& wset) {
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

bool SpecEx::test_valid(set_ent * set1, set_ent * set2) {
	for (UInt32 i = 0; i < set1->set_size; i++)
		for (UInt32 j = 0; j < set2->set_size; j++) {
			if (set1->rows[i] == set2->rows[j]) {
				return false;
			}
		}
	return true;
}
