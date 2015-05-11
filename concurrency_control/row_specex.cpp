#include "txn.h"
#include "row.h"
#include "row_specex.h"
#include "mem_alloc.h"

void 
Row_specex::init(row_t * row) {
	_row = row;
	int part_id = row->get_part_id();
	_latch = (pthread_mutex_t *) 
		mem_allocator.alloc(sizeof(pthread_mutex_t), part_id);
	pthread_mutex_init( _latch, NULL );
	wts = 0;
	blatch = false;
}

RC
Row_specex::access(txn_man * txn, TsType type) {
	RC rc = RCOK;
	pthread_mutex_lock( _latch );
	if (type == R_REQ) {
	  txn->cur_row->copy(_row);
		rc = RCOK;
	} else 
		assert(false);
	pthread_mutex_unlock( _latch );
	return rc;
}

void
Row_specex::latch() {
	pthread_mutex_lock( _latch );
}

bool
Row_specex::validate(uint64_t ts) {
	if (ts < wts) return false;
	else return true;
}

void
Row_specex::write(row_t * data, uint64_t ts) {
	_row->copy(data);
	if (PER_ROW_VALID) {
		assert(ts > wts);
		wts = ts;
	}
}

void
Row_specex::release() {
	pthread_mutex_unlock( _latch );
}
