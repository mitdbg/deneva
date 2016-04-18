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

#include "row.h"
#include "txn.h"
#include "row_maat.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"
#include "maat.h"

void Row_maat::init(row_t * row) {
	_row = row;

  timestamp_last_read = 0;
  timestamp_last_write = 0;
	pthread_mutex_init(&latch, NULL);
	
}

RC Row_maat::access(access_t type, TxnManager * txn) {
  if(type == RD)
    read(txn);
  if(type == WR)
    prewrite(txn);
  return RCOK;
}

RC Row_maat::read(TxnManager * txn) {
	assert (CC_ALG == MAAT);
	RC rc = RCOK;

  pthread_mutex_lock( &latch );

  // Copy uncommitted writes
  for(auto it = uncommitted_writes.begin(); it != uncommitted_writes.end(); it++) {
    txn->uncommitted_writes.insert(*it);
  }

  // Copy write timestamp
  if(txn->greatest_write_timestamp < timestamp_last_write)
    txn->greatest_write_timestamp = timestamp_last_write;

  //Add to uncommitted reads (soft lock)
  uncommitted_reads.insert(txn->get_txn_id());

  pthread_mutex_unlock( &latch );

	return rc;
}

RC Row_maat::prewrite(TxnManager * txn) {
	assert (CC_ALG == MAAT);
	RC rc = RCOK;

  pthread_mutex_lock( &latch );

  // Copy uncommitted reads 
  for(auto it = uncommitted_reads.begin(); it != uncommitted_reads.end(); it++) {
    txn->uncommitted_reads.insert(*it);
  }

  // Copy uncommitted writes 
  for(auto it = uncommitted_writes.begin(); it != uncommitted_writes.end(); it++) {
    txn->uncommitted_writes_y.insert(*it);
  }

  // Copy read timestamp
  if(txn->greatest_read_timestamp < timestamp_last_read)
    txn->greatest_read_timestamp = timestamp_last_read;

  //Add to uncommitted writes (soft lock)
  uncommitted_writes.insert(txn->get_txn_id());

  pthread_mutex_unlock( &latch );

	return rc;
}


RC Row_maat::abort(access_t type, TxnManager * txn) {	
  pthread_mutex_lock( &latch );
  if(type == RD) {
    uncommitted_reads.erase(txn->get_txn_id());
  }

  if(type == WR) {
    uncommitted_writes.erase(txn->get_txn_id());
  }

  pthread_mutex_unlock( &latch );
  return Abort;
}

RC Row_maat::commit(access_t type, TxnManager * txn, row_t * data) {	
  pthread_mutex_lock( &latch );

  if(type == RD) {
    if(txn->get_commit_timestamp() >  timestamp_last_read)
      timestamp_last_read = txn->get_commit_timestamp();
    uncommitted_reads.erase(txn->get_txn_id());
  }

  if(type == WR) {
    if(txn->get_commit_timestamp() >  timestamp_last_write)
      timestamp_last_write = txn->get_commit_timestamp();
    uncommitted_writes.erase(txn->get_txn_id());
    // Apply write to DB
    write(data);
  }

  pthread_mutex_unlock( &latch );
	return RCOK;
}

void
Row_maat::write(row_t * data) {
	_row->copy(data);
}


