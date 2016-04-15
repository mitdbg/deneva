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

void Row_maat::init(row_t * row) {
	_row = row;

  timestamp_last_read = 0;
  timestamp_last_write = 0;
	pthread_mutex_init(&latch, NULL);
	
}

RC Row_maat::read(TxnManager * txn) {
	assert (CC_ALG == MAAT);
	RC rc;

  pthread_mutex_lock( &latch );

  //Add to uncommitted reads

  // Copy uncommitted writes

  // Copy write timestamp

  pthread_mutex_unlock( &latch );

	return rc;
}

RC Row_maat::prewrite(TxnManager * txn) {
	assert (CC_ALG == MAAT);
	RC rc;

  pthread_mutex_lock( &latch );

  //Add to uncommitted writes

  // Copy uncommitted reads 

  // Copy read timestamp

  // Copy uncommitted writes 

  pthread_mutex_unlock( &latch );

	return rc;
}



RC Row_maat::commit(TxnManager * txn) {	
  /*
  if(time_table.get_commit_timestamp(txn->get_txn_id()) >  timestamp_last_read)
    timestamp_last_read = time_table.get_commit_timestamp(txn->get_txn_id());
  if(time_table.get_commit_timestamp(txn->get_txn_id()) >  timestamp_last_write)
    timestamp_last_write = time_table.get_commit_timestamp(txn->get_txn_id());
    */
  release(txn);
	return RCOK;
}

RC Row_maat::release(TxnManager * txn) {	

  pthread_mutex_lock( &latch );

  pthread_mutex_unlock( &latch );

	return RCOK;
}


