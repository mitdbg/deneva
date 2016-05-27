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
#include "index_hash.h"
#include "mem_alloc.h"
	
RC IndexHash::init(uint64_t bucket_cnt) {
	_bucket_cnt = bucket_cnt;
	_bucket_cnt_per_part = bucket_cnt;
	//_bucket_cnt_per_part = bucket_cnt / g_part_cnt;
	//_buckets = new BucketHeader * [g_part_cnt];
	_buckets = new BucketHeader * [1];
  _buckets[0] = (BucketHeader *) mem_allocator.alloc(sizeof(BucketHeader) * _bucket_cnt_per_part);
  uint64_t buckets_init_cnt = 0;
  for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
			_buckets[0][n].init();
      ++buckets_init_cnt;
  }
  /*
	for (UInt32 i = 0; i < g_part_cnt; i++) {
		_buckets[i] = (BucketHeader *) mem_allocator.alloc(sizeof(BucketHeader) * _bucket_cnt_per_part);

  // TODO: is this ok for tpcc?
#if WORKLOAD == YCSB
    if((i % g_node_cnt) != g_node_id)
      continue;
#endif

		for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
			_buckets[i][n].init();
      ++buckets_init_cnt;
    }
	}
  */
  printf("Index init with %ld buckets\n",buckets_init_cnt);
	return RCOK;
}

RC 
IndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
	init(bucket_cnt);
	this->table = table;
	return RCOK;
}

bool IndexHash::index_exist(idx_key_t key) {
	assert(false);
}

void 
IndexHash::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void 
IndexHash::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

	
RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);

	cur_bkt->read_item(key, item);
	
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, 
						int part_id, int thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);

	
	cur_bkt->read_item(key, item);
	
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;
}

/************** BucketHeader Operations ******************/

void BucketHeader::init() {
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}

void BucketHeader::insert_item(idx_key_t key, 
		itemid_t * item, 
		int part_id) 
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {		
		BucketNode * new_node = (BucketNode *) 
			mem_allocator.alloc(sizeof(BucketNode));		
		new_node->init(key);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		item->next = cur_node->items;
		cur_node->items = item;
	}
}

void BucketHeader::read_item(idx_key_t key, itemid_t * &item) 
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
	M_ASSERT_V(cur_node != NULL, "Key does not exist! %ld\n",key);
	//M_ASSERT(cur_node != NULL, "Key does not exist!");
	//M_ASSERT(cur_node->key == key, "Key does not exist!");
  assert(cur_node->key == key);
	item = cur_node->items;
}
