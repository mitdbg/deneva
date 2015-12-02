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

#ifndef _MEM_ALLOC_H_
#define _MEM_ALLOC_H_

#include "global.h"
#include <map>

const int SizeNum = 4;
const UInt32 BlockSizes[] = {32, 64, 256, 1024};

typedef struct free_block {
    int size;
    struct free_block* next;
} FreeBlock;

class Arena {
public:
	void init(int arena_id, int size);
	void * alloc();
	void free(void * ptr);
private:
	char * 		_buffer;
	int 		_size_in_buffer;
	int 		_arena_id;
	int 		_block_size;
	FreeBlock * _head;
	char 		_pad[128 - sizeof(int)*3 - sizeof(void *)*2 - 8];
};

class mem_alloc {
public:
    void init(uint64_t part_cnt, uint64_t bytes_per_part);
    void register_thread(int thd_id);
    void unregister();
    void * alloc(uint64_t size, uint64_t part_id);
    void * realloc(void * ptr, uint64_t size, uint64_t part_id);
    void free(void * block, uint64_t size);
	int get_arena_id();
private:
    void init_thread_arena();
	int get_size_id(UInt32 size);
	
	// each thread has several arenas for different block size
	Arena ** _arenas;
	int _bucket_cnt;
    std::pair<pthread_t, int>* pid_arena;//                     max_arena_id;
    pthread_mutex_t         map_lock; // only used for pid_to_arena update
};

#endif
