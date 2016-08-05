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
#include "pps.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "pps_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "pps_const.h"

RC PPSWorkload::init() {
	Workload::init();
	char * cpath = getenv("SCHEMA_PATH");
	string path;	
	if (cpath == NULL) 
		path = "./benchmarks/";
	else { 
		path = string(cpath);
	}
	path += "PPS_schema.txt";
	cout << "reading schema file: " << path << endl;
	
  printf("Initializing schema... ");
  fflush(stdout);
	init_schema( path.c_str() );
  printf("Done\n");
  printf("Initializing table... ");
  fflush(stdout);
	init_table();
  printf("Done\n");
  fflush(stdout);
	t_supplies = tables["supplies"];
	return RCOK;
}

RC PPSWorkload::init_schema(const char * schema_file) {
	Workload::init_schema(schema_file);
	t_suppliers = tables["SUPPLIERS"];
	t_products = tables["PRODUCTS"];
	t_parts = tables["PARTS"];
	t_supplies = tables["SUPPLIES"];
	t_uses = tables["USES"];

	i_suppliers = indexes["SUPPLIERS_IDX"];
	i_uses = indexes["USES_IDX"];
	return RCOK;
}

RC PPSWorkload::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
// supplies
// --parts 
// --suppliers
// uses 
// --products 
/**********************************/

	pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
  thr_args * tt = new thr_args[g_init_parallelism];
	for (UInt32 i = 0; i < g_init_parallelism ; i++) {
    tt[i].wl = this;
    tt[i].id = i;
  }
  // Stock table
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
    pthread_create(&p_thds[i], NULL, threadInitStock, &tt[i]);
	}
  threadInitStock(&tt[g_init_parallelism-1]);
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
  }
  printf("STOCK Done\n");
  fflush(stdout);
	printf("\nData Initialization Complete!\n\n");
	return RCOK;
}

RC PPSWorkload::get_txn_man(TxnManager *& txn_manager) {
  DEBUG_M("PPSWorkload::get_txn_man PPSTxnManager alloc\n");
	txn_manager = (PPSTxnManager *)
		mem_allocator.align_alloc( sizeof(PPSTxnManager));
	new(txn_manager) PPSTxnManager();
	//txn_manager->init( this);
	return RCOK;
}

void PPSWorkload::init_tab_suppliers() {
}

// TODO: init other tables

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void * PPSWorkload::threadInitSuppliers(void * This) {
  PPSWorkload * wl = ((thr_args*) This)->wl;
  int id = ((thr_args*) This)->id;
	wl->init_tab_suppliers(id);
	printf("SUPPLIERS Done\n");
	return NULL;
}

