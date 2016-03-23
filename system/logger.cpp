#include "logger.h"
#include "work_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include <fstream>


void Logger::init(const char * log_file_name) {
  this->log_file_name = log_file_name;
  log_file.open(log_file_name, ios::out | ios::app | ios::binary);
  assert(log_file.is_open());
  pthread_mutex_init(&mtx,NULL);

}

void Logger::release() {
  log_file.close(); 
}

LogRecord * Logger::createRecord( 
    uint64_t txn_id,
    uint64_t table_id,
    uint64_t key
    ) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.lsn = ATOM_FETCH_ADD(lsn,1);
  record->rcd.txn_id = txn_id;
  record->rcd.table_id = table_id;
  record->rcd.key = key;
  return record;
}

void Logger::enqueueRecord(LogRecord* record) {
  DEBUG("Enqueue Log Record %ld\n",record->rcd.txn_id);
  pthread_mutex_lock(&mtx);
  log_queue.push(record); 
  pthread_mutex_unlock(&mtx);
}

void Logger::processRecord() {
  if(log_queue.empty())
    return;
  LogRecord * record = NULL;
  pthread_mutex_lock(&mtx);
  if(!log_queue.empty()) {
    record = log_queue.front(); 
    log_queue.pop();
  }
  pthread_mutex_unlock(&mtx);

  if(record) {
    uint64_t starttime = get_sys_clock();
    DEBUG("Dequeue Log Record %ld\n",record->rcd.txn_id);
    if(record->rcd.iud == L_NOTIFY) {
      flushBuffer();
      work_queue.enqueue(0,Message::create_message(record->rcd.txn_id,LOG_FLUSHED),false);

    }
    writeToBuffer(record);
    //writeToBuffer((char*)(&record->rcd),sizeof(record->rcd));
    log_buf_cnt++;
    mem_allocator.free(record,sizeof(LogRecord));
    INC_STATS(0,log_process_time,get_sys_clock() - starttime);
  }
  
}

uint64_t Logger::reserveBuffer(uint64_t size) {
  return ATOM_FETCH_ADD(aries_write_offset,size);
}


//void Logger::writeToBuffer(char * data, uint64_t offset, uint64_t size) {
void Logger::writeToBuffer(char * data, uint64_t size) {
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
  log_file.write(data,size);
  INC_STATS(0,log_write_time,get_sys_clock() - starttime);

}

void Logger::notify_on_sync(uint64_t txn_id) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.txn_id = txn_id;
  record->rcd.iud = L_NOTIFY;
  enqueueRecord(record);
}

void Logger::writeToBuffer(LogRecord * record) {
  DEBUG("Buffer Write\n");
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
#if LOG_COMMAND

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
#if WORKLOAD == TPCC
  WRITE_VAL(log_file,record->rcd.txntype);
#endif
  WRITE_VAL_SIZE(log_file,record->rcd.params,record->rcd.params_size);

#else

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.iud);
  WRITE_VAL(log_file,record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
  WRITE_VAL(log_file,record->rcd.table_id);
  WRITE_VAL(log_file,record->rcd.key);
  /*
  WRITE_VAL(log_file,record->rcd.n_cols);
  WRITE_VAL(log_file,record->rcd.cols);
  WRITE_VAL_SIZE(log_file,record->rcd.before_image,record->rcd.before_image_size);
  WRITE_VAL_SIZE(log_file,record->rcd.after_image,record->rcd.after_image_size);
  */

#endif
  INC_STATS(0,log_write_time,get_sys_clock() - starttime);

}

void Logger::flushBufferCheck() {
  if(log_buf_cnt >= g_log_buf_max || get_sys_clock() - last_flush > g_log_flush_timeout) {
    flushBuffer();
  }
}

void Logger::flushBuffer() {
  DEBUG("Flush Buffer\n");
  uint64_t starttime = get_sys_clock();
  log_file.flush();
  INC_STATS(0,log_flush_time,get_sys_clock() - starttime);

  last_flush = get_sys_clock();
  log_buf_cnt = 0;
}
