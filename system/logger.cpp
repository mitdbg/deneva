#include "logger.h"
#include "mem_alloc.h"
#include <fstream>

Logger::~Logger() {
  log_file.close(); 
}

void Logger::init(const char * log_file_name) {
  this->log_file_name = log_file_name;
  log_file.open(log_file_name, ios::out | ios::app | ios::binary);
  assert(log_file.is_open());

}

LogRecord * Logger::createRecord( 
    LogRecType type,
    LogIUD iud,
    uint64_t txnid,
    uint64_t partid,
    uint64_t tableid,
    uint64_t key
    ) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord),0);
  record->rcd.lsn = ATOM_FETCH_ADD(lsn,1);
  record->rcd.type = type;
  record->rcd.iud = iud;
  record->rcd.txnid = txnid;
  record->rcd.partid = partid;
  record->rcd.tableid = tableid;
  record->rcd.key = key;
  record->rcd.before_image_size = 0;
  record->rcd.after_image_size = 0;
  record->rcd.checksum = record->computeChecksum();
  return record;
}

void Logger::enqueueRecord(LogRecord* record) {
  log_queue.enqueue(record); 
}

void Logger::processRecord() {
  LogRecord * record;
  bool r = log_queue.try_dequeue(record); 
  if(r) {
    uint64_t starttime = get_sys_clock();
    writeToBuffer(record);
    //writeToBuffer((char*)(&record->rcd),sizeof(record->rcd));
    // FIXME: Only notify after commit
    txns_to_notify.insert(record->rcd.txnid);
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

void Logger::writeToBuffer(LogRecord * record) {
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
#if LOG_COMMAND

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.txnid);
  WRITE_VAL(log_file,record->rcd.partid);
#if WORKLOAD == TPCC
  WRITE_VAL(log_file,record->rcd.txntype);
#endif
  WRITE_VAL_SIZE(log_file,record->rcd.params,record->rcd.params_size);

#else

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.iud);
  WRITE_VAL(log_file,record->rcd.txnid);
  WRITE_VAL(log_file,record->rcd.partid);
  WRITE_VAL(log_file,record->rcd.tableid);
  WRITE_VAL(log_file,record->rcd.n_cols);
  WRITE_VAL(log_file,record->rcd.cols);
  WRITE_VAL_SIZE(log_file,record->rcd.before_image,record->rcd.before_image_size);
  WRITE_VAL_SIZE(log_file,record->rcd.after_image,record->rcd.after_image_size);

#endif
  INC_STATS(0,log_write_time,get_sys_clock() - starttime);

}

void Logger::flushBufferCheck() {
  if(log_buf_cnt >= g_log_buf_max || get_sys_clock() - last_flush > g_log_flush_timeout) {
    flushBuffer();
  }
}

void Logger::flushBuffer() {
  uint64_t starttime = get_sys_clock();
  log_file.flush();
  INC_STATS(0,log_flush_time,get_sys_clock() - starttime);

  last_flush = get_sys_clock();
  log_buf_cnt = 0;
  while(!txns_to_notify.empty()) {
    // Add notification to queue for *txns_to_notify.begin()
    txns_to_notify.erase(txns_to_notify.begin());
  }
}
