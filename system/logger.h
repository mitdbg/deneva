#ifndef LOGGER_H
#define LOGGER_H

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"
#include <set>
#include <fstream>

enum LogRecType { LRT_INVALID = 0, LRT_INSERT, LRT_UPDATE, LRT_DELETE, LRT_TRUNCATE };
enum LogIUD { L_INSERT = 0, L_UPDATE, L_DELETE };

// Command log record (logical logging)
struct CmdLogRecord {
  uint32_t checksum;
  uint64_t lsn;
  LogRecType type;
  uint64_t txnid; // transaction id
  uint32_t partid; // partition id
#if WORKLOAD==TPCC
  TPCCTxnType txntype;
#elif WORKLOAD==YCSB
  //YCSBTxnType txntype;
#endif
  uint32_t params_size;
  char * params; // input parameters for this transaction type
};

// ARIES-style log record (physiological logging)
struct AriesLogRecord {
  uint32_t checksum;
  uint64_t lsn;
  LogRecType type;
  LogIUD iud;
  uint64_t txnid; // transaction id
  uint32_t partid; // partition id
  uint32_t tableid; // table being updated
  uint64_t key; // primary key
  uint32_t n_cols; //how many columns are being updated
  uint32_t* cols; //ids of modified columns
  uint32_t before_image_size; 
  char * before_image; // data buffer for before image
  uint32_t after_image_size; 
  char * after_image; // data buffer for after image

};

class LogRecord {
public:
  LogRecord();
  LogRecType getType() { return rcd.type; }
  // FIXME: compute a reasonable checksum
  uint64_t computeChecksum() {return (uint64_t)rcd.txnid;};
#if LOG_COMMAND
  CmdLogRecord rcd;
#else
  AriesLogRecord rcd;
#endif
private:
  bool isValid;

};

class Logger {
public:
  ~Logger();
  void init(const char * log_file);
  void flushBufferCheck();
  LogRecord * createRecord(
    LogRecType type,
    LogIUD iud,
    uint64_t txnid,
    uint64_t partid,
    uint64_t table,
    uint64_t key);
  void enqueueRecord(LogRecord* record); 
  void processRecord(); 
  void writeToBuffer(char * data, uint64_t size); 
  void writeToBuffer(LogRecord* record); 
  uint64_t reserveBuffer(uint64_t size); 
private:
  uint64_t lsn;

  void flushBuffer();
  moodycamel::ConcurrentQueue<LogRecord *,moodycamel::ConcurrentQueueDefaultTraits> log_queue;
  const char * log_file_name;
  std::ofstream log_file;
  uint64_t aries_write_offset;
  std::set<uint64_t> txns_to_notify;
  uint64_t last_flush;
  uint64_t log_buf_cnt;
};


#endif
