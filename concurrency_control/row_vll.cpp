#include "row.h"
#include "row_vll.h"
#include "global.h"
#include "helper.h"

void 
Row_vll::init(row_t * row) {
	_row = row;
	cs = 0;
	cx = 0;
}

bool 
Row_vll::insert_access(access_t type) {
  if(cs > 0 || cx > 0) {
      INC_STATS(0,cc_busy_cnt,1);
  }
	if (type == RD) {
		cs ++;
		return (cx > 0);
	} else { 
		cx ++;
		return (cx > 1) || (cs > 0);
	}
}

void 
Row_vll::remove_access(access_t type) {
	if (type == RD) {
		assert (cs > 0);
		cs --;
	} else {
		assert (cx > 0);
		cx --;
	}
}
