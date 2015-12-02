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

#ifndef ROW_VLL_H
#define ROW_VLL_H

class Row_vll {
public:
	void init(row_t * row);
	// return true   : the access is blocked.
	// return false	 : the access is NOT blocked 
	bool insert_access(access_t type);
	void remove_access(access_t type);
	int get_cs() { return cs; };
private:
	row_t * _row;
    int cs;
    int cx;
};

#endif
