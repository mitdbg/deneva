CC=g++
CFLAGS=-Wall -g -gdwarf-3 -std=c++0x

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./client/ ./concurrency_control/ ./storage/ ./transport/ ./system/
DEPS = -I. -I./benchmarks -I./client/ -I./concurrency_control -I./storage -I./transport -I./system

CFLAGS += $(DEPS) -D NOGRAPHITE=1 -Werror -Wno-sizeof-pointer-memaccess
LDFLAGS = -Wall -L. -L./nanomsg-0.5-beta -pthread -gdwarf-3 -lrt -std=c++0x 
LDFLAGS += $(CFLAGS)
LIBS = -lnanomsg -lanl

DB_MAINS = ./client/client_main.cpp ./system/sequencer.cpp
CL_MAINS = ./system/main.cpp ./system/sequencer.cpp
SQ_MAINS = ./system/main.cpp ./client/client_main.cpp

CPPS_DB = $(foreach dir,$(SRC_DIRS),$(filter-out $(DB_MAINS), $(wildcard $(dir)*.cpp))) 
CPPS_CL = $(foreach dir,$(SRC_DIRS),$(filter-out $(CL_MAINS), $(wildcard $(dir)*.cpp))) 
CPPS_SQ = $(foreach dir,$(SRC_DIRS),$(filter-out $(SQ_MAINS), $(wildcard $(dir)*.cpp))) 

#CPPS = $(wildcard *.cpp)
OBJS_DB = $(addprefix obj/, $(notdir $(CPPS_DB:.cpp=.o)))
OBJS_CL = $(addprefix obj/, $(notdir $(CPPS_CL:.cpp=.o)))
OBJS_SQ = $(addprefix obj/, $(notdir $(CPPS_SQ:.cpp=.o)))

#NOGRAPHITE=1

all:rundb runcl runsq

.PHONY: deps_db
deps:$(CPPS_DB)
	$(CC) $(CFLAGS) -MM $^ > obj/deps
	sed '/^[^ ]/s/^/obj\//g' obj/deps > obj/deps.tmp
	mv obj/deps.tmp obj/deps
-include obj/deps

rundb : $(OBJS_DB)
	$(CC) -static -o $@ $^ $(LDFLAGS) $(LIBS)
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: transport/%.cpp
	$(CC) -static -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<


runcl : $(OBJS_CL)
	$(CC) -static -o $@ $^ $(LDFLAGS) $(LIBS)
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: transport/%.cpp
	$(CC) -static -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<

runsq : $(OBJS_SQ)
	$(CC) -static -o $@ $^ $(LDFLAGS) $(LIBS)
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: transport/%.cpp
	$(CC) -static -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<

.PHONY: clean
clean:
	rm -f obj/*.o obj/.depend rundb runcl runsq
