OPT = -g # debugging
#OPT += -O0 # debugging
#OPT += -O3 # production
#PROF = -lprofiler

CXX = g++

# source code directory
SOURCEDIR = ./src

GTEST_DIR = /opt/gtest-1.7.0
TCMALLOC_DIR = /urs/local/lib
TCMALLOC_LIBDIR = /usr/local/lib
TCMALLOC_EXTRA_FLAGS = -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
BOOST_INCLUDE_DIR = /usr/local/include/boost
BOOST_LIB_DIR=/usr/local/lib

# Flags passed to the compiler

########  CHECK IF PG IS ENABLED  ##########

CFLAGS = $(OPT)  -std=c++11  -w -Wall -Wextra -I $(BOOST_INCLUDE_DIR) -I $(GTEST_DIR)/include -I $(SOURCEDIR) -D_GLIBCXX_USE_NANOSLEEP -DLOCALITY -DSIXTY_FOUR_BIT_CLOCK -DNO_LOCAL_WAIT -DLOCAL_LINEARIZABLE -DUSE_GSV_AS_DV  -DUSE_ASSERT -DMEASURE_STATISTICS -DDEP_VECTORS -DPARALLEL_XACTS

LDFLAGS = -z muldefs -static-libgcc -static-libstdc++ -Bstatic -l:$(BOOST_LIB_DIR)/libboost_system.a -l:$(BOOST_LIB_DIR)/libboost_thread.a -l:$(BOOST_LIB_DIR)/libboost_date_time.a -L:$(BOOST_LIB_DIR) -l:$(BOOST_LIB_DIR)/libprotobuf.a -lpthread -lrt ${PROF}
   
# built file directory
PROJECTDIR = /pocc
BUILDDIR = $(PROJECTDIR)/build
$(shell mkdir -p $(BUILDDIR))

# protocol buffer
PROTOBUFDIR = ./src/messages
pb_middleman = $(PROTOBUFDIR)/pb_middleman
PROTOBUFS = $(PROTOBUFDIR)/op_log_entry.proto \
	    $(PROTOBUFDIR)/rpc_messages.proto

# objects
LIBS = $(SOURCEDIR)/messages/op_log_entry.pb.o \
       $(SOURCEDIR)/messages/rpc_messages.pb.o \
       $(SOURCEDIR)/kvservice/parallel_xact.o \
       $(SOURCEDIR)/kvservice/coordinator.o \
       $(SOURCEDIR)/kvservice/kv_server.o \
       $(SOURCEDIR)/kvstore/mv_kvstore.o \
       $(SOURCEDIR)/common/sys_config.o \
       $(SOURCEDIR)/common/sys_stats.o \
       $(SOURCEDIR)/common/sys_logger.o \
       $(SOURCEDIR)/common/generators.o \
       $(SOURCEDIR)/kvstore/item_anchor.o \
       $(SOURCEDIR)/kvstore/log_manager.o \
       $(SOURCEDIR)/rpc/socket.o \
       $(SOURCEDIR)/rpc/message_channel.o \
       $(SOURCEDIR)/rpc/async_rpc_client.o \
       $(SOURCEDIR)/rpc/sync_rpc_client.o \
       $(SOURCEDIR)/rpc/rpc_server.o \
       $(SOURCEDIR)/kvservice/public_kv_client.o \
       $(SOURCEDIR)/kvservice/public_kv_client_lb.o \
       $(SOURCEDIR)/kvservice/replication_kv_client.o \
       $(SOURCEDIR)/kvservice/partition_kv_client.o \
       $(SOURCEDIR)/kvservice/experiment.o \
       $(SOURCEDIR)/groupservice/group_server.o \
       $(SOURCEDIR)/groupservice/group_client.o \
       $(SOURCEDIR)/kvservice/run_experiments.o \

# programs
PROGRAMS = $(BUILDDIR)/kv_server_program \
	   $(BUILDDIR)/interactive_kv_client_program \
	   $(BUILDDIR)/group_server_program \
	   $(BUILDDIR)/run_experiments \

# build targets
all: $(pb_middleman) $(LIBS) $(PROGRAMS)

clean:
	rm -rf $(BUILDDIR)/*
	rm -rf */*/*.o
	rm -rf $(pb_middleman) $(PROTOBUFDIR)/*.pb.cc $(PROTOBUFDIR)/*.pb.h

clean-bin:
	rm -rf $(BUILDDIR)/*


# protobufs
$(pb_middleman) : $(PROTOBUFS)
	protoc -I=$(PROTOBUFDIR) --cpp_out=$(PROTOBUFDIR) $^
	@touch $@

# objects
%.o : %.cc
	$(CXX) -c $(CFLAGS) $^ -o $@

# targets
$(BUILDDIR)/kv_server_program: $(SOURCEDIR)/kvservice/kv_server_program.o $(LIBS)
	$(CXX) $^ -o $@ $(LDFLAGS)
$(BUILDDIR)/interactive_kv_client_program: $(SOURCEDIR)/kvservice/interactive_kv_client_program.o $(LIBS)
	$(CXX) $^ -o $@ $(LDFLAGS)
$(BUILDDIR)/run_experiments: $(SOURCEDIR)/kvservice/run_experiments.o $(LIBS)
	$(CXX) $^ -o $@ $(LDFLAGS)
$(BUILDDIR)/group_server_program: $(SOURCEDIR)/groupservice/group_server_program.o $(LIBS)
	$(CXX) $^ -o $@ $(LDFLAGS)
$(BUILDDIR)/generators: $(SOURCEDIR)/common/generators.o $(LIBS)
	$(CXX) $^ -o $@ $(LDFLAGS)
