# d := $(dir $(lastword $(MAKEFILE_LIST)))

# SRCS += $(addprefix $(d), \
# 	client.cc benchmark.cc replica.cc) $(vrpaxos/lib/simtransport.cc)

# OBJS-benchmark := $(o)benchmark.o \
#                   $(LIB-message) $(LIB-latency) \
#                   $(o)simtransport.o

# $(d)client: $(o)client.o  $(OBJS-vr-client)  $(OBJS-benchmark) $(LIB-udptransport)

# $(d)replica: $(o)replica.o $(OBJS-vr-replica)  $(LIB-udptransport)

# BINS += $(d)client $(d)replica





d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc benchmark.cc replica.cc)

OBJS-benchmark := $(o)benchmark.o \
                  $(LIB-message) $(LIB-latency)

$(d)client: $(o)client.o $(OBJS-vr-client) $(OBJS-benchmark) $(LIB-udptransport) $(LIB-simtransport)  $(OBJS-witness) $(OBJS-replica) 

$(d)replica: $(o)replica.o  $(OBJS-vr-replica) $(OBJS-vr-witness) $(LIB-udptransport) $(LIB-simtransport) $(OBJS-witness) $(OBJS-replica) 

BINS += $(d)client $(d)replica


