d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc witness.cc clock.cc)

PROTOS += $(addprefix $(d), \
	    vr-proto.proto)

OBJS-vr-client := $(o)client.o $(o)vr-proto.o $(o)clock.o \
                   $(OBJS-client) $(LIB-message) \
                   $(LIB-configuration)

OBJS-vr-replica := $(o)replica.o $(o)vr-proto.o $(o)clock.o \
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-configuration) $(LIB-latency)

OBJS-vr-witness := $(o)witness.o $(o)vr-proto.o $(o)clock.o \
                   $(OBJS-witness) $(LIB-message) \
                   $(LIB-configuration) $(LIB-latency)

include $(d)tests/Rules.mk
