# d := $(dir $(lastword $(MAKEFILE_LIST)))

# SRCS += $(addprefix $(d), \
# 	client.cc txnstore.cc server.cc \
# 	lockstore.cc lockserver.cc kvstore.cc \
# 	benchClient.cc versionedKVStore.cc occstore.cc)

# PROTOS += $(addprefix $(d), \
# 	    request.proto)

# LIB-kvstore := $(o)kvstore.o

# LIB-stores := $(o)lockserver.o $(o)versionedKVStore.o

# OBJS-ni-store := $(o)server.o $(o)txnstore.o $(o)lockstore.o $(o)occstore.o

# OBJS-ni-client := $(o)request.o $(o)client.o \
#   $(OBJS-vr-client) $(LIB-udptransport) 

# $(d)benchClient: $(OBJS-ni-client) $(o)benchClient.o

# $(d)replica: $(o)request.o $(OBJS-ni-store) $(LIB-kvstore) $(LIB-stores) \
# 	$(OBJS-vr-replica) $(OBJS-vr-witness) $(LIB-udptransport)

# BINS += $(d)benchClient $(d)replica



NISTORE_OBJS := $(OBJDIR)/nistore/benchClient.o \
                $(OBJDIR)/nistore/client.o

# Link against the VR protocol
NISTORE_OBJS += $(OBJDIR)/vr/client.o

# Add needed lib files
NISTORE_OBJS += $(OBJDIR)/lib/transport.o \
                $(OBJDIR)/lib/replica.o \
                $(OBJDIR)/lib/simtransport.o \
                $(OBJDIR)/lib/configuration.o \
                $(OBJDIR)/lib/message.o \
                $(OBJDIR)/lib/assert.o

$(OBJDIR)/nistore/benchClient: $(NISTORE_OBJS)
	$(LD) $(LDFLAGS) -o $@ $^ $(LIBS)
