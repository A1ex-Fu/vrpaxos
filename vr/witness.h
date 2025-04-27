// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/replica.h:
 *   Viewstamped Replication protocol
 *
 * Copyright 2013-2016 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#ifndef _VR_WITNESS_H_
#define _VR_WITNESS_H_

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/transport.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "vr/vr-proto.pb.h"

#include <map>
#include <memory>
#include <list>

namespace specpaxos {
namespace vr {

class VRWitness : public Replica
{
public:
    VRWitness(Configuration config, int myIdx, bool initialize,
              Transport *transport, AppReplica *app);
    ~VRWitness();
    
    void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data);

    //______________________
    //for testing
    std::string filename = "trace.txt";
    size_t GetLogSize();
    void UpdateTraceFile(std::string filename);



private:

    view_t view;
    opnum_t lastCommitted;
    opnum_t lastOp;

    opnum_t cleanUpTo;
    std::vector<opnum_t> lastCommitteds;

    int viewEpoch;
    
    // Log log;
    std::map<int, std::map<int, int>> clientRequestMap;

    QuorumSet<view_t, proto::StartViewChangeMessage> startViewChangeQuorum;

    Timeout *heartbeatCheckTimeout;
    Timeout *recoveryTimeout;

    Latency_t requestLatency;
    Latency_t executeAndReplyLatency;

    bool printingTraces;
    std::ofstream traceFile;




    void CommitUpTo(opnum_t upto);
    void EnterView(view_t newview);
    void StartViewChange(view_t newview);
    void CleanLog();
    opnum_t GetLowestReplicaCommit();
    void HandleRequest(const TransportAddress &remote,
                       const proto::RequestMessage &msg);
    void HandleStartViewChange(const TransportAddress &remote,
                               const proto::StartViewChangeMessage &msg);
    void HandleStartView(const TransportAddress &remote,
                         const proto::StartViewMessage &msg);
    void HandleChainMessage(const TransportAddress &remote,
                         const proto::ChainMessage &msg);
    void HandleHeartbeat(const TransportAddress &remote,
                         const proto::Heartbeat &msg);
    bool SendMessageToAllReplicas(const ::google::protobuf::Message &m);
    
    void WriteToTrace(const std::string& line);
    std::string getRole(int input);
    void SendAndWrite(const ::google::protobuf::Message &msg, int code);
    void WriteMessageContents(const ::google::protobuf::Message &msg);
    
};

} // namespace specpaxos::vr
} // namespace specpaxos

#endif  /* _VR_WITNESS_H_ */
