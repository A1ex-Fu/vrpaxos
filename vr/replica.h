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

#ifndef _VR_REPLICA_H_
#define _VR_REPLICA_H_

#include "lib/configuration.h"
#include "lib/latency.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "vr/vr-proto.pb.h"
#include "lib/simtransport.h"


#include <map>
#include <memory>
#include <list>

namespace specpaxos {
namespace vr {

class VRReplica : public Replica
{
public:
    VRReplica(Configuration config, int myIdx, bool initialize,
              Transport *transport, int batchSize,
              AppReplica *app);
    ~VRReplica() noexcept;
    
    void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data);

    size_t GetLogSize();

private:
    view_t view;
    opnum_t lastCommitted;
    opnum_t lastOp;
    view_t lastRequestStateTransferView;
    opnum_t lastRequestStateTransferOpnum;
    uint64_t recoveryNonce;
    std::list<std::pair<TransportAddress *,
                        proto::PrepareMessage> > pendingPrepares;

    proto::PrepareMessage lastPrepare;
    int batchSize;
    opnum_t lastBatchEnd;
    bool batchComplete;
    bool isDelegated;
    std::map<uint32_t, uint32_t> heartbeatCheck;
    opnum_t cleanUpTo;
	std::vector<opnum_t> lastCommitteds; 

    int viewEpoch; //note that the view in this implementation functions more similarly to a ballot

    Log log;
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
        uint64_t lastReqId;
        bool replied;
        proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;
    
    QuorumSet<viewstamp_t, proto::PrepareOKMessage> prepareOKQuorum;
    QuorumSet<view_t, proto::StartViewChangeMessage> startViewChangeQuorum;
    QuorumSet<view_t, proto::DoViewChangeMessage> doViewChangeQuorum;
    QuorumSet<uint64_t, proto::RecoveryResponseMessage> recoveryResponseQuorum;

    Timeout *viewChangeTimeout;
    Timeout *nullCommitTimeout;
    Timeout *stateTransferTimeout;
    Timeout *resendPrepareTimeout;
    Timeout *closeBatchTimeout;
    Timeout *recoveryTimeout;
    Timeout *heartbeatTimeout;

    Latency_t requestLatency;
    Latency_t executeAndReplyLatency;

    uint64_t GenerateNonce() const;
    bool AmLeader() const;
    void CommitUpTo(opnum_t upto);
    void ExecuteLog();
    
    void SendPrepareOKs(opnum_t oldLastOp);
    void SendRecoveryMessages();
    void RequestStateTransfer();
    void EnterView(view_t newview);
    void StartViewChange(view_t newview);
    void SendNullCommit();
    void UpdateClientTable(const Request &req);
    void ResendPrepare();
    void CloseBatch();
    opnum_t GetLowestReplicaCommit();
    void CleanLog();
    void OnHeartbeatTimer();
    
    void HandleRequest(const TransportAddress &remote,
                       const proto::RequestMessage &msg);
    void HandleUnloggedRequest(const TransportAddress &remote,
                               const proto::UnloggedRequestMessage &msg);
    
    void HandlePrepare(const TransportAddress &remote,
                       const proto::PrepareMessage &msg);
    void HandlePrepareOK(const TransportAddress &remote,
                         const proto::PrepareOKMessage &msg);
    void HandleCommit(const TransportAddress &remote,
                      const proto::CommitMessage &msg);
    void HandleRequestStateTransfer(const TransportAddress &remote,
                                    const proto::RequestStateTransferMessage &msg);
    void HandleStateTransfer(const TransportAddress &remote,
                             const proto::StateTransferMessage &msg);
    void HandleStartViewChange(const TransportAddress &remote,
                               const proto::StartViewChangeMessage &msg);
    void HandleDoViewChange(const TransportAddress &remote,
                            const proto::DoViewChangeMessage &msg);
    void HandleStartView(const TransportAddress &remote,
                         const proto::StartViewMessage &msg);
    void HandleRecovery(const TransportAddress &remote,
                        const proto::RecoveryMessage &msg);
    void HandleRecoveryResponse(const TransportAddress &remote,
                        const proto::RecoveryResponseMessage &msg);
    void HandleWitnessDecision(const TransportAddress &remote,
                        const proto::WitnessDecision &msg);
    void HandleHeartbeat(const TransportAddress &remote,
                        const proto::Heartbeat &msg);
    void HandleHeartbeatReply(const TransportAddress &remote,
                        const proto::HeartbeatReply &msg);
    bool SendMessageToAllReplicas(const ::google::protobuf::Message &m);
    bool SendMessageToAll(const ::google::protobuf::Message &m);
    
};

} // namespace specpaxos::vr
} // namespace specpaxos

#endif  /* _VR_REPLICA_H_ */
