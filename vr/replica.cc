// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/replica.cc:
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

 #include "common/replica.h"
 #include "vr/replica.h"
 #include "vr/vr-proto.pb.h"
 
 #include "lib/assert.h"
 #include "lib/configuration.h"
 #include "lib/latency.h"
 #include "lib/message.h"
 #include "lib/transport.h"
 #include "lib/simtransport.h"
 #include <unordered_map>

 
 #include <algorithm>
 #include <random>
 #include "clock.h"
using namespace Clock; 

#define RDebug(fmt, ...) Debug("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, myIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace vr {

using namespace proto;
    
VRReplica::VRReplica(Configuration config, int myIdx,
                     bool initialize,
                     Transport *transport, int batchSize,
                     AppReplica *app)
    : Replica(config, myIdx, initialize, transport, app),
      batchSize(batchSize),
      lastCommitteds(config.n, 0),
      log(false),
      prepareOKQuorum(config.QuorumSize()-1),
      startViewChangeQuorum(config.QuorumSize()-1),
      doViewChangeQuorum(config.QuorumSize()-1),
      recoveryResponseQuorum(config.QuorumSize())
{


    this->status = STATUS_NORMAL;
    this->view = 0;
    this->lastOp = 0;
    this->lastCommitted = 0;
    this->lastRequestStateTransferView = 0;
    this->lastRequestStateTransferOpnum = 0;

    //init to 0 because always starts in a view
    this->viewEpoch = 0;


    lastBatchEnd = 0;
    batchComplete = true;
    isDelegated = (config.n>1);
    Notice("\n\n\nDELEGATED\n\n\n");
    //change for replica only TESTING
    // isDelegated = false;
    startRequestToDecision = -1;

    this->cleanUpTo = 0;

    if (batchSize > 1) {
        // Notice("Batching enabled; batch size %d", batchSize);
    }

    this->viewChangeTimeout = new Timeout(transport, 10000, [this,myIdx,config]() {
            // RWarning("Have not heard from leader; starting view change");
			view_t step = 1;
			while (IsWitness(configuration.GetLeaderIndex(view + step))) {
				step++; 
			}
        
            StartViewChange(view + step);
        });
    this->nullCommitTimeout = new Timeout(transport, 1000, [this]() {
            SendNullCommit();
        });
    this->stateTransferTimeout = new Timeout(transport, 1000, [this]() {
            this->lastRequestStateTransferView = 0;
            this->lastRequestStateTransferOpnum = 0;            
        });
    this->stateTransferTimeout->Start();
    this->resendPrepareTimeout = new Timeout(transport, 500, [this]() {
            if(!isDelegated){
                ResendPrepare();
            }
        });
    this->closeBatchTimeout = new Timeout(transport, 300, [this]() {
            CloseBatch();
        });
    this->recoveryTimeout = new Timeout(transport, 5000, [this]() {
            SendRecoveryMessages();
        });

    this->heartbeatTimeout = new Timeout(transport, 2500, [this]() {
            // Warning("heartbeat timeout triggered");
            OnHeartbeatTimer();
        });

    _Latency_Init(&requestLatency, "request");
    _Latency_Init(&executeAndReplyLatency, "executeAndReply");
    
    if (initialize) {
        if (AmLeader()) {
            nullCommitTimeout->Start();
            heartbeatTimeout->Start();

            for (uint32_t i = 0; i < configuration.n; ++i) {
                heartbeatCheck[i] = 0;
            }
        } else {
            viewChangeTimeout->Start();
        }        
    } else {
        Notice("init was false\n\n\n");
        this->status = STATUS_RECOVERING;
        this->recoveryNonce = GenerateNonce();
        SendRecoveryMessages();
        recoveryTimeout->Start();
    }
}

VRReplica::~VRReplica()
{
    Latency_Dump(&requestLatency);
    Latency_Dump(&executeAndReplyLatency);

    delete viewChangeTimeout;
    delete nullCommitTimeout;
    delete stateTransferTimeout;
    delete resendPrepareTimeout;
    delete closeBatchTimeout;
    delete recoveryTimeout;
    delete heartbeatTimeout;
    
    for (auto &kv : pendingPrepares) {
        delete kv.first;
    }
}

uint64_t
VRReplica::GenerateNonce() const
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);    
}

bool
VRReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == myIdx);
}

void
VRReplica::CommitUpTo(opnum_t upto)
{

    while (lastCommitted < upto) {
        Latency_Start(&executeAndReplyLatency);
        
        lastCommitted++;

        /* Find operation in log */
        const LogEntry *entry = log.Find(lastCommitted);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", lastCommitted);
        }

        /* Execute it */
        RDebug("Executing request " FMT_OPNUM, lastCommitted);
        ReplyMessage reply;
        Execute(lastCommitted, entry->request, reply);

        reply.set_view(entry->viewstamp.view);
        reply.set_opnum(entry->viewstamp.opnum);
        reply.set_clientreqid(entry->request.clientreqid());
        reply.set_replicaidx(myIdx);
        reply.set_n(configuration.n);
        
        /* Mark it as committed */
        log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);

        // Store reply in the client table
        ClientTableEntry &cte =
            clientTable[entry->request.clientid()];
        if (cte.lastReqId <= entry->request.clientreqid()) {
            cte.lastReqId = entry->request.clientreqid();
            cte.replied = true;
            cte.reply = reply;
        } else {
            // We've subsequently prepared another operation from the
            // same client. So this request must have been completed
            // at the client, and there's no need to record the
            // result.
        }
        
        auto iter = clientAddresses.find(entry->request.clientid());
        if (iter != clientAddresses.end()) {
            const auto &recipient = *iter->second;
            // Notice("SENDING REPLY/ACK to %s", recipient.ToString().c_str());
        
            if (AmLeader() || !isDelegated) {
                transport->SendMessage(this, recipient, reply);
            } else {
                PaxosAck paxosAck;
                paxosAck.set_clientreqid(entry->request.clientreqid());
                paxosAck.set_replicaidx(myIdx);
                paxosAck.set_n(configuration.n);
        
                transport->SendMessage(this, recipient, paxosAck);
            }
            // logCycleMeasurement(("LastRDToReply" + std::to_string(myIdx)), startLastRDtoReply, rdtsc_clock());
        }
        

        Latency_End(&executeAndReplyLatency);
    }
}



void
VRReplica::ExecuteLog()
{
    while (lastCommitted <= log.LastOpnum()) {
        Latency_Start(&executeAndReplyLatency);
    
        /* Find operation in log */
        const LogEntry *entry = log.Find(lastCommitted);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", lastCommitted);
        }
        lastCommitted++;
        
        /* want to loop through only the committed ops - mocking execute() func from java code*/
        bool wasChanged = log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);
        if(wasChanged){
            log.SetStatus(lastCommitted, LOG_STATE_PREPARED);
            break;
        }

        /* Execute it (if it was already committed)*/
        RDebug("Executing request " FMT_OPNUM, lastCommitted);
        ReplyMessage reply;
        Execute(lastCommitted, entry->request, reply);

        reply.set_view(entry->viewstamp.view);
        reply.set_opnum(entry->viewstamp.opnum);
        reply.set_clientreqid(entry->request.clientreqid());
        reply.set_replicaidx(myIdx);
        reply.set_n(configuration.n);

        // Store reply in the client table
        ClientTableEntry &cte =
            clientTable[entry->request.clientid()];
        if (cte.lastReqId <= entry->request.clientreqid()) {
            cte.lastReqId = entry->request.clientreqid();
            cte.replied = true;
            cte.reply = reply;            
        } else {
            // We've subsequently prepared another operation from the
            // same client. So this request must have been completed
            // at the client, and there's no need to record the
            // result.
        }
        
        if(AmLeader()){
            /* Send reply */
            RPanic("actually replying");
            auto iter = clientAddresses.find(entry->request.clientid());
            if (iter != clientAddresses.end()) {
                transport->SendMessage(this, *iter->second, reply);
            }
            Notice("%d sending reply from 2 and isleader=%d", myIdx, AmLeader());
        }

        Latency_End(&executeAndReplyLatency);
    }
}




void
VRReplica::OnHeartbeatTimer()
{
    
    int heartbeatMissThreshold = 10;
    for(auto& pair : heartbeatCheck) {
        if(pair.first == myIdx){
            continue;
        }

        if(pair.second >= heartbeatMissThreshold){
            //node is dead
            if (specpaxos::IsWitness(pair.first) && isDelegated){
                // Warning("[%d] turned off delegation", myIdx);
                isDelegated = false;
                Notice("\n\n\nNOT DELEGATED\n\n\n");
            }else{
                //TODO - idle node --> replica node
                Notice("%d is unresponsive", pair.first);
            }
        }else if (pair.second < heartbeatMissThreshold){
            pair.second++;

            //send them a hb message to make sure they are responsive
            Heartbeat m;
            m.set_view(view);
            m.set_slotexecuted(GetLowestReplicaCommit());

            m.set_cleanupto(GetLowestReplicaCommit()); 
            if (m.cleanupto() > cleanUpTo) {
                // Clean log up to the lowest committed entry by any replica
                cleanUpTo = m.cleanupto();
                CleanLog(); 
            } else if (m.cleanupto() < cleanUpTo) {
                RPanic("cleanUpTo decreased! Got " FMT_OPNUM ", had " FMT_OPNUM, 
                        m.cleanupto(), cleanUpTo);
            }
            
            if(pair.first >= configuration.n){
                int x = pair.first;
                int y = configuration.n;
            }else{
                int x = pair.first;
                int y = configuration.n;
            }
            // RNotice("Sending heartbeat to %d who has %d misses", pair.first, pair.second);
            if (!transport->SendMessageToReplica(this, pair.first, m)) {
                RWarning("Failed to send heartbeat message to all replicas");
            }
        }
    }

    heartbeatTimeout->Reset();
}

void
VRReplica::SendPrepareOKs(opnum_t oldLastOp)
{

    /* Send PREPAREOKs for new uncommitted operations */
    for (opnum_t i = oldLastOp; i <= lastOp; i++) {

        /* It has to be new *and* uncommitted */
        if (i <= lastCommitted) {
            continue;
        }

        const LogEntry *entry = log.Find(i);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", i);
        }
        ASSERT(entry->state == LOG_STATE_PREPARED);
        UpdateClientTable(entry->request);

        PrepareOKMessage reply;
        reply.set_view(view);
        reply.set_opnum(i);
        reply.set_replicaidx(myIdx);
        reply.set_lastcommitted(lastCommitted);

        RDebug("Sending PREPAREOK " FMT_VIEWSTAMP " for new uncommitted operation",
               reply.view(), reply.opnum());
    
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              reply))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
    }
}

void
VRReplica::SendRecoveryMessages()
{
    RecoveryMessage m;
    m.set_replicaidx(myIdx);
    m.set_nonce(recoveryNonce);
    
    RNotice("Requesting recovery");
    if (!transport->SendMessageToAll(this, m)) {
        RWarning("Failed to send Recovery message to all replicas");
    }
}

void
VRReplica::RequestStateTransfer()
{
    RequestStateTransferMessage m;
    m.set_view(view);
    m.set_opnum(lastCommitted);

    if ((lastRequestStateTransferOpnum != 0) &&
        (lastRequestStateTransferView == view) &&
        (lastRequestStateTransferOpnum == lastCommitted)) {
        RDebug("Skipping state transfer request " FMT_VIEWSTAMP
               " because we already requested it", view, lastCommitted);
        return;
    }
    
    // RNotice("Requesting state transfer: " FMT_VIEWSTAMP, view, lastCommitted);

    this->lastRequestStateTransferView = view;
    this->lastRequestStateTransferOpnum = lastCommitted;

    if (!SendMessageToAllReplicas(m)) {
        RWarning("Failed to send RequestStateTransfer message to all replicas");
    }
}

void
VRReplica::EnterView(view_t newview)
{
    RNotice("Entering new view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_NORMAL;
    lastBatchEnd = lastOp;
    batchComplete = true;


    this->viewEpoch++;

    recoveryTimeout->Stop();

    if (AmLeader()) {
        viewChangeTimeout->Stop();
        nullCommitTimeout->Start();
        heartbeatTimeout->Start();
        for (uint32_t i = 0; i < configuration.n; ++i) {
            heartbeatCheck[i] = 0;
        }

    } else {
        viewChangeTimeout->Start();
        nullCommitTimeout->Stop();
        resendPrepareTimeout->Stop();
        closeBatchTimeout->Stop();
    }
    

    prepareOKQuorum.Clear();
    startViewChangeQuorum.Clear();
    doViewChangeQuorum.Clear();
    recoveryResponseQuorum.Clear();
}

void
VRReplica::StartViewChange(view_t newview)
{
    RNotice("Starting view change for view " FMT_VIEW, newview);

    view = newview;

    status = STATUS_VIEW_CHANGE;

    viewChangeTimeout->Reset();
    nullCommitTimeout->Stop();
    resendPrepareTimeout->Stop();
    closeBatchTimeout->Stop();

    StartViewChangeMessage m;
    m.set_view(newview);
    m.set_replicaidx(myIdx);
    m.set_lastcommitted(lastCommitted);
    m.set_viewepoch(viewEpoch);

    if (!SendMessageToAll(m)) {
        RWarning("Failed to send StartViewChange message to all nodes");
    }
}





void
VRReplica::SendNullCommit()
{
    // Notice("Sending null commit");
    CommitMessage cm;
    cm.set_view(this->view);
    cm.set_opnum(this->lastCommitted);

    ASSERT(AmLeader());
    
    if (!(SendMessageToAllReplicas(cm))) {
        RWarning("Failed to send null COMMIT message to all replicas");
    }
}

void
VRReplica::UpdateClientTable(const Request &req)
{
    ClientTableEntry &entry = clientTable[req.clientid()];

    // ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId >= req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
}

void
VRReplica::ResendPrepare()
{
    ASSERT(AmLeader());
    if (lastOp == lastCommitted) {
        return;
    }
    RNotice("Resending prepare");
    if(isDelegated && AmLeader()){
        Warning("Did NOT resend prepare because the leader is delegated");
        return;
    }
    if (!(SendMessageToAllReplicas(lastPrepare))) {
        RWarning("Failed to ressend prepare message to all replicas");
    }else{
        // RWarning("reset heartbeattimeout in resendprepare");
        
    }
}

void
VRReplica::CloseBatch()
{
    ASSERT(AmLeader());
    ASSERT(lastBatchEnd < lastOp);

    opnum_t batchStart = lastBatchEnd+1;
    
    RDebug("Sending batched prepare from " FMT_OPNUM
           " to " FMT_OPNUM,
           batchStart, lastOp);
    /* Send prepare messages */
    PrepareMessage p;
    p.set_view(view);
    p.set_opnum(lastOp);
    p.set_batchstart(batchStart);


    for (opnum_t i = batchStart; i <= lastOp; i++) {
        Request *r = p.add_request();
        const LogEntry *entry = log.Find(i);
        ASSERT(entry != NULL);
        ASSERT(entry->viewstamp.view == view);
        ASSERT(entry->viewstamp.opnum == i);
        *r = entry->request;
    }

    lastPrepare = p;
    if(!isDelegated) {
        // Warning("send prepares");
        if (!(SendMessageToAllReplicas(p))) {
            RWarning("Failed to send prepare message to all replicas");
        }else{
            // RWarning("reset heartbeattimeout in closebatch");
            
        }
    }
    lastBatchEnd = lastOp;
    batchComplete = false;
    
    resendPrepareTimeout->Reset();
    closeBatchTimeout->Stop();

}






void
VRReplica::ReceiveMessage(const TransportAddress &remote,
                          const string &type, const string &data)
{
    static RequestMessage request;
    static UnloggedRequestMessage unloggedRequest;
    static PrepareMessage prepare;
    static PrepareOKMessage prepareOK;
    static CommitMessage commit;
    static RequestStateTransferMessage requestStateTransfer;
    static StateTransferMessage stateTransfer;
    static StartViewChangeMessage startViewChange;
    static DoViewChangeMessage doViewChange;
    static StartViewMessage startView;
    static RecoveryMessage recovery;
    static RecoveryResponseMessage recoveryResponse;
    static WitnessDecision witnessDecision;
    static Heartbeat heartbeat;
    static HeartbeatReply heartbeatReply;



    // const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    // int srcAddr = simRemote.GetAddr();

    // Notice("%d Received %s message in VR Replica from %s", myIdx, type.c_str(), remote.ToString().c_str());

    // //made the assumption that messages received from servers with a replica index < configuration.n are replicas/witnesses
    // // if this assumption is false, we must find some way to determine if a sender is a replica or a client
    // if(AmLeader() && (srcAddr<configuration.n)){
    //     heartbeatCheck[srcAddr] = 0;
    //     // RWarning("created heartbeatcheck entry for %d", srcAddr);
    // }


    if (type == request.GetTypeName()) {
        request.ParseFromString(data);
        HandleRequest(remote, request);
    } else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnloggedRequest(remote, unloggedRequest);
    } else if (type == prepare.GetTypeName()) {
        prepare.ParseFromString(data);
        HandlePrepare(remote, prepare);
    } else if (type == prepareOK.GetTypeName()) {
        prepareOK.ParseFromString(data);
        HandlePrepareOK(remote, prepareOK);
    } else if (type == commit.GetTypeName()) {
        commit.ParseFromString(data);
        HandleCommit(remote, commit);
    } else if (type == requestStateTransfer.GetTypeName()) {
        requestStateTransfer.ParseFromString(data);
        HandleRequestStateTransfer(remote, requestStateTransfer);
    } else if (type == stateTransfer.GetTypeName()) {
        stateTransfer.ParseFromString(data);
        HandleStateTransfer(remote, stateTransfer);
    } else if (type == startViewChange.GetTypeName()) {
        startViewChange.ParseFromString(data);
        HandleStartViewChange(remote, startViewChange);
    } else if (type == doViewChange.GetTypeName()) {
        doViewChange.ParseFromString(data);
        HandleDoViewChange(remote, doViewChange);
    } else if (type == startView.GetTypeName()) {
        startView.ParseFromString(data);
        HandleStartView(remote, startView);
    } else if (type == recovery.GetTypeName()) {
        recovery.ParseFromString(data);
        HandleRecovery(remote, recovery);
    } else if (type == recoveryResponse.GetTypeName()) {
        recoveryResponse.ParseFromString(data);
        HandleRecoveryResponse(remote, recoveryResponse);
    } else if (type == witnessDecision.GetTypeName()) {
        witnessDecision.ParseFromString(data);
        HandleWitnessDecision(remote, witnessDecision);
    } else if (type == heartbeat.GetTypeName()) {
        heartbeat.ParseFromString(data);
        HandleHeartbeat(remote, heartbeat);
    } else if (type == heartbeatReply.GetTypeName()) {
        heartbeatReply.ParseFromString(data);
        HandleHeartbeatReply(remote, heartbeatReply);
    } else {
        RPanic("Received unexpected message type in VR replica: %s",
              type.c_str());
    }
}

void
VRReplica::HandleRequest(const TransportAddress &remote,
                         const RequestMessage &msg)
{
    // Notice("got request %d from client %d", msg.req().clientreqid(), msg.req().clientid());
    
    viewstamp_t v;
    Latency_Start(&requestLatency);
    
    if (status != STATUS_NORMAL) {
        RNotice("Ignoring request due to abnormal status");
        return;
    }

    // Save the client's address
    clientAddresses.erase(msg.req().clientid());
    clientAddresses.insert(
        std::pair<uint64_t, std::unique_ptr<TransportAddress> >(
            msg.req().clientid(),
            std::unique_ptr<TransportAddress>(remote.clone())));

    // Check the client table to see if this is a duplicate request
    auto kv = clientTable.find(msg.req().clientid());
    if (kv != clientTable.end()) {
        const ClientTableEntry &entry = kv->second;
        if (msg.req().clientreqid() < entry.lastReqId) {
            RNotice("Ignoring stale request");
            return;
        }
        if (msg.req().clientreqid() == entry.lastReqId) {
            // This is a duplicate request. Resend the reply if we
            // have one. We might not have a reply to resend if we're
            // waiting for the other replicas; in that case, just
            // discard the request.
            if (!AmLeader()) {
                RDebug("Non-leader replica has duplicate request");
                PaxosAck paxosAck;
                paxosAck.set_clientreqid(msg.req().clientreqid());
                paxosAck.set_replicaidx(myIdx);
                // Request *r = paxosAck.add_req();
                // *r = msg.req();
                // *paxosAck.mutable_req() = msg.req();
                paxosAck.set_n(configuration.n);
                if (!(transport->SendMessage(this, remote, paxosAck)))
                    Warning("Failed to send paxosAck message");
                return;        
            }

            if (entry.replied) {
                // RNotice("Received duplicate request; resending reply");
                if (!(transport->SendMessage(this, remote,
                                             entry.reply))) {
                    RWarning("Failed to resend reply to client");
                }
                return;
            } else {
                // RNotice("Received duplicate request but no reply available; ignoring");
            }
        }
    }

    //request will be added to log upon receival of witnessDecision when delegated
    //also wait for witness to add it to client table when delegated to avoid misclassifying the request as a duplicate
    if(!isDelegated){
        // Update the client table
        UpdateClientTable(msg.req());

        // Leader Upcall
        bool replicate = false;
        string res;
        LeaderUpcall(lastCommitted, msg.req().op(), replicate, res);
        ClientTableEntry &cte =
            clientTable[msg.req().clientid()];
        
        
        // Check whether this request should be committed to replicas
        if (!replicate) {
            // Warning("Executing request failed. Not committing to replicas");
            ReplyMessage reply;

            reply.set_reply(res);
            reply.set_view(0);
            reply.set_opnum(0);
            reply.set_clientreqid(msg.req().clientreqid());
            reply.set_replicaidx(myIdx);
            reply.set_n(configuration.n);
            cte.replied = true;
            cte.reply = reply;
            transport->SendMessage(this, remote, reply);
            Latency_EndType(&requestLatency, 'f');
            Notice("%d sending reply from 3 and isleader=%d", myIdx, AmLeader());
        } else {
            
            Request request;
            request.set_op(res);
            request.set_clientid(msg.req().clientid());
            request.set_clientreqid(msg.req().clientreqid());
        
            /* Assign it an opnum */
            ++this->lastOp;
            v.view = this->view;
            v.opnum = this->lastOp;

            // Warning("Received REQUEST, assigning " FMT_VIEWSTAMP, VA_VIEWSTAMP(v));

            /* Add the request to my log */
            log.Append(v, request, LOG_STATE_PREPARED);
            

            if (batchComplete ||
                (lastOp - lastBatchEnd+1 > (unsigned int)batchSize)) {
                CloseBatch();
            } else {
                Warning("Keeping in batch");
                if (!closeBatchTimeout->Active()) {
                    closeBatchTimeout->Start();
                }
            }

            nullCommitTimeout->Reset();
            Latency_End(&requestLatency);
            
        }
    } else {
        // Store request in ring buffer using client ID as index
        pendingClientRequests.insert(msg.req());
    
        if (hasWitnessDecisionMsg) {
            HandleWitnessDecision(remote, witnessDecisionMsg);
            hasWitnessDecisionMsg = false;
        }
    }
}

void
VRReplica::HandleUnloggedRequest(const TransportAddress &remote,
                                 const UnloggedRequestMessage &msg)
{
    if (status != STATUS_NORMAL) {
        // Not clear if we should ignore this or just let the request
        // go ahead, but this seems reasonable.
        RNotice("Ignoring unlogged request due to abnormal status");
        return;
    }

    UnloggedReplyMessage reply;
    
    Debug("Received unlogged request %s", (char *)msg.req().op().c_str());

    ExecuteUnlogged(msg.req(), reply);
    
    if (!(transport->SendMessage(this, remote, reply)))
        Warning("Failed to send reply message");
}

void
VRReplica::HandlePrepare(const TransportAddress &remote,
                         const PrepareMessage &msg)
{
    // Warning("handle prepares");
    if (msg.view() == this->view && this->status == STATUS_VIEW_CHANGE) {
		if (AmLeader()) {
			RPanic("Unexpected PREPARE: I'm the leader of this view");
		}
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }

    RDebug("Received PREPARE <" FMT_VIEW "," FMT_OPNUM "-" FMT_OPNUM ">",
           msg.view(), msg.batchstart(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPARE due to abnormal status");
        return;
    }
    
    if (msg.view() < this->view) {
        RDebug("Ignoring PREPARE due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected PREPARE: I'm the leader of this view");
    }

   

    ASSERT(msg.batchstart() <= msg.opnum());
    ASSERT_EQ(msg.opnum()-msg.batchstart()+1, (unsigned)msg.request_size());
              
    viewChangeTimeout->Reset();
    
    if (msg.opnum() <= this->lastOp) {
        RDebug("Ignoring PREPARE; already prepared that operation");
        // Resend the prepareOK message
        PrepareOKMessage reply;
        reply.set_view(msg.view());
        reply.set_opnum(msg.opnum());
        reply.set_replicaidx(myIdx);
        reply.set_lastcommitted(lastCommitted);
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              reply))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
        return;
    }

    if (msg.batchstart() > this->lastOp+1) {
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }
    
    /* Add operations to the log */
    opnum_t op = msg.batchstart()-1;
    for (auto &req : msg.request()) {
        op++;
        if (op <= lastOp) {
            continue;
        }
    
        this->lastOp++;
        log.Append(viewstamp_t(msg.view(), op),
                   req, LOG_STATE_PREPARED);
        UpdateClientTable(req);
    }
    ASSERT(op == msg.opnum());
    
    /* Build reply and send it to the leader */
    PrepareOKMessage reply;
    reply.set_view(msg.view());
    reply.set_opnum(msg.opnum());
    reply.set_replicaidx(myIdx);
    reply.set_lastcommitted(lastCommitted);
    
    if (!(transport->SendMessageToReplica(this,
                                          configuration.GetLeaderIndex(view),
                                          reply))) {
        RWarning("Failed to send PrepareOK message to leader");
    }
}

void
VRReplica::HandlePrepareOK(const TransportAddress &remote,
                           const PrepareOKMessage &msg)
{

    Notice("handle prepare ok\n\n\n");

    RDebug("Received PREPAREOK <" FMT_VIEW ", "
           FMT_OPNUM  "> from replica %d",
           msg.view(), msg.opnum(), msg.replicaidx());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPAREOK due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring PREPAREOK due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        return;
    }

    if (!AmLeader()) {
        RWarning("Ignoring PREPAREOK because I'm not the leader");
        return;        
    }

    try {
		opnum_t replicaLastRecordedCommit = lastCommitteds.at(msg.replicaidx());
		ASSERT(replicaLastRecordedCommit <= msg.lastcommitted());
		lastCommitteds.at(msg.replicaidx()) = msg.lastcommitted();
	} catch (std::out_of_range const& exc) {
		RPanic("Tried to access an element that was out of range in lastCommitteds!");
	}
    
    viewstamp_t vs = { msg.view(), msg.opnum() };
    if (auto msgs = prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg, isDelegated)) {
        /*
         * We have a quorum of PrepareOK messages for this
         * opnumber. Execute it and all previous operations.
         *
         * (Note that we might have already executed it. That's fine,
         * we just won't do anything.)
         *
         * This also notifies the client of the result.
         */
        if(msg.replicaidx()%2 == 0){
            CommitUpTo(msg.opnum());
            lastCommitteds.at(myIdx) = lastCommitted;
        }

        if (msgs->size() >= (unsigned)configuration.QuorumSize()) {
            return;
        }
        
        /*
         * Send COMMIT message to the other replicas.
         *
         * This can be done asynchronously, so it really ought to be
         * piggybacked on the next PREPARE or something.
         */
        CommitMessage cm;
        cm.set_view(this->view);
        cm.set_opnum(this->lastCommitted);

        if (!(SendMessageToAllReplicas(cm))) {
            RWarning("Failed to send COMMIT message to all replicas");
        }

        nullCommitTimeout->Reset();

        // XXX Adaptive batching -- make this configurable
        if (lastBatchEnd == msg.opnum()) {
            batchComplete = true;
            if  (lastOp > lastBatchEnd) {
                CloseBatch();
            }
        }
    }
}

void
VRReplica::HandleCommit(const TransportAddress &remote,
                        const CommitMessage &msg)
{
    RDebug("Received COMMIT " FMT_VIEWSTAMP, msg.view(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring COMMIT due to abnormal status");
        return;
    }
    
    if (msg.view() < this->view) {
        RDebug("Ignoring COMMIT due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected COMMIT: I'm the leader of this view");
    }

    viewChangeTimeout->Reset();

    if (msg.opnum() <= this->lastCommitted) {
        RDebug("Ignoring COMMIT; already committed that operation");
        return;
    }

    if (msg.opnum() > this->lastOp) {
        RequestStateTransfer();
        return;
    }

    CommitUpTo(msg.opnum());
}


void
VRReplica::HandleRequestStateTransfer(const TransportAddress &remote,
                                      const RequestStateTransferMessage &msg)
{    
    RDebug("Received REQUESTSTATETRANSFER " FMT_VIEWSTAMP,
           msg.view(), msg.opnum());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring REQUESTSTATETRANSFER due to abnormal status");
        return;
    }

    if (msg.view() > view) {
        RequestStateTransfer();
        return;
    }

    // RNotice("Sending state transfer from " FMT_VIEWSTAMP " to "
    //         FMT_VIEWSTAMP,
    //         msg.view(), msg.opnum(), view, lastCommitted);

    StateTransferMessage reply;
    reply.set_view(view);
    reply.set_opnum(lastCommitted);
    
    log.Dump(msg.opnum()+1, reply.mutable_entries());

    transport->SendMessage(this, remote, reply);
}

void
VRReplica::HandleStateTransfer(const TransportAddress &remote,
                               const StateTransferMessage &msg)
{
    RDebug("Received STATETRANSFER " FMT_VIEWSTAMP, msg.view(), msg.opnum());
    
    if (msg.view() < view) {
        RWarning("Ignoring state transfer for older view");
        return;
    }
    
    opnum_t oldLastOp = lastOp;
    
    /* Install the new log entries */
    for (auto newEntry : msg.entries()) {
        if (newEntry.opnum() <= lastCommitted) {
            // Already committed this operation; nothing to be done.
#if PARANOID
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry->viewstamp.opnum == newEntry.opnum());
            ASSERT(entry->viewstamp.view == newEntry.view());
//          ASSERT(entry->request == newEntry.request());
#endif
        } else if (newEntry.opnum() <= lastOp) {
            // We already have an entry with this opnum, but maybe
            // it's from an older view?
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry->viewstamp.opnum == newEntry.opnum());
            ASSERT(entry->viewstamp.view <= newEntry.view());
            
            if (entry->viewstamp.view == newEntry.view()) {
                // We already have this operation in our log.
                ASSERT(entry->state == LOG_STATE_PREPARED);
#if PARANOID
//              ASSERT(entry->request == newEntry.request());                
#endif
            } else {
                // Our operation was from an older view, so obviously
                // it didn't survive a view change. Throw out any
                // later log entries and replace with this one.
                ASSERT(entry->state != LOG_STATE_COMMITTED);
                log.RemoveAfter(newEntry.opnum());
                lastOp = newEntry.opnum();
                oldLastOp = lastOp;

                viewstamp_t vs = { newEntry.view(), newEntry.opnum() };
                log.Append(vs, newEntry.request(), LOG_STATE_PREPARED);
            }
        } else {
            // This is a new operation to us. Add it to the log.
            ASSERT(newEntry.opnum() == lastOp+1);
            
            lastOp++;
            viewstamp_t vs = { newEntry.view(), newEntry.opnum() };

            log.Append(vs, newEntry.request(), LOG_STATE_PREPARED);
        }
        //TODO figure out why we need this - from vrw
        UpdateClientTable(newEntry.request());
    }
    

    if (msg.view() > view || (msg.view() == view && status == STATUS_VIEW_CHANGE)){
        EnterView(msg.view());
    }

    /* Execute committed operations */
    ASSERT(msg.opnum() <= lastOp);
    CommitUpTo(msg.opnum());

    SendPrepareOKs(oldLastOp);

    // Process pending prepares
    std::list<std::pair<TransportAddress *, PrepareMessage> >pending = pendingPrepares;
    pendingPrepares.clear();
    for (auto & msgpair : pendingPrepares) {
        RDebug("Processing pending prepare message");
        HandlePrepare(*msgpair.first, msgpair.second);
        delete msgpair.first;
    }
}

void
VRReplica::HandleStartViewChange(const TransportAddress &remote,
                                 const StartViewChangeMessage &msg)
{
    RDebug("Received STARTVIEWCHANGE " FMT_VIEW " from replica %d",
           msg.view(), msg.replicaidx());

    if (msg.view() < view) {
        RDebug("Ignoring STARTVIEWCHANGE for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring STARTVIEWCHANGE for current view");
        return;
    }

    if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
        RWarning("Received StartViewChange for view " FMT_VIEW
                 "from replica %d", msg.view(), msg.replicaidx());
        StartViewChange(msg.view());
    }

    ASSERT(msg.view() == view);


    if (auto msgs =
        startViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                   msg.replicaidx(),
                                                   msg, true)) {
        int leader = configuration.GetLeaderIndex(view);


        // only send out message if not leader
        if (leader != myIdx) {            
            DoViewChangeMessage dvc;
            dvc.set_view(view);
            dvc.set_lastnormalview(log.LastViewstamp().view);
            dvc.set_lastop(lastOp);
            dvc.set_lastcommitted(lastCommitted);
            dvc.set_replicaidx(myIdx);
            dvc.set_viewepoch(viewEpoch);

            // Figure out how much of the log to include
            opnum_t minCommitted = std::min_element(
                msgs->begin(), msgs->end(),
                [](decltype(*msgs->begin()) a,
                   decltype(*msgs->begin()) b) {
                    return a.second.lastcommitted() < b.second.lastcommitted();
                })->second.lastcommitted();
            minCommitted = std::min(minCommitted, lastCommitted);
            minCommitted = std::min(minCommitted, GetLowestReplicaCommit());
            
            log.Dump(minCommitted,
                     dvc.mutable_entries());
            

            RDebug("    sending DOVIEWCHANGE " FMT_VIEW " from replica %d to %d",msg.view(), msg.replicaidx(), leader);

            if (!(transport->SendMessageToReplica(this, leader, dvc))) {
                RWarning("Failed to send DoViewChange message to leader of new view");
            }
        }
    }
}


void
VRReplica::HandleDoViewChange(const TransportAddress &remote,
                              const DoViewChangeMessage &msg)
{
    RDebug("Received DOVIEWCHANGE " FMT_VIEW " from replica %d, "
           "lastnormalview=" FMT_VIEW " op=" FMT_OPNUM " committed=" FMT_OPNUM,
           msg.view(), msg.replicaidx(),
           msg.lastnormalview(), msg.lastop(), msg.lastcommitted());

    if (msg.view() < view) {
        RDebug("Ignoring DOVIEWCHANGE for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring DOVIEWCHANGE for current view");
        return;
    }

    if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
        // It's superfluous to send the StartViewChange messages here,
        // but harmless...
        RWarning("Received DoViewChange for view " FMT_VIEW
                 "from replica %d", msg.view(), msg.replicaidx());
        StartViewChange(msg.view());
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) == myIdx);

    ASSERT(msg.viewepoch() == viewEpoch);
    
    auto msgs = doViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                        msg.replicaidx(),
                                                        msg, isDelegated);
    if (msgs != NULL) {
 
        // Find the response with the most up to date log, i.e. the
        // one with the latest viewstamp
        view_t latestView = log.LastViewstamp().view;
        opnum_t latestOp = log.LastViewstamp().opnum;
		opnum_t highestCommitted = lastCommitted; 
        DoViewChangeMessage latestMsgObj;
        DoViewChangeMessage *latestMsg = NULL;

        for (const auto &kv : *msgs) {
            const DoViewChangeMessage &x = kv.second;

            if(x.replicaidx() % 2 != 0){
                continue;
            }

			highestCommitted = std::max(x.lastcommitted(), highestCommitted); 
            if ((x.lastnormalview() > latestView) ||
                (((x.lastnormalview() == latestView) &&
                  (x.lastop() > latestOp)))) {
                latestView = x.lastnormalview();
                latestOp = x.lastop();
				latestMsgObj = kv.second;
                latestMsg = &latestMsgObj;
            }
        }

        // Install the new log. We might not need to do this, if our
        // log was the most current one.
        if (latestMsg != NULL) {
            RDebug("Selected log from replica %d with lastop=" FMT_OPNUM,
                   latestMsg->replicaidx(), latestMsg->lastop());
            if (latestMsg->entries_size() == 0) {
                // There weren't actually any entries in the
                // log. That should only happen in the corner case
                // that everyone already had the entire log, maybe
                // because it actually is empty.
                ASSERT(lastCommitted == msg.lastcommitted());
                ASSERT(msg.lastop() == msg.lastcommitted());
            } else {
                if (latestMsg->entries(0).opnum() > lastCommitted+1) {
                    RPanic("Received log that didn't include enough entries to install it");
                }
                
                log.RemoveAfter(latestMsg->lastop()+1);
                log.Install(latestMsg->entries().begin(),
                            latestMsg->entries().end());
                for (auto entry : latestMsg->entries()) {
					UpdateClientTable(entry.request()); 
				}
            }
        } else {
            RDebug("My log is most current, lastnormalview=" FMT_VIEW " lastop=" FMT_OPNUM,
                   log.LastViewstamp().view, lastOp);
        }

        // How much of the log should we include when we send the
        // STARTVIEW message? Start from the lowest committed opnum of
        // any of the STARTVIEWCHANGE or DOVIEWCHANGE messages we got.
        //
        // We need to compute this before we enter the new view
        // because the saved messages will go away.
        auto svcs = startViewChangeQuorum.GetMessages(view);
        opnum_t minCommittedSVC = std::min_element(
            svcs.begin(), svcs.end(),
            [](decltype(*svcs.begin()) a,
               decltype(*svcs.begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        opnum_t minCommittedDVC = std::min_element(
            msgs->begin(), msgs->end(),
            [](decltype(*msgs->begin()) a,
               decltype(*msgs->begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        opnum_t minCommitted = std::min(minCommittedSVC, minCommittedDVC);
        minCommitted = std::min(minCommitted, lastCommitted);
        minCommitted = std::min(minCommitted, GetLowestReplicaCommit());

        lastOp = latestOp;

        EnterView(msg.view());
        
        viewEpoch += 1;
        
        Notice("incremented view epoch to %d\n", viewEpoch);
        // fflush(stdout);

        ASSERT(AmLeader());
        
		CommitUpTo(highestCommitted);

        for (size_t i = 0; i < lastCommitteds.size(); i++) {
			lastCommitteds[i] = cleanUpTo;
		}

        // Send a STARTVIEW message with the new log
        StartViewMessage sv;
        sv.set_view(view);
        sv.set_lastop(lastOp);
        sv.set_lastcommitted(lastCommitted);
        sv.set_viewepoch(viewEpoch);
        
        log.Dump(minCommitted, sv.mutable_entries());


        Notice("[%d] is leader of view %d - sending startviewmessage to all", myIdx, view);
        if (!(transport->SendMessageToAll(this, sv))) {
            RPanic("failed to send to all");
            RWarning("Failed to send StartView message to all replicas");
        }
    }    
}

void
VRReplica::HandleStartView(const TransportAddress &remote,
                           const StartViewMessage &msg)
{
    RDebug("Received STARTVIEW " FMT_VIEW 
          " op=" FMT_OPNUM " committed=" FMT_OPNUM " entries=%d",
          msg.view(), msg.lastop(), msg.lastcommitted(), msg.entries_size());
    RDebug("Currently in view " FMT_VIEW " op " FMT_OPNUM " committed " FMT_OPNUM,
          view, lastOp, lastCommitted);

    if (msg.view() < view) {
        RWarning("Ignoring STARTVIEW for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RWarning("Ignoring STARTVIEW for current view");
        return;
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) != myIdx);

    ASSERT(viewEpoch < msg.viewepoch());
    viewEpoch = msg.viewepoch();

    if (msg.entries_size() == 0) {
        ASSERT(msg.lastcommitted() == lastCommitted);
        ASSERT(msg.lastop() == msg.lastcommitted());
    } else {
        if (msg.entries(0).opnum() > lastCommitted+1) {
            RPanic("Not enough entries in STARTVIEW message to install new log");
        }
        
        // Install the new log
        log.RemoveAfter(msg.lastop()+1);
        log.Install(msg.entries().begin(),
                    msg.entries().end());        
        for (auto entry : msg.entries()) {
			UpdateClientTable(entry.request());
		}
    }


    EnterView(msg.view());
    opnum_t oldLastOp = lastOp;
    lastOp = msg.lastop();

    ASSERT(!AmLeader());

    CommitUpTo(msg.lastcommitted());
    
    SendPrepareOKs(oldLastOp);
}

void
VRReplica::HandleRecovery(const TransportAddress &remote,
                          const RecoveryMessage &msg)
{
    RDebug("Received RECOVERY from replica %d", msg.replicaidx());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring RECOVERY due to abnormal status");
        return;
    }

    RecoveryResponseMessage reply;
    reply.set_replicaidx(myIdx);
    reply.set_view(view);
    reply.set_nonce(msg.nonce());
    if (AmLeader()) {
        reply.set_lastcommitted(lastCommitted);
        reply.set_lastop(lastOp);
        log.Dump(0, reply.mutable_entries());
    }

    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send recovery response");
    }
    return;
}

void
VRReplica::HandleRecoveryResponse(const TransportAddress &remote,
                                  const RecoveryResponseMessage &msg)
{
    RDebug("Received RECOVERYRESPONSE from replica %d",
           msg.replicaidx());

    if (status != STATUS_RECOVERING) {
        RDebug("Ignoring RECOVERYRESPONSE because we're not recovering");
        return;
    }

    if (msg.nonce() != recoveryNonce) {
        RNotice("Ignoring recovery response because nonce didn't match");
        return;
    }

    auto msgs = recoveryResponseQuorum.AddAndCheckForQuorum(msg.nonce(),
                                                            msg.replicaidx(),
                                                            msg, false);
    if (msgs != NULL) {
        view_t highestView = 0;
        for (const auto &kv : *msgs) {
            if (kv.second.view() > highestView) {
                highestView = kv.second.view();
            }
        }
        
        int leader = configuration.GetLeaderIndex(highestView);
        ASSERT(leader != myIdx);
        auto leaderResponse = msgs->find(leader);
        if ((leaderResponse == msgs->end()) ||
            (leaderResponse->second.view() != highestView)) {
            RDebug("Have quorum of RECOVERYRESPONSE messages, "
                   "but still need to wait for one from the leader");
            return;
        }

        Notice("Recovery completed");
        
        log.Install(leaderResponse->second.entries().begin(),
                    leaderResponse->second.entries().end());        
        EnterView(leaderResponse->second.view());
        lastOp = leaderResponse->second.lastop();
        CommitUpTo(leaderResponse->second.lastcommitted());
    }
}



void
VRReplica::HandleWitnessDecision(const TransportAddress &remote,
                                  const WitnessDecision &msg)
{
    Assert(specpaxos::IsWitness(msg.replicaidx()));

    if (msg.view() > view) {
        RequestStateTransfer();
        return;
    }

    // Check if we have the corresponding request
    const auto &reqOpt = pendingClientRequests.get(msg.clientid(), msg.clientreqid());
    if (reqOpt == nullptr) {
        // no request yet; stop processing
        return;
    }

    const specpaxos::Request &storedReq = *reqOpt;

    if (msg.clientreqid() > storedReq.clientreqid()) {
        // We have an outdated client request; remove it and wait for fresh request
        pendingClientRequests.remove(msg.clientid(), msg.clientreqid());
        return;
    }

    if (msg.clientreqid() < storedReq.clientreqid()) {
        // WitnessDecision is outdated, remove decision & slot if present
        pendingWitnessDecisions.remove(msg.clientid(), msg.clientreqid());
        return;
    }

    // clientreqid matches, safe to insert decision
    pendingWitnessDecisions.insert(msg);

    // Process slots in order
    while (true) {
        uint32_t nextSlot = log.LastOpnum() + 1;

        const WitnessDecision *nextWD = pendingWitnessDecisions.getSlotNum(nextSlot);
        if (nextWD == nullptr) {
            break;
        }
        
        const Request *r = pendingClientRequests.get(nextWD->clientid(), nextWD->clientreqid());
        if (r == nullptr) {
            break;
        }
        
        const Request& req = *r;

        viewstamp_t v;
        ++this->lastOp;
        v.view = this->view;
        v.opnum = nextSlot;

        log.Append(v, req, LOG_STATE_COMMITTED);
        UpdateClientTable(req);


        pendingWitnessDecisions.remove(req.clientid(), req.clientreqid());
        pendingClientRequests.remove(req.clientid(), req.clientreqid());
    }



    CommitUpTo(this->lastOp);
}











void
VRReplica::HandleHeartbeat(const TransportAddress &remote,
                                  const Heartbeat &msg)
{

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring heartbeat due to abnormal status");
        return;
    }

    Assert(!AmLeader());

    viewChangeTimeout->Reset();

    RDebug("Received Heartbeat from leader %d",
           msg.slotexecuted());

    //TODO maybe add log updates? need to decide whether to keep state transfers or not
    CommitUpTo(this->lastOp);

    HeartbeatReply reply;
    reply.set_view(view);
    reply.set_slotout(lastCommitted);
    reply.set_replicaidx(myIdx);

    RDebug("Sending HBReply " FMT_VIEWSTAMP,
            reply.view(), reply.slotout());

    // Warning("%d sending heartbeatreply to %d", myIdx, configuration.GetLeaderIndex(view));

    if (!(transport->SendMessageToReplica(this,
                                            configuration.GetLeaderIndex(view),
                                            reply))) {
        RWarning("Failed to send HBReply message to leader");
    }
}




void
VRReplica::HandleHeartbeatReply(const TransportAddress &remote,
                                  const HeartbeatReply &msg)
{

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring heartbeatreply due to abnormal status");
        return;
    }
    Assert(AmLeader());

    // const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    // int srcAddr = simRemote.GetAddr();
    
    // heartbeatCheck[srcAddr] = 0;
    //the above doesnt work unless its a simulated address - had to add index field
    int srcAddr = msg.replicaidx();

    if (heartbeatCheck.find(srcAddr) == heartbeatCheck.end()) {
        RDebug("Received HeartbeatReply from new address %d; adding to heartbeatCheck", srcAddr);
    }
    

    heartbeatCheck[srcAddr] = 0;

    if(IsReplica(myIdx)){
        //TODO - implement idle node updates here as well
        // note that while the java implementatin performs GC atp, I have opted 
        //  to use Theano's vrw implementation of GC (since the C++ code was 
        //  already tested)
    }

}


opnum_t VRReplica::GetLowestReplicaCommit()
{
    opnum_t lowest = 0;
    if (!lastCommitteds.empty()) {
        *std::min_element(lastCommitteds.begin(), lastCommitteds.end()); 
    }
    return lowest;
}


void
VRReplica::CleanLog()
{
	/* 
	 * Truncate the log up to the current cleanUpTo value.
	 */
	RNotice("Cleaning up to " FMT_OPNUM, cleanUpTo);
	log.RemoveUpTo(cleanUpTo);
}


bool VRReplica::SendMessageToAllReplicas(const ::google::protobuf::Message &msg) {
    //replicas are odd numbered (but zero-index so start at 0)
    for(int i=0; i<configuration.n; i+=2){
        if(IsWitness(i)){
            RPanic(" designed as witness ");
        }
        if(i==myIdx){
            continue;
        }
        if (!(transport->SendMessageToReplica(this,
                                            i,
                                            msg))) {
            return false;
        }
    }
    return true;
}

bool VRReplica::SendMessageToAll(const ::google::protobuf::Message &msg) {
    //replicas are odd numbered (but zero-index so start at 0)
    for(int i=0; i<configuration.n; i++){
        if(i==myIdx){
            continue;
        }
        if (!(transport->SendMessageToReplica(this,
                                            i,
                                            msg))) {
            return false;
        }
    }
    return true;
}



size_t VRReplica::GetLogSize(){
    return log.Size();
}


} // namespace specpaxos::vr
} // namespace specpaxos
