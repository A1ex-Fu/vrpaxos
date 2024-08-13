// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/witness.cc:
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
#include "common/log.h"
#include "vr/witness.h"
#include "vr/vr-proto.pb.h"

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"

#include "lib/simtransport.h"

#include <algorithm>
#include <random>

#define RDebug(fmt, ...) Debug("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, myIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace vr {

using namespace proto;
    
VRWitness::VRWitness(Configuration config, int myIdx,
                     bool initialize,
                     Transport *transport, int batchSize,
                     AppReplica *app)
    : Replica(config, myIdx, initialize, transport, app),
      batchSize(batchSize),
	  lastCommitteds(config.n, 0),
      log(false),
      startViewChangeQuorum(config.QuorumSize()-1),
      doViewChangeQuorum(config.QuorumSize()-1)
{
    this->status = STATUS_NORMAL;
    this->view = 0;
    this->lastOp = 1;
    this->lastCommitted = 0;
    this->isDelegated = true;

	this->cleanUpTo = 0;

    if (batchSize > 1) {
        Warning("Batching enabled; batch size %d", batchSize);
    }

    this->viewChangeTimeout = new Timeout(transport, 5000, [this,myIdx]() {
            // RWarning("Have not heard from leader; witness");
            // status = STATUS_VIEW_CHANGE;
        });

    _Latency_Init(&requestLatency, "request");
    _Latency_Init(&executeAndReplyLatency, "executeAndReply");

    if (initialize) {
		viewChangeTimeout->Start();
    } else {
        RWarning("Witness initialized with initialize set to false; witness");
    }
}

VRWitness::~VRWitness()
{
    
    Latency_Dump(&requestLatency);
    Latency_Dump(&executeAndReplyLatency);

    delete viewChangeTimeout;
}

uint64_t
VRWitness::GenerateNonce() const
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);    
}

void
VRWitness::CommitUpTo(opnum_t upto)
{
    while (lastCommitted < upto) {
        Latency_Start(&executeAndReplyLatency);
        
        lastCommitted++;

        /* Find operation in log */
        const LogEntry *entry = log.Find(lastCommitted);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", lastCommitted);
        }
        
        /* Mark it as committed */
        log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);

        Latency_End(&executeAndReplyLatency);
    }
}


void
VRWitness::EnterView(view_t newview)
{
    RNotice("Entering new view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_NORMAL;

	viewChangeTimeout->Start();

    startViewChangeQuorum.Clear();
    doViewChangeQuorum.Clear();

}


void
VRWitness::ReceiveMessage(const TransportAddress &remote,
                          const string &type, const string &data)
{
    static RequestMessage request;
    static StartViewChangeMessage startViewChange;
    static StartViewMessage startView;
    static DoViewChangeMessage doViewChange;
    static Heartbeat heartbeat;
    static ChainMessage chainMessage;

    const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    int srcAddr = simRemote.GetAddr();

    // RWarning("Received %s message in VR Witness from %d", type.c_str(), srcAddr);
    
    if (type == request.GetTypeName()) {
        Latency_Start(&requestLatency);
        request.ParseFromString(data);
        HandleRequest(remote, request);
        Latency_EndType(&requestLatency, 'i');
    } else if (type == startViewChange.GetTypeName()) {
        startViewChange.ParseFromString(data);
        HandleStartViewChange(remote, startViewChange);
    } else if (type == startView.GetTypeName()) {
        startView.ParseFromString(data);
        HandleStartView(remote, startView);
    } else if (type == doViewChange.GetTypeName()) {
        doViewChange.ParseFromString(data);
        HandleDoViewChange(remote, doViewChange);
    } else if (type == heartbeat.GetTypeName()) {
        heartbeat.ParseFromString(data);
        HandleHeartbeat(remote, heartbeat);
    } else if (type == chainMessage.GetTypeName()) {
        chainMessage.ParseFromString(data);
        HandleChainMessage(remote, chainMessage);
    } else {
        RPanic("Received unexpected message type in VR witness: %s",
              type.c_str());
    }

}

void
VRWitness::StartViewChange(view_t newview)
{
    RNotice("Starting view change for view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_VIEW_CHANGE;

    viewChangeTimeout->Reset();


    StartViewChangeMessage m;
    m.set_view(newview);
    m.set_replicaidx(myIdx);
    m.set_lastcommitted(lastCommitted);
    if (!transport->SendMessageToAll(this, m)) {
        RWarning("Failed to send StartViewChange message to all replicas");
    }
}

void
VRWitness::HandleRequest(const TransportAddress &remote,
                         const RequestMessage &msg)
{        

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring request due to abnormal status %s", status);
        return;
    }
    
    if(isDelegated && (status != STATUS_VIEW_CHANGE)) {
        viewstamp_t v;

        //ony first witness replies to requests
        if(myIdx == 1) {
            int slotNum = log.Contains(msg.req());
            //decide on slotNum depending on whether it is a new command or not
            if(slotNum==-1){
                //new command - get new slotNum, add to log, and increment slotin
                slotNum = lastOp;
                this->lastOp++;
                
                //auto commit requests for the witness
                ++this->lastCommitted;

                bool replicate = true;
                string res;
                LeaderUpcall(lastCommitted, msg.req().op(), replicate, res);
            
                Request request;
                request.set_op(res);
                request.set_clientid(msg.req().clientid());
                request.set_clientreqid(msg.req().clientreqid());
                v.view = this->view;
                v.opnum = slotNum;

                /* Add the request to my log */
                log.Append(v, request, LOG_STATE_COMMITTED);
            }

            //chaining or not
            if(myIdx == (configuration.n-2)){
                //last witness
                //send witnessDecision to all replicas
                WitnessDecision reply;
                reply.set_view(view);
                reply.set_opnum(slotNum);
                reply.set_replicaidx(myIdx);
                *reply.mutable_req() = msg.req();
                reply.set_reqstr(msg.reqstr());

                // RWarning("sending WitnessDecision with slot %d for request %s", slotNum, msg.reqstr().c_str());
                if(!SendMessageToAllReplicas(reply)){
                    RWarning("Failed to send prepare message to all replicas - from witness HandleRequest");
                }else{
                    // Warning("[%d] SENT WITNESSDECISION   -    client: %d; reply: %s; slot: %d", myIdx, reply.req().clientreqid(), reply.reqstr().c_str(), reply.opnum());
                }
            } else {
                //not the last witness
                Warning("CHAINING");
                //send chainmessage to next witness - every other node is a witness
                ChainMessage reply;
                reply.set_view(view);
                reply.set_opnum(slotNum);
                reply.set_replicaidx(myIdx);
                *reply.mutable_req() = msg.req();

                if(!transport->SendMessageToReplica(this, (myIdx+2), reply)){
                    RWarning("Failed to send prepare message to next witness in chain");
                }
            }

        }
    }

}


void
VRWitness::HandleStartViewChange(const TransportAddress &remote,
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
        StartViewChange(msg.view());
    }

    ASSERT(msg.view() == view);
    
    if (auto msgs =
        startViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                   msg.replicaidx(),
                                                   msg, false)) {
        int leader = configuration.GetLeaderIndex(view);
        // Don't try to send a DoViewChange message to ourselves
        if (leader != myIdx) {            
            DoViewChangeMessage dvc;
            dvc.set_view(view);
            dvc.set_lastnormalview(log.LastViewstamp().view);
            dvc.set_lastop(lastOp);
            dvc.set_lastcommitted(lastCommitted);
            dvc.set_replicaidx(myIdx);

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

            if (!(transport->SendMessageToReplica(this, leader, dvc))) {
                RWarning("Failed to send DoViewChange message to leader of new view");
            }
        }
    }
}
void
VRWitness::HandleStartView(const TransportAddress &remote,
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
    }


    EnterView(msg.view());
    opnum_t oldLastOp = lastOp;
    lastOp = msg.lastop();


    CommitUpTo(msg.lastcommitted());

    //presumably, witnesses will be up to date but not sure yet whether sending prepareOKs is
    // SendPrepareOKs(oldLastOp);
}

void
VRWitness::HandleDoViewChange(const TransportAddress &remote,
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

        for (auto kv : *msgs) {
            DoViewChangeMessage &x = kv.second;
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

        
		CommitUpTo(highestCommitted);

        for (size_t i = 0; i < lastCommitteds.size(); i++) {
			lastCommitteds[i] = cleanUpTo;
		}

        // Send a STARTVIEW message with the new log
        StartViewMessage sv;
        sv.set_view(view);
        sv.set_lastop(lastOp);
        sv.set_lastcommitted(lastCommitted);
        
        log.Dump(minCommitted, sv.mutable_entries());

        if (!(transport->SendMessageToAll(this, sv))) {
            RWarning("Failed to send StartView message to all replicas");
        }
    }    
}


void
VRWitness::HandleChainMessage(const TransportAddress &remote,
                                  const ChainMessage &msg)
{
    Assert(specpaxos::IsWitness(msg.replicaidx()));
    
    //first witness should not get chain messages
    if(myIdx==1){
        return;
    }

    this->isDelegated = true;

    //TODO: not sure about this view thing
    Assert(msg.view() >= view);
    //check that we go through entire chain
    Assert(msg.replicaidx()+2==myIdx);
 
    if(isDelegated && (status != STATUS_VIEW_CHANGE)) {
        viewstamp_t v;

        int slotNum = log.Contains(msg.req());
        //decide on slotNum depending on whether it is a new command or not
        if(slotNum==-1){
            //new command - get new slotNum, add to log, and increment slotin
            slotNum = lastOp;
            ++this->lastOp;
            //auto commit requests for the witness
            ++this->lastCommitted;

            bool replicate = true;
            string res;
            LeaderUpcall(lastCommitted, msg.req().op(), replicate, res);
        
            Request request;
            request.set_op(res);
            request.set_clientid(msg.req().clientid());
            request.set_clientreqid(msg.req().clientreqid());
            v.view = this->view;
            v.opnum = slotNum;

            /* Add the request to my log */
            log.Append(v, request, LOG_STATE_PREPARED);
        }

        //chaining or not
        if(myIdx == (configuration.n/2)){
            //last witness
            //send witnessDecision to all replicas
            WitnessDecision reply;
            reply.set_view(view);
            reply.set_opnum(slotNum);
            reply.set_replicaidx(myIdx);
            *reply.mutable_req() = msg.req();

            if(!SendMessageToAllReplicas(reply)){
                RWarning("Failed to send prepare message to all replicas - from witness HandleRequest");
            }
        } else {
            //not the last witness
            //send chainmessage to next witness - every other node is a witness
            ChainMessage reply;
            reply.set_view(view);
            reply.set_opnum(slotNum);
            reply.set_replicaidx(myIdx);
            *reply.mutable_req() = msg.req();
            
            if(!transport->SendMessageToReplica(this, (myIdx+2), reply)){
                RWarning("Failed to send prepare message to next witness in chain");
            }
        }
    }

}

void
VRWitness::HandleHeartbeat(const TransportAddress &remote,
                                  const Heartbeat &msg)
{
    RDebug("Received Heartbeat from leader %d",
           msg.slotexecuted());

    viewChangeTimeout->Reset();
    HeartbeatReply reply;
    reply.set_view(view);
    reply.set_slotout(lastCommitted);

    RDebug("Sending HBReply " FMT_VIEWSTAMP,
            reply.view(), reply.slotout());

    // Warning("%d sending heartbeatreply to %d", myIdx, configuration.GetLeaderIndex(view));

    if (!(transport->SendMessageToReplica(this,
                                            configuration.GetLeaderIndex(view),
                                            reply))) {
        RWarning("Failed to send HBReply message to leader");
    }
}



opnum_t
VRWitness::GetLowestReplicaCommit()
{
	opnum_t lowest = *std::min_element(lastCommitteds.begin(), lastCommitteds.end()); 
	return lowest;
}

void
VRWitness::CleanLog()
{
	/* 
	 * Truncate the log up to the current cleanUpTo value.
	 */
	RNotice("Cleaning up to " FMT_OPNUM, cleanUpTo);
	log.RemoveUpTo(cleanUpTo);
}

bool VRWitness::SendMessageToAllReplicas(const ::google::protobuf::Message &msg) {
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

size_t VRWitness::GetLogSize(){
    return log.Size();
}


} // namespace specpaxos::vr
} // namespace specpaxos
