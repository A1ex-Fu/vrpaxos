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
                     Transport *transport,
                     AppReplica *app)
    : Replica(config, myIdx, initialize, transport, app),
	  lastCommitteds(config.n, 0),
      log(false),
      startViewChangeQuorum(config.QuorumSize()-1)
{
    this->status = STATUS_NORMAL; 
    this->view = 0;
    this->lastOp = 1;
    this->lastCommitted = 0;

	this->cleanUpTo = 0;


    this->heartbeatCheckTimeout = new Timeout(transport, 5000, [this,myIdx]() {
            // RWarning("Have not heard from leader; witness");
            status = STATUS_VIEW_CHANGE;
        });

    _Latency_Init(&requestLatency, "request");
    _Latency_Init(&executeAndReplyLatency, "executeAndReply");

    if (initialize) {
		heartbeatCheckTimeout->Start();
    } else {
        // RWarning("Witness initialized with initialize set to false; witness");
    }
}

VRWitness::~VRWitness()
{
    
    Latency_Dump(&requestLatency);
    Latency_Dump(&executeAndReplyLatency);

    delete heartbeatCheckTimeout;
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
    RPanic("witness enter new view");

    view = newview;
    Warning("witness %d has status normal", myIdx);
    status = STATUS_NORMAL;

	heartbeatCheckTimeout->Start();

    startViewChangeQuorum.Clear();

}


void
VRWitness::ReceiveMessage(const TransportAddress &remote,
                          const string &type, const string &data)
{
    static RequestMessage request;
    static StartViewChangeMessage startViewChange;
    static StartViewMessage startView;
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

    heartbeatCheckTimeout->Reset();


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
        RNotice("Ignoring request due to abnormal status");
        return;
    }
    
    if((status != STATUS_VIEW_CHANGE)) {
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

    RPanic("got startviewmessage");
    EnterView(msg.view());
    opnum_t oldLastOp = lastOp;
    lastOp = msg.lastop();


    CommitUpTo(msg.lastcommitted());

    //presumably, witnesses will be up to date but not sure yet whether sending prepareOKs is
    // SendPrepareOKs(oldLastOp);
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

    Assert(msg.view() >= view);
    //check that we go through entire chain
    Assert(msg.replicaidx()+2==myIdx);
 
    if((status != STATUS_VIEW_CHANGE)) {
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


    // note: there is an issue where the witness is a part of a minority that believed the leader
    //   was unresponsive. In the java implementation, it would've ignored that fact and continued
    //   believing the leader was responsive until another replica said otherwise. However, in this 
    //   system, the witness is allowed to believe the leader is dead. They can start searching for 
    //   a quorum they might never acheive and then will leave the system undelegated for no good 
    //   reason. 

    if(msg.view()<view){
        Warning("got old heartbeat");
        return;
    }

    status = STATUS_NORMAL;

    if (msg.cleanupto() > lastCommitted) {
		RPanic("Asking me to clean an entry after my lastCommitted!");
	}

	if (msg.cleanupto() > cleanUpTo) {
		// Clean log up to the lowest committed entry by any replica
		cleanUpTo = msg.cleanupto();
		CleanLog(); 
	} else if (msg.cleanupto() < cleanUpTo) {
		// A node can see a lower cleanUpTo if the leader fell behind: when it reconstructs
		// state, it will use its own cleanUpTo as a "safe" value, and will update it 
		// later once it hears from all the other replicas. 
		RWarning("cleanUpTo decreased! Got " FMT_OPNUM ", had " FMT_OPNUM, 
				msg.cleanupto(), cleanUpTo);
	}

    heartbeatCheckTimeout->Reset();
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

