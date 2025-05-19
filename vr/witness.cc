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
#include <fstream>
#include <sstream>
#include <map>

#define RDebug(fmt, ...) Debug("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, myIdx, ##__VA_ARGS__)

#include "clock.h"
using namespace Clock; 


namespace specpaxos {
namespace vr {

using namespace proto;
    
VRWitness::VRWitness(Configuration config, int myIdx,
                     bool initialize,
                     Transport *transport,
                     AppReplica *app)
    : Replica(config, myIdx, initialize, transport, app),
	  lastCommitteds(config.n, 0),
    //   log(false),
      startViewChangeQuorum(config.QuorumSize()-1)
{
    this->status = STATUS_NORMAL; 
    this->view = 0;
    this->lastOp = 1;
    this->lastCommitted = 0;

	this->cleanUpTo = 0;

    this->viewEpoch = 0;


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

    //___________________________________________
    //for tracing
    //Flag to turn off trace printing so the tests can be run in a reasonable amount of time
    this->printingTraces = true;

    // traceFile.open(filename, std::ios::out);
    // if (!traceFile.is_open()) {
    //     std::cerr << "Failed to open file: " << filename << std::endl;
    //     Panic("Failed to open file %s", filename.c_str());
    // }else{
    //     traceFile << "temp str to clear";
    //     traceFile.close();
    // }


}

VRWitness::~VRWitness()
{
    
    Latency_Dump(&requestLatency);
    Latency_Dump(&executeAndReplyLatency);

    delete heartbeatCheckTimeout;

    if(traceFile.is_open()) {
        traceFile.close();
    }
}


void
VRWitness::EnterView(view_t newview)
{
    RNotice("Entering new view " FMT_VIEW, newview);

    this->viewEpoch++;

    view = newview;
    Warning("witness %d has status normal", myIdx);
    status = STATUS_NORMAL;

	heartbeatCheckTimeout->Start();

    startViewChangeQuorum.Clear();

}


std::string VRWitness::getRole(int input) {
    if (input >= configuration.n) {
        return "Client";
    } else if (IsWitness(input)) {
        return "Witness";
    } else {
        return "Replica";
    }
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

    // const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    // int srcAddr = simRemote.GetAddr();

    // Notice("%d Received %s message in VR Witness from %s", myIdx, type.c_str(), remote.ToString().c_str());
    
    
    // std::stringstream ss;
    // size_t pos = type.rfind('.');
    // std::string messageName = (pos != std::string::npos) ? type.substr(pos + 1) : type;

    // // ss << "received " << messageName << " from " << getRole(srcAddr) << " " << srcAddr;
    // WriteToTrace(ss.str());
    
    if (type == request.GetTypeName()) {
        // Notice("%d Received request message from %s", myIdx, remote.ToString().c_str());
        request.ParseFromString(data);
        // WriteMessageContents(request);
        // Notice("got request %d", request.req().clientreqid());
        start = rdtsc_clock();
        HandleRequest(remote, request);
    } else if (type == startViewChange.GetTypeName()) {
        startViewChange.ParseFromString(data);
        WriteMessageContents(startViewChange);
        HandleStartViewChange(remote, startViewChange);
    } else if (type == startView.GetTypeName()) {
        startView.ParseFromString(data);
        WriteMessageContents(startView);
        HandleStartView(remote, startView);
    } else if (type == heartbeat.GetTypeName()) {
        heartbeat.ParseFromString(data);
        WriteMessageContents(heartbeat);
        HandleHeartbeat(remote, heartbeat);
    } else if (type == chainMessage.GetTypeName()) {
        chainMessage.ParseFromString(data);
        WriteMessageContents(chainMessage);
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
    m.set_viewepoch(viewEpoch);

    SendAndWrite(m, -2);
}

void
VRWitness::HandleRequest(const TransportAddress &remote,
                         const RequestMessage &msg)
{   
    // Notice("got request from client %d with ID %d", msg.req().clientid(), msg.req().clientreqid());
    if (status != STATUS_NORMAL || myIdx != 1) {
        return; 
    }
    
    int slotNum = -1;

    auto clientIt = clientRequestMap.find(msg.req().clientid());
    if (clientIt != clientRequestMap.end()) {
        auto reqIt = clientIt->second.find(msg.req().clientreqid());
        if (reqIt != clientIt->second.end()) {
            slotNum = reqIt->second;
        }
    }

    if (slotNum == -1) {
        slotNum = lastOp++;
        lastCommitted++;
        clientRequestMap[msg.req().clientid()][msg.req().clientreqid()] = slotNum;
    }

    //chaining or not
    if (myIdx == configuration.n - 2) {
        //last witness
        //send witnessDecision to all replicas
        WitnessDecision reply;
        reply.set_view(view);
        reply.set_opnum(slotNum);
        reply.set_replicaidx(myIdx);
        reply.set_clientreqid(msg.req().clientreqid());
        reply.set_clientid(msg.req().clientid());
        // reply.set_reqstr(msg.reqstr());
        
        // Notice("sending WitnessDecision for req %lu for client %lu with slot %d", reply.clientreqid(), reply.clientid(), reply.opnum());
        SendMessageToAllReplicas(reply);
        end = rdtsc_clock();
        // logCycleMeasurement("RequestToDecision", start, end);
    } else {
        ChainMessage reply;
        reply.set_view(view);
        reply.set_opnum(slotNum);
        reply.set_replicaidx(myIdx);
        reply.set_clientreqid(msg.req().clientreqid());
        reply.set_clientid(msg.req().clientid());
        // reply.set_reqstr(msg.reqstr());

        transport->SendMessageToReplica(this, myIdx + 2, reply);
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
            dvc.set_lastnormalview(view);
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
            
            SendAndWrite(dvc, leader);  
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


    ASSERT(viewEpoch < msg.viewepoch());
    viewEpoch = msg.viewepoch();

    if (msg.entries_size() == 0) {
        ASSERT(msg.lastcommitted() == lastCommitted);
        ASSERT(msg.lastop() == msg.lastcommitted());
    } else {
        if (msg.entries(0).opnum() > lastCommitted + 1) {
            RPanic("Not enough entries in STARTVIEW message to install new log");
        }

   
        for (int i = 0; i < msg.entries_size(); ++i) {
            const proto::StartViewMessage_LogEntry &entry = msg.entries(i); // Changed type here
            clientRequestMap[entry.request().clientid()][entry.request().clientreqid()] = entry.opnum();
        }
    }

    EnterView(msg.view());
    opnum_t oldLastOp = lastOp;

    Notice("setting lastop %d to have value %d", lastOp, msg.lastop());
    lastOp = msg.lastop() + 1;

    lastCommitted = msg.lastcommitted();
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

        // int slotNum = log.ContainsRequestID(msg.clientreqid());
        int slotNum = -1;
        auto clientIt = clientRequestMap.find(msg.clientid());
        if (clientIt != clientRequestMap.end()) {
            auto &requests = clientIt->second;
            auto reqIt = requests.find(msg.clientreqid());
            if (reqIt != requests.end()) {
                slotNum = reqIt->second;
            }
        }

        //decide on slotNum depending on whether it is a new command or not
        if(slotNum==-1){
            //new command - get new slotNum, add to log, and increment slotin
            slotNum = lastOp;
            ++this->lastOp;
            //auto commit requests for the witness
            ++this->lastCommitted;

            // bool replicate = true;
            // string res;
            // LeaderUpcall(lastCommitted, msg.req().op(), replicate, res);
        
            v.view = this->view;
            v.opnum = slotNum;

            /* Add the request to my log */
            // log.Append(v, request, LOG_STATE_PREPARED);
            clientRequestMap[msg.clientid()][msg.clientreqid()] = v.opnum;
            // Notice("set %d, %d as %d", msg.clientid(), msg.clientreqid(), v.opnum);
        }

        //chaining or not
        if(myIdx == (configuration.n-2)){
            //last witness
            //send witnessDecision to all replicas
            Notice("set opnum p2 %d", slotNum);
            WitnessDecision reply;
            reply.set_view(view);
            reply.set_opnum(slotNum);
            reply.set_replicaidx(myIdx);
            reply.set_clientreqid(msg.clientreqid());
            reply.set_clientid(msg.clientid());
            // reply.set_reqstr(msg.reqstr());

            SendAndWrite(reply, -1);
        } else {
            //not the last witness
            //send chainmessage to next witness - every other node is a witness
            ChainMessage reply;
            reply.set_view(view);
            reply.set_opnum(slotNum);
            reply.set_replicaidx(myIdx);
            reply.set_clientreqid(msg.clientreqid());
            reply.set_clientid(msg.clientid());
            // reply.set_reqstr(msg.reqstr());
            // 
            //Assumes witnesses are every other node
            SendAndWrite(reply, (myIdx+2));
        }
    }

}

void
VRWitness::HandleHeartbeat(const TransportAddress &remote,
                                  const Heartbeat &msg)
{
    RDebug("Received Heartbeat from leader %d",
           msg.slotexecuted());


    // note: there was an (extremely annoying) issue where the witness is a part of a minority 
    //   that believed the leader was unresponsive. 
    //   In the java implementation, it would've ignored that fact and continued
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

    //got valid hb - reset hbcheck and send a hbreply
    heartbeatCheckTimeout->Reset();
    HeartbeatReply reply;
    reply.set_view(view);
    reply.set_slotout(lastCommitted);
    reply.set_replicaidx(myIdx);

    RDebug("Sending HBReply " FMT_VIEWSTAMP,
            reply.view(), reply.slotout());

    SendAndWrite(reply, configuration.GetLeaderIndex(view));
}


/**
 * @brief lowest replica commit (GC-able indices)
 * 
 * @return opnum_t - lowest commit found
 */
opnum_t
VRWitness::GetLowestReplicaCommit()
{
	opnum_t lowest = *std::min_element(lastCommitteds.begin(), lastCommitteds.end()); 
	return lowest;
}

/**
 * @brief Garbage collects log up to the current cleanUpTo value
 * 
 */
void 
VRWitness::CleanLog() {
    RNotice("Cleaning up to " FMT_OPNUM, cleanUpTo);
    // log.RemoveUpTo(cleanUpTo); // Removed

    // for (auto it = clientRequestMap.begin(); it != clientRequestMap.end(); ) {
    //     if (it->second <= cleanUpTo) {
    //         it = clientRequestMap.erase(it);
    //     } else {
    //         ++it;
    //     }
    // }
}


/**
 * @brief send message @p msg to only the replica nodes in the system, not the witness nodes
 * 
 * @param msg - message to be sent
 * @return true - if send was successful
 * @return false - if any sends failed
 */
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

// /**
//  * @brief testing function from vrw code
//  * 
//  * @return size_t - size of the log
//  */
// size_t VRWitness::GetLogSize(){
//     return log.Size();
// }












//________________________________________
//trace code
// void VRWitness::UpdateTraceFile(std::string filename){
//     if (traceFile.is_open()) {
//         traceFile.close();
//     }

//     traceFile.open(filename, std::ios::out);
//     if (!traceFile.is_open()) {
//         std::cerr << "Failed to open file: " << filename << std::endl;
//         Panic("Failed to open file %s", filename.c_str());
//     }else{
//         //push string to clear file (i think its necessary but idk - no impact anyways)
//         traceFile << "";
//         traceFile.close();
//     }


//     traceFile.open(filename, std::ios::out | std::ios::app);
//     if (!traceFile.is_open()) {
//         std::cerr << "Failed to open file: " << filename << std::endl;
//         Panic("Failed to open file %s", filename.c_str());
//     }
//     this->filename = filename;
// }

/**
 * @brief writes input string to trace file with the prefix "Witness {myIdx} "
 * 
 * @param line string to be written
 */
// void VRWitness::WriteToTrace(const std::string& line) {
//     if(!printingTraces){
//         return;
//     }
//     // Check if the trace file is open
//     if (!traceFile.is_open()) {
//         // attempt to open the trace file
//         traceFile.open(filename, std::ios::out | std::ios::app);
//         if (!traceFile) {
//             throw std::ios_base::failure("failed to open trace file");
//         }
//     }

//     // write the line to the trace file
//     traceFile << "Witness " << myIdx << " " << line << std::endl;
// }


/**
 * @brief helps to indent debugStrings for print outs
 * 
 * @param debugString - string to be indented
 * @return std::string - the indented string
 */
std::string indentDebugString(const std::string& debugString) {
    std::stringstream indentedStream;
    std::string line;
    std::istringstream stream(debugString);

    while (std::getline(stream, line)) {
        indentedStream << "        " << line << "\n"; // 8 spaces
    }

    return indentedStream.str();
}

//NOTE: tracing was removed for speed
/**
 * @brief Write the contents of the message to the trace.txt file
 * 
 * @param msg - message to write the contents of
 */
void VRWitness::WriteMessageContents(const ::google::protobuf::Message &msg) {
    // if(!printingTraces){
    //     return;
    // }

    // if (!traceFile.is_open()) {
    //     // attempt to open the trace file
    //     traceFile.open(filename, std::ios::out | std::ios::app);
    //     if (!traceFile) {
    //         throw std::ios_base::failure("failed to open trace file");
    //     }
    // }

    // std::string type = msg.GetTypeName();
    // std::stringstream ss;
    // size_t pos = type.rfind('.');
    // std::string messageName = (pos != std::string::npos) ? type.substr(pos + 1) : type;

    // ss << "    " << messageName << ":\n";

    // // indent content prints for fancier formatting


    // if (type == RequestMessage().GetTypeName()) {
    //     const RequestMessage& request = dynamic_cast<const RequestMessage&>(msg);
    //     if(request.DebugString().empty()){
    //         Panic("empty req");
    //     }
    //     ss << indentDebugString(request.DebugString());
    // } else if (type == StartViewChangeMessage().GetTypeName()) {
    //     const StartViewChangeMessage& startViewChange = dynamic_cast<const StartViewChangeMessage&>(msg);
    //     ss << indentDebugString(startViewChange.DebugString());
    // } else if (type == StartViewMessage().GetTypeName()) {
    //     const StartViewMessage& startView = dynamic_cast<const StartViewMessage&>(msg);
    //     ss << indentDebugString(startView.DebugString());
    // } else if (type == Heartbeat().GetTypeName()) {
    //     const Heartbeat& heartbeat = dynamic_cast<const Heartbeat&>(msg);
    //     ss << indentDebugString(heartbeat.DebugString());
    // } else if (type == ChainMessage().GetTypeName()) {
    //     const ChainMessage& chainMessage = dynamic_cast<const ChainMessage&>(msg);
    //     ss << indentDebugString(chainMessage.DebugString());
    // } else if (type == WitnessDecision().GetTypeName()) {
    //     const WitnessDecision& witnessDecision = dynamic_cast<const WitnessDecision&>(msg);
    //     ss << indentDebugString(witnessDecision.DebugString());
    // } else if (type == HeartbeatReply().GetTypeName()) {
    //     const HeartbeatReply& heartbeatReply = dynamic_cast<const HeartbeatReply&>(msg);
    //     ss << indentDebugString(heartbeatReply.DebugString());
    // } else if (type == DoViewChangeMessage().GetTypeName()) {
    //     const DoViewChangeMessage& dvc = dynamic_cast<const DoViewChangeMessage&>(msg);
    //     ss << indentDebugString(dvc.DebugString());
    // } else {
    //     RPanic("attempted to write unexpected message type in VR witness: %s", type.c_str());
    // }

    // traceFile << ss.str() << "\n" << std::endl;
}





//NOTE: tracing was removed for speed
// /**
//  * @brief Sends messages and also writes to the trace.txt file
//  * 
//  * @param msg - message to send
//  * @param code - 0<=code<=configuration.n for sending to specific address code, 
//  *  -1 for sending to all replicas, and -2 for sending to all nodes 
//  */
// void VRWitness::SendAndWrite(const ::google::protobuf::Message &msg, int code){
//     size_t pos = msg.GetTypeName().rfind('.');
//     std::string messageName = (pos != std::string::npos) ? msg.GetTypeName().substr(pos + 1) : msg.GetTypeName();

//     std::stringstream ss;
        
//     ss << "sent " << messageName << " to {";

//     if(code>=0 && code<=configuration.n){
//         //send to specific node with replicaIdx of code
//         if (!(transport->SendMessageToReplica(this,
//                                             code,
//                                             msg))) {
//             RWarning("Failed to send %s message to %s %d", messageName, getRole(code), code);
//             return;
//         }

//         ss << getRole(code) << " " << code;
//     }else if(code == -1){
//         //send to all replicas
//         if(!SendMessageToAllReplicas(msg)){
//             RWarning("Failed to send %s message to all replicas", messageName);
//             return;
//         }
        
//         for(int i=0; i<configuration.n; i++){
//             if(!(myIdx==i || IsWitness(i))){
//                 ss << getRole(i) << " " << i;
//                 if(i!=configuration.n-1){
//                     ss << ", ";
//                 }
//             }
//         }
//     }else if(code == -2){
//         //send to all nodes
//         if (!transport->SendMessageToAll(this, msg)) {
//             RWarning("Failed to send %s message to all nodes", messageName);
//             return;
//         } 

//         for(int i=0; i<configuration.n; i++){
//             if(!(myIdx==i)){
//                 ss << getRole(i) << " " << i;
//                 if(i!=configuration.n-1){
//                     ss << ", ";
//                 }
//             }
//         }
//     }else{
//         Panic("invalid code %d", code);
//     }

//     //sends were all successful
//     ss << "}";
//     if(!printingTraces){
//         return;
//     }
//     WriteToTrace(ss.str());
//     WriteMessageContents(msg);
// } 

void VRWitness::SendAndWrite(const ::google::protobuf::Message &msg, int code) {
    if (code >= 0 && code <= configuration.n) {
        // Send to specific replica
        if (!transport->SendMessageToReplica(this, code, msg)) {
            RWarning("Failed to send message to %s %d", getRole(code), code);
        }
    } else if (code == -1) {
        // Send to all replicas
        if (!SendMessageToAllReplicas(msg)) {
            RWarning("Failed to send message to all replicas");
        }
    } else if (code == -2) {
        // Send to all nodes
        if (!transport->SendMessageToAll(this, msg)) {
            RWarning("Failed to send message to all nodes");
        }
    } else {
        Panic("Invalid code %d", code);
    }
}





} // namespace specpaxos::vr
} // namespace specpaxos

