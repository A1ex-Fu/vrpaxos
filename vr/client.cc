// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/client.cc:
 *   Viewstamped Replication clinet
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

 #include "common/client.h"
 #include "common/request.pb.h"
 #include "lib/assert.h"
 #include "lib/message.h"
 #include "lib/transport.h"
 #include "vr/client.h"
 #include "vr/vr-proto.pb.h"
 #include "lib/simtransport.h"
 #include "vr/witness.h"
 #include <sstream>




namespace specpaxos {
namespace vr {

VRClient::VRClient(const Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, transport, clientid)
{
    msg = NULL;
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqId = 0;
    gotAck = false; 
    view = 0;
    
    requestTimeout = new Timeout(transport, 1000, [this]() {
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
}

VRClient::~VRClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
    
    delete requestTimeout;
    delete unloggedRequestTimeout;
}

void
VRClient::Invoke(const string &request,
                 continuation_t continuation)
{
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    pendingRequest = new PendingRequest(request, reqId, continuation);
    this->gotAck = false;
    replicas.clear();

    SendRequest();
}

void
VRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    // XXX Can only handle one pending request for now
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    
    pendingUnloggedRequest = new PendingRequest(request, reqId, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    proto::UnloggedRequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingUnloggedRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingUnloggedRequest->clientReqId);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();
    
    transport->SendMessageToReplica(this, replicaIdx, reqMsg);
}

void
VRClient::SendRequest()
{
    proto::RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqId);
    reqMsg.set_reqstr(pendingRequest->request);
    
    // Warning("sending message %s to all", reqMsg.reqstr().c_str());
    // XXX Try sending only to (what we think is) the leader first
    // transport->SendMessageToAll(this, reqMsg);

    SendMessageToAllReplicas(reqMsg);
    // first witness always has address 1 in the current address indexing scheme
    transport->SendMessageToReplica(this, 1, reqMsg);

    requestTimeout->Reset();
}


/**
 * @brief send message @p msg to only the replica nodes in the system, not the witness nodes
 * 
 * @param msg - message to be sent
 * @return true - if send was successful
 * @return false - if any sends failed
 */
bool VRClient::SendMessageToAllReplicas(const ::google::protobuf::Message &msg) {
    // Send to all non-witness nodes (even indices)
    for(int i=0; i<config.n; i++) {
        if(!IsWitness(i)) {
            if (!(transport->SendMessageToReplica(this, i, msg))) {
                return false;
            }
        }
    }
    return true;
}




void
VRClient::ResendRequest()
{
    SendRequest();
}


void
VRClient::ReceiveMessage(const TransportAddress &remote,
                         const string &type,
                         const string &data)
{
    static proto::ReplyMessage reply;
    static proto::UnloggedReplyMessage unloggedReply;
    static proto::PaxosAck paxosAck;

    // const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    // int srcAddr = simRemote.GetAddr();

    // Notice("Received %s message in VR Client from %s", type.c_str(), remote.ToString().c_str());

    // Warning("Received %s message in VR Client from %d", type.c_str(), srcAddr);
    
    if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else if (type == unloggedReply.GetTypeName()) {
        unloggedReply.ParseFromString(data);
        HandleUnloggedReply(remote, unloggedReply);
    } else if (type == paxosAck.GetTypeName()) {
        paxosAck.ParseFromString(data);
        HandlePaxosAck(remote, paxosAck);
    } else {
        Client::ReceiveMessage(remote, type, data);
    }
}




void
VRClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    // Notice("got reply ");
    if (pendingRequest == NULL) {
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqId) {
        Warning("Client %d Received reply for a different request from %d %s\n    was expecting: %d\n    got: %d", clientid, msg.replicaidx(), msg.reply().c_str(), pendingRequest->clientReqId, msg.clientreqid());
        return;
    }

    if(replicas.size()==0 && gotAck==false) {
        //add in replicas based on config from replica
        for(int i =0; i<msg.n(); i++){
            if(specpaxos::IsReplica(i)){
                replicas.push_back(i);
            }
        }
    }

    int srcAddr = msg.replicaidx();
    auto it = std::find(replicas.begin(), replicas.end(), srcAddr); 
    if (it != replicas.end()) { 
        replicas.erase(it); 
    } 
    if (replicas.size()==0){
        this->gotAck = true;
    }

    std::ostringstream oss;
    for (int replica : replicas) {
        oss << replica << " ";
    }
    // Notice("Client %d received paxosReply for %d from %d - still need %s", clientid, msg.clientreqid(), msg.replicaidx(), oss.str().c_str());




    this->msg = new specpaxos::vr::proto::ReplyMessage(msg);
    if(this->gotAck && pendingRequest != NULL){
        // Notice("got reply and ack");
        requestTimeout->Stop();
        PendingRequest *req = pendingRequest;
        pendingRequest = NULL;
        req->continuation(req->request, this->msg->reply());
        this->msg = NULL;
        delete req;
        // Notice("client %d finished request %d", clientid, req->clientReqId);
    }else{
        // Notice("got reply and no ack");
        // Warning("in handle reply for %s but no ack", this->msg->reply().c_str());
        // if(replicas.size()>0){
        //     string str;
        //     for (size_t i = 0; i < replicas.size(); ++i) {
        //         if (i != 0) {
        //             str += ",";
        //         }
        //         str += replicas[i];
        //     }
           
        //     Warning("Waiting for acks from: %s", str);
        // }
        
    }

}

void
VRClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }
    
    Debug("Client received unloggedReply");

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;
    
    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::HandlePaxosAck(const TransportAddress &remote,
                              const proto::PaxosAck &msg)
{
    // Notice("Client received paxosAck from %d", msg.replicaidx());


    if (pendingRequest==NULL || msg.clientreqid() != pendingRequest->clientReqId) {
        return;
    }

    if(replicas.size()==0 && gotAck==false) {
        //add in replicas based on config from replica
        for(int i =0; i<msg.n(); i++){
            if(specpaxos::IsReplica(i)){
                replicas.push_back(i);
            }
        }
    }

    // const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    // int srcAddr = simRemote.GetAddr();
    // // Notice("got ack from %d", srcAddr);

    int srcAddr = msg.replicaidx();
    auto it = std::find(replicas.begin(), replicas.end(), srcAddr); 
    if (it != replicas.end()) { 
        replicas.erase(it); 
        // Warning("got ack from %d - still have %d replicas", srcAddr, replicas.size());
    } 

    
    std::ostringstream oss;
    for (int replica : replicas) {
        oss << replica << " ";
    }
    // Notice("Client %d received paxosAck for %d from %d - still need %s", clientid, msg.clientreqid(), msg.replicaidx(), oss.str().c_str());



    if (replicas.size()==0){
        this->gotAck = true;
    }

    if(this->gotAck && this->msg != NULL && pendingRequest != NULL){
        requestTimeout->Stop();
        // Warning("finished with %s with id %d in ack\n\n\n", this->msg->reply().c_str(), pendingRequest->clientReqId);
        PendingRequest *req = pendingRequest;
        pendingRequest = NULL;
        req->continuation(req->request, this->msg->reply());
        this->msg = NULL;
        delete req;
        // Notice("client %d finished request %d", clientid, req->clientReqId);
    }

}

void
VRClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    // Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();
    
    req->timeoutContinuation(req->request);
}

} // namespace vr
} // namespace specpaxos
