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
#include "common/replica.h"


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
    
    requestTimeout = new Timeout(transport, 7000, [this]() {
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
    // reqMsg.set_reqstr(pendingRequest->request);
    
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
    //replicas are odd numbered (but zero-index so start at 0)
    for(int i=0; i<config.n; i+=2){
        if(IsWitness(i)){
            Panic(" designed as witness ");
        }

        if (!(transport->SendMessageToReplica(this,
                                            i,
                                            msg))) {
            return false;
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

    const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    int srcAddr = simRemote.GetAddr();

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
    if (pendingRequest == NULL) {
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqId) {
        Warning("Received reply for a different request %s\n    was expecting: %d\n    got: %d", msg.reply().c_str(), pendingRequest->clientReqId, msg.clientreqid());
        return;
    }

    Debug("Client received reply");

    this->msg = new specpaxos::vr::proto::ReplyMessage(msg);
    if(this->gotAck && pendingRequest != NULL){
        requestTimeout->Stop();
        PendingRequest *req = pendingRequest;
        pendingRequest = NULL;
        req->continuation(req->request, this->msg->reply());
        this->msg = NULL;
        delete req;
    }else{
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
    Debug("Client received paxosAck");

    if (pendingRequest==NULL || msg.clientreqid() != pendingRequest->clientReqId) {
        return;
    }

    if(replicas.size()==0) {
        //add in replicas based on config from replica
        for(int i =0; i<msg.n(); i++){
            if(specpaxos::IsReplica(i)){
                replicas.push_back(i);
            }
        }
    }

    const SimulatedTransportAddress& simRemote = dynamic_cast<const SimulatedTransportAddress&>(remote);
    int srcAddr = simRemote.GetAddr();

    auto it = std::find(replicas.begin(), replicas.end(), srcAddr); 
    if (it != replicas.end()) { 
        replicas.erase(it); 
        // Warning("got ack from %d - still have %d replicas", srcAddr, replicas.size());
    } 

    if (replicas.size()==1){
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
    }

}

void
VRClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();
    
    req->timeoutContinuation(req->request);
}

} // namespace vr
} // namespace specpaxos
