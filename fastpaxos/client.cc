// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fastpaxos/client.cc:
 *   Fast Paxos client
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
#include "fastpaxos/client.h"
#include "fastpaxos/fastpaxos-proto.pb.h"

namespace specpaxos {
namespace fastpaxos {

FastPaxosClient::FastPaxosClient(const Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, transport, clientid)
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqId = 0;
    
    requestTimeout = new Timeout(transport, 7000, [this]() {
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
}

FastPaxosClient::~FastPaxosClient()
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
FastPaxosClient::Invoke(const string &request,
                 continuation_t continuation)
{
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    pendingRequest = new PendingRequest(request, reqId, continuation);

    SendRequest();
}

void
FastPaxosClient::InvokeUnlogged(int replicaIdx,
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
FastPaxosClient::SendRequest()
{
    proto::RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqId);
    
    // XXX Try sending only to (what we think is) the leader first
    transport->SendMessageToAll(this, reqMsg);
    
    requestTimeout->Reset();
}

void
FastPaxosClient::ResendRequest()
{
    Warning("Client timeout; resending request");
    SendRequest();
}


void
FastPaxosClient::ReceiveMessage(const TransportAddress &remote,
                         const string &type,
                         const string &data)
{
    static proto::ReplyMessage reply;
    static proto::UnloggedReplyMessage unloggedReply;
    
    if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else if (type == unloggedReply.GetTypeName()) {
        unloggedReply.ParseFromString(data);
        HandleUnloggedReply(remote, unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data);
    }
}

void
FastPaxosClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    Debug("Client received reply for " FMT_VIEWSTAMP, msg.view(), msg.opnum());
    if (pendingRequest == NULL) {
        Debug("Received reply when no request was pending");
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqId) {
        Debug("Received reply for a different request");
        return;
    }
    
    requestTimeout->Stop();

    PendingRequest *req = pendingRequest;
    pendingRequest = NULL;
    
    req->continuation(req->request, msg.reply());
    delete req;
}

void
FastPaxosClient::HandleUnloggedReply(const TransportAddress &remote,
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
FastPaxosClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    // Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();
    
    req->timeoutContinuation(req->request);
}

} // namespace vr
} // namespace specpaxos
