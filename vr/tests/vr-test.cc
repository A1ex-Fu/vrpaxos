// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr-test.cc:
 *   test cases for Viewstamped Replication protocol
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

 #include "lib/configuration.h"
 #include "lib/message.h"
 #include "lib/transport.h"
 #include "lib/simtransport.h"
 
 #include "common/client.h"
 #include "common/replica.h"
 #include "vr/client.h"
 #include "vr/replica.h"
 #include "vr/witness.h"
 
 #include <stdlib.h>
 #include <stdio.h>
 #include <gtest/gtest.h>
 #include <vector>
 #include <sstream>
 
 static string replicaLastOp;
 static string clientLastOp;
 static string clientLastReply;
 
 using google::protobuf::Message;
 using namespace specpaxos;
 using namespace specpaxos::vr;
 using namespace specpaxos::vr::proto;
 
 class VRTestApp : public AppReplica
 {
 public:
     VRTestApp() { };
     virtual ~VRTestApp() { };
 
     virtual void ReplicaUpcall(opnum_t opnum, const string &req, string &reply) {
         ops.push_back(req);
         reply = "reply: " + req;
     }
      
     virtual void UnloggedUpcall(const string &req, string &reply) {
         unloggedOps.push_back(req);
         reply = "unlreply: " + req;
     }
 
     std::vector<string> ops;
     std::vector<string> unloggedOps;
 
 };
 
 class VRTest : public  ::testing::TestWithParam<int>
 {
 protected:
     std::vector<VRTestApp *> apps;
     std::vector<Replica *> replicas;
     VRClient *client;
     SimulatedTransport *transport;
     Configuration *config;
     int requestNum;
     
     virtual void SetUp() {
         bool testingFiveNode = true;
         std::string folder = std::string("traces/");
         std::vector<ReplicaAddress> replicaAddrs =
             { { "localhost", "12345" },
               { "localhost", "12346" },
               { "localhost", "12347" }};
         config = new Configuration(3, 1, replicaAddrs);
 
         if(testingFiveNode){
             std::vector<ReplicaAddress> replicaAddrs =
                 { { "localhost", "12345" },
                 { "localhost", "12346" },
                 { "localhost", "12347" },
                 { "localhost", "12348" },
                 { "localhost", "12349" }};
             config = new Configuration(5, 2, replicaAddrs);
             folder = std::string("traces_5Node/");
         }
         transport = new SimulatedTransport();
         
         for (int i = 0; i < config->n; i++) {
             apps.push_back(new VRTestApp());
             if(IsWitness(i)){
                 // replicas.push_back(new VRWitness(*config, i, true, transport, apps[i])); 
 
                 auto witness = new VRWitness(*config, i, true, transport, apps[i]);
                 std::string formatted = ::testing::UnitTest::GetInstance()->current_test_info()->name() + std::string(".txt");
                 std::replace(formatted.begin(), formatted.end(), '/', '_');
                 formatted = folder + formatted;
             
                //  witness->UpdateTraceFile(formatted);
                 replicas.push_back(witness);
             }else{
                 replicas.push_back(new VRReplica(*config, i, true, transport, GetParam(), apps[i])); 
             }
         }
 
         client = new VRClient(*config, transport);
         requestNum = -1;
 
         // Only let tests run for a simulated minute. This prevents
         // infinite retry loops, etc.
 //        transport->Timer(60000, [&]() {
 //                transport->CancelAllTimers();
 //            });
     }
 
     virtual string RequestOp(int n) {
         std::ostringstream stream;
         stream << "test: " << n;
         return stream.str();
     }
 
     virtual string LastRequestOp() {
         return RequestOp(requestNum);
     }
     
     virtual void ClientSendNext(Client::continuation_t upcall) {
         requestNum++;
         client->Invoke(LastRequestOp(), upcall);
     }
 
     virtual void ClientSendNextUnlogged(int idx, Client::continuation_t upcall,
                                         Client::timeout_continuation_t timeoutContinuation = nullptr,
                                         uint32_t timeout = Client::DEFAULT_UNLOGGED_OP_TIMEOUT) {
         requestNum++;
         client->InvokeUnlogged(idx, LastRequestOp(), upcall, timeoutContinuation, timeout);
     }
     
     virtual void TearDown() {
         for (auto x : replicas) {
             delete x;
         }
         for (auto a : apps) {
             delete a;
         }
 
         apps.clear();
         replicas.clear();
 
         delete client;
         delete transport;
         delete config;
     }
 };
 
 
 
 
 
 
 TEST_P(VRTest, OneOp)
 {
     auto upcall = [this](const string &req, const string &reply) {
         EXPECT_EQ(req, LastRequestOp());
         EXPECT_EQ(reply, "reply: "+LastRequestOp());
 
         // Not guaranteed that any replicas except the leader have
         // executed this request.
         EXPECT_EQ(apps[0]->ops.back(), req);
         transport->CancelAllTimers();
     };
     
     ClientSendNext(upcall);
     transport->Run();
 
     // By now, they all should have executed the last request.
     for (int i = 0; i < config->n; i++) {
         std::cout<< i <<std::endl;
         if (IsWitness(i)) {
             continue;
         }
         EXPECT_EQ(apps[i]->ops.size(), 1);
         EXPECT_EQ(apps[i]->ops.back(),  LastRequestOp());
     }
 }
 
 TEST_P(VRTest, Unlogged)
 {
     auto upcall = [this](const string &req, const string &reply) {
         EXPECT_EQ(req, LastRequestOp());
         EXPECT_EQ(reply, "unlreply: "+LastRequestOp());
 
         EXPECT_EQ(apps[2]->unloggedOps.back(), req);
         transport->CancelAllTimers();
     };
     int timeouts = 0;
     auto timeout = [&](const string &req) {
         timeouts++;
     };
     
     ClientSendNextUnlogged(2, upcall, timeout);
     transport->Run();
 
     for (int i = 0; i < apps.size(); i++) {
         EXPECT_EQ(0, apps[i]->ops.size());
         EXPECT_EQ((i == 2 ? 1 : 0), apps[i]->unloggedOps.size());
     }
     EXPECT_EQ(0, timeouts);
 }
 
 TEST_P(VRTest, UnloggedTimeout)
 {
     auto upcall = [this](const string &req, const string &reply) {
         FAIL();
         transport->CancelAllTimers();
     };
     int timeouts = 0;
     auto timeout = [&](const string &req) {
         timeouts++;
     };
 
     // Drop messages to or from replica 1
     transport->AddFilter(10, [](TransportReceiver *src, int srcIdx,
                                 TransportReceiver *dst, int dstIdx,
                                 Message &m, uint64_t &delay) {
                              if ((srcIdx == 1) || (dstIdx == 1)) {
                                  return false;
                              }
                              return true;
                          });
 
     // Run for 10 seconds
     transport->Timer(10000, [&]() {
             transport->CancelAllTimers();
         });
 
     ClientSendNextUnlogged(1, upcall, timeout);
     transport->Run();
 
     for (int i = 0; i < apps.size(); i++) {
         EXPECT_EQ(0, apps[i]->ops.size());
         EXPECT_EQ(0, apps[i]->unloggedOps.size());
     }
     EXPECT_EQ(1, timeouts);
 }
 
 
 TEST_P(VRTest, ManyOps)
 {
 
     Client::continuation_t upcall = [&](const string &req, const string &reply) {
         // Warning("expected: %s", LastRequestOp().c_str());
         // Warning("reply: %s", reply.c_str());
 
 
         EXPECT_EQ(req, LastRequestOp());
         EXPECT_EQ(reply, "reply: "+ LastRequestOp());
         
         // Not guaranteed that any replicas except the leader have
         // executed this request.
         EXPECT_EQ(apps[0]->ops.back(), req);
         if (requestNum < 9) {
             ClientSendNext(upcall);
         } else {
             transport->CancelAllTimers();
         }
     };
     
     ClientSendNext(upcall);
     transport->Run();
     
     // By now, they all should have executed the last request.
     
     for (int i = 0; i < config->n; i+=2) {
         EXPECT_EQ(10, apps[i]->ops.size());
         for (int j = 0; j < 10; j++) {
             EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);            
         }
     }
 }
 
 
 
 
 //TODO - decide what to do. Failed replica means that the min number of acks cannot be achieved
 TEST_P(VRTest, FailedReplica)
 {
     Warning("starting failed replica test");
     Client::continuation_t upcall = [&](const string &req, const string &reply) {
         EXPECT_EQ(req, LastRequestOp());
         EXPECT_EQ(reply, "reply: "+LastRequestOp());
 
         // Not guaranteed that any replicas except the leader have
         // executed this request
         EXPECT_EQ(apps[0]->ops.back(), req);
 
         if (requestNum < 9) {
             ClientSendNext(upcall);
         } else {
             transport->CancelAllTimers();
         }
     };
     
     ClientSendNext(upcall);
 
     // Drop messages to or from replica 1
     transport->AddFilter(10, [](TransportReceiver *src, int srcIdx,
                                 TransportReceiver *dst, int dstIdx,
                                 Message &m, uint64_t &delay) {
                              if ((srcIdx == 1) || (dstIdx == 1)) {
                                  return false;
                              }
                              return true;
                          });
     
     transport->Run();
 
     // By now, they all should have executed the last request.
     // for (int i = 0; i < config->n; i++) {
     //     if (i == 1) {
     //         continue;
     //     }
     //     EXPECT_EQ(10, apps[i]->ops.size());
     //     for (int j = 0; j < 10; j++) {
     //         EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);            
     //     }
     // }
 
     for (int i = 0; i < config->n; i++) {
         if (!IsWitness(i)) {
             // Replicas should have executed these ops
             EXPECT_EQ(10, apps[i]->ops.size());
             for (int j = 0; j < 10; j++) {
                 EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);            
             }
             // Non-failed replicas should have full log
             EXPECT_EQ(10, static_cast<VRReplica *>(replicas[i])->GetLogSize());
         }
     }
 }
 
 // TEST_P(VRTest, StateTransfer)
 // {
 //     Client::continuation_t upcall = [&](const string &req, const string &reply) {
 //         EXPECT_EQ(req, LastRequestOp());
 //         EXPECT_EQ(reply, "reply: "+LastRequestOp());
 
 //         // Not guaranteed that any replicas except the leader have
 //         // executed this request.
 //         EXPECT_EQ(apps[0]->ops.back(), req);
 
 //         if (requestNum == 5) {
 //             // Restore replica 1
 //             transport->RemoveFilter(10);
 //         }
 
 //         if (requestNum < 9) {
 //             ClientSendNext(upcall);
 //         } else {
 //             transport->CancelAllTimers();
 //         }
 //     };
     
 //     ClientSendNext(upcall);
 
 //     // Drop messages to or from replica 1
 //     transport->AddFilter(10, [](TransportReceiver *src, int srcIdx,
 //                                 TransportReceiver *dst, int dstIdx,
 //                                 Message &m, uint64_t &delay) {
 //                              if ((srcIdx == 2) || (dstIdx == 2)) {
 //                                  return false;
 //                              }
 //                              return true;
 //                          });
     
 //     transport->Run();
 
 //     // By now, they all should have executed the last request.
 //     for (int i = 0; i < config->n; i++) {
 //         if (!IsWitness(i)) {
 // 			EXPECT_EQ(10, apps[i]->ops.size());
 // 			for (int j = 0; j < 10; j++) {
 // 				EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);            
 // 			}
 // 			EXPECT_EQ(2, static_cast<VRReplica *>(replicas[i])->GetLogSize());
 // 		} else {
 // 			EXPECT_EQ(2, static_cast<VRWitness *>(replicas[i])->GetLogSize());
 // 		}
 //     }
 // }
 
 
 // TEST_P(VRTest, FailedLeader)
 // {
 //     Client::continuation_t upcall = [&](const string &req, const string &reply) {
 //         EXPECT_EQ(req, LastRequestOp());
 //         EXPECT_EQ(reply, "reply: "+LastRequestOp());
 
 //         if (requestNum == 5) {
 //             // Drop messages to or from replica 0
 //             transport->AddFilter(10, [](TransportReceiver *src, int srcIdx,
 //                                         TransportReceiver *dst, int dstIdx,
 //                                         Message &m, uint64_t &delay) {
 //                                      if ((srcIdx == 0) || (dstIdx == 0)) {
 //                                          return false;
 //                                      }
 //                                      return true;
 //                                  });
 //         }
 //         if (requestNum < 9) {
 //             ClientSendNext(upcall);
 //         } else {
 //             transport->CancelAllTimers();
 //         }
 //     };
     
 //     ClientSendNext(upcall);
     
 //     transport->Run();
 
 //     // By now, they all should have executed the last request.
 //     for (int i = 0; i < config->n; i++) {
 //         if (i == 0) {
 //             continue;
 //         }
 //         EXPECT_EQ(10, apps[i]->ops.size());
 //         for (int j = 0; j < 10; j++) {
 //             EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);            
 //         }
 //     }
 // }
 
 TEST_P(VRTest, DroppedReply)
 {
     bool received = false;
     Client::continuation_t upcall = [&](const string &req, const string &reply) {
         EXPECT_EQ(req, LastRequestOp());
         EXPECT_EQ(reply, "reply: "+LastRequestOp());
         transport->CancelAllTimers();
         received = true;
     };
 
     // Drop the first ReplyMessage
     bool dropped = false;
     transport->AddFilter(10, [&dropped](TransportReceiver *src, int srcIdx,
                                         TransportReceiver *dst, int dstIdx,
                                         Message &m, uint64_t &delay) {
                              ReplyMessage r;
                              if (m.GetTypeName() == r.GetTypeName()) {
                                  if (!dropped) {
                                      dropped = true;
                                      return false;
                                  }
                              }
                              return true;
                          });
     ClientSendNext(upcall);
     
     transport->Run();
 
     EXPECT_TRUE(received);
     
     // Each replica should have executed only one request
     for (int i = 0; i < config->n; i++) {
         if (IsWitness(i)) {
             continue;
         }
         Notice("chekcing app %d", i);
         EXPECT_EQ(1, apps[i]->ops.size());
    }
 }
 
 // TEST_P(VRTest, DroppedReplyThenFailedLeader)
 // {
 //     bool received = false;
 //     Client::continuation_t upcall = [&](const string &req, const string &reply) {
 //         EXPECT_EQ(req, LastRequestOp());
 //         EXPECT_EQ(reply, "reply: "+LastRequestOp());
 //         transport->CancelAllTimers();
 //         received = true;
 //     };
 
 //     // Drop the first ReplyMessage
 //     bool dropped = false;
 //     transport->AddFilter(10, [&dropped](TransportReceiver *src, int srcIdx,
 //                                         TransportReceiver *dst, int dstIdx,
 //                                         Message &m, uint64_t &delay) {
 //                              ReplyMessage r;
 //                              if (m.GetTypeName() == r.GetTypeName()) {
 //                                  if (!dropped) {
 //                                      dropped = true;
 //                                      return false;
 //                                  }
 //                              }
 //                              return true;
 //                          });
 
 //     // ...and after we've done that, fail the leader altogether
 //     transport->AddFilter(20, [&dropped](TransportReceiver *src, int srcIdx,
 //                                         TransportReceiver *dst, int dstIdx,
 //                                         Message &m, uint64_t &delay) {
 //                              if ((srcIdx == 0) || (dstIdx == 0)) {
 //                                  return !dropped;
 //                              }
 //                              return true;
 //                          });
     
 //     ClientSendNext(upcall);
     
 //     transport->Run();
 
 //     EXPECT_TRUE(received);
     
 //     // Each replica should have executed only one request
 //     // (and actually the faulty one should too, but don't check that)
 //     for (int i = 0; i < config->n; i++) {
 //         if (i != 0) {
 //             EXPECT_EQ(1, apps[i]->ops.size());            
 //         }
 //     }
 // }
 
 TEST_P(VRTest, ManyClients)
 {
     const int NUM_CLIENTS = 10;
     const int MAX_REQS = 100;
     
     std::vector<VRClient *> clients;
     std::vector<int> lastReq;
     std::vector<Client::continuation_t> upcalls;
     for (int i = 0; i < NUM_CLIENTS; i++) {
         clients.push_back(new VRClient(*config, transport));
         lastReq.push_back(0);
         upcalls.push_back([&, i](const string &req, const string &reply) {
                 EXPECT_EQ("reply: "+RequestOp(lastReq[i]), reply);
                 lastReq[i] += 1;
                 if (lastReq[i] < MAX_REQS) {
                     clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
                 }
             });
         clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
     }
 
     // This could take a while; simulate two hours
     transport->Timer(7200000, [&]() {
             transport->CancelAllTimers();
         });
 
     transport->Run();
 
     for (int i = 0; i < config->n; i++) {
         if (IsWitness(i)) {
             continue;
         }
         ASSERT_EQ(NUM_CLIENTS * MAX_REQS, apps[i]->ops.size());
     }
 
     for (int i = 0; i < NUM_CLIENTS*MAX_REQS; i++) {
         for (int j = 0; j < config->n; j++) {
                 if (IsWitness(j)) {
                     continue;
                 }
             ASSERT_EQ(apps[0]->ops[i], apps[j]->ops[i]);
         }
     }
 
     for (VRClient *c : clients) {
         delete c;
     }
 }
 
 // TEST_P(VRTest, Recovery)
 // {
 //     Client::continuation_t upcall = [&](const string &req, const string &reply) {
 //         EXPECT_EQ(req, LastRequestOp());
 //         EXPECT_EQ(reply, "reply: "+LastRequestOp());
 
 //         if (requestNum == 5) {
 //             // Drop messages to or from replica 0
 //             transport->AddFilter(10, [](TransportReceiver *src, int srcIdx,
 //                                         TransportReceiver *dst, int dstIdx,
 //                                         Message &m, uint64_t &delay) {
 //                                      if ((srcIdx == 0) || (dstIdx == 0)) {
 //                                          return false;
 //                                      }
 //                                      return true;
 //                                  });
 //         }
 //         if (requestNum == 7) {
 //             // Destroy and recover replica 0
 //             delete apps[0];
 //             delete replicas[0];
 //             transport->RemoveFilter(10);
 //             apps[0] = new VRTestApp();
 //             replicas[0] = new VRReplica(*config, 0, false,
 //                                         transport, GetParam(), apps[0]);
 //         }
 //         if (requestNum < 9) {
 //             transport->Timer(10000, [&]() {
 //                     ClientSendNext(upcall);
 //                 });
 //         } else {
 //             transport->CancelAllTimers();
 //         }
 //     };
     
 //     ClientSendNext(upcall);
     
 //     transport->Run();
 
 //     // By now, they all should have executed the last request,
 //     // including the recovered replica 0
 //     for (int i = 0; i < config->n; i++) {
 //         EXPECT_EQ(10, apps[i]->ops.size());
 //         for (int j = 0; j < 10; j++) {
 //             EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);            
 //         }
 //     }
 // }
 
 
 
 
 
 
 
 
 
 
 
 
 TEST_P(VRTest, Stress)
 {

    // const int NUM_CLIENTS = 1;
    // const int MAX_REQS = 5;
     const int NUM_CLIENTS = 10;
     const int MAX_REQS = 100;
     const int MAX_DELAY = 1;
     const int DROP_PROBABILITY = 10; // 1/x
     
     std::vector<VRClient *> clients;
     std::vector<int> lastReq;
     std::vector<Client::continuation_t> upcalls;
     for (int i = 0; i < NUM_CLIENTS; i++) {
         clients.push_back(new VRClient(*config, transport));
         lastReq.push_back(0);
         upcalls.push_back([&, i](const string &req, const string &reply) {
                 EXPECT_EQ("reply: "+RequestOp(lastReq[i]), reply);
                 lastReq[i] += 1;
                 if (lastReq[i] < MAX_REQS) {
                     clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
                 }
             });
         clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
     }
 
     srand(time(NULL));
     
     // Delay messages from clients by a random amount, and drop some
     // of them
     transport->AddFilter(10, [=](TransportReceiver *src, int srcIdx,
                                 TransportReceiver *dst, int dstIdx,
                                 Message &m, uint64_t &delay) {
                              if (srcIdx == -1) {
                                  delay = rand() % MAX_DELAY;
                              }
                              return ((rand() % DROP_PROBABILITY) != 0);
                          });
     
     // // This could take a while; simulate two hours
     transport->Timer(7200000, [&]() {
             transport->CancelAllTimers();
         });
 
     transport->Run();
 
     for (int i = 0; i < config->n; i++) {
         if (IsWitness(i)) {
             continue;
         }
 
         Warning("%d only has opsize of %d", i, apps[i]->ops.size());
         if(apps[i]->ops.size()<1000){
             for (size_t i = 0; i < apps.size(); ++i) {
                 std::cerr << "App " << i << " ops:" << std::endl;
                 int j = 0;
                 for (const auto &op : apps[i]->ops) {
                     std::cerr << j << "th op: " << op << std::endl;
                     j++;
                 }
             }
         }
 
         ASSERT_EQ(NUM_CLIENTS * MAX_REQS, apps[i]->ops.size());
     }
 
     for (int i = 0; i < NUM_CLIENTS*MAX_REQS; i++) {
         for (int j = 0; j < config->n; j++) {
             if (IsWitness(j)) {
                 continue;
             }
 
             ASSERT_EQ(apps[0]->ops[i], apps[j]->ops[i]);
         }
     }
 
     for (VRClient *c : clients) {
         delete c;
     }
 }
 
 
 
 
 
 
 
 
 
 
 
 
 
 TEST_P(VRTest, StressDropClientReqs)
 {
     const int NUM_CLIENTS = 10;
     const int MAX_REQS = 1000;
     const int MAX_DELAY = 1;
     const int DROP_PROBABILITY = 10; // 1/x
     
     std::vector<VRClient *> clients;
     std::vector<int> lastReq;
     std::vector<Client::continuation_t> upcalls;
     for (int i = 0; i < NUM_CLIENTS; i++) {
         clients.push_back(new VRClient(*config, transport));
         lastReq.push_back(0);
         upcalls.push_back([&, i](const string &req, const string &reply) {
                 EXPECT_EQ("reply: "+RequestOp(lastReq[i]), reply);
                 lastReq[i] += 1;
                 if (lastReq[i] < MAX_REQS) {
                     clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
                 }
             });
         clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
     }
 
     srand(time(NULL));
     
     // Delay messages from clients by a random amount, and drop some
     // of them
     transport->AddFilter(10, [=](TransportReceiver *src, int srcIdx,
                                 TransportReceiver *dst, int dstIdx,
                                 Message &m, uint64_t &delay) {
                              if (srcIdx == -1) {
                                  delay = rand() % MAX_DELAY;
                              }
                              return ((rand() % DROP_PROBABILITY) != 0);
                          });
     
     // This could take a while; simulate two hours
     transport->Timer(72000000, [&]() {
             transport->CancelAllTimers();
         });
 
     transport->Run();
 
     for (int i = 0; i < config->n; i++) {
         if (IsWitness(i)) {
             continue;
         }
         ASSERT_EQ(NUM_CLIENTS * MAX_REQS, apps[i]->ops.size());
     }
 
     for (int i = 0; i < NUM_CLIENTS*MAX_REQS; i++) {
         for (int j = 0; j < config->n; j++) {
             if (IsWitness(j)) {
                 continue;
             }
             ASSERT_EQ(apps[0]->ops[i], apps[j]->ops[i]);
         }
     }
 
     for (VRClient *c : clients) {
         delete c;
     }
 }
 
 
 
 TEST_P(VRTest, StressDropNodeReqs)
 {
     const int NUM_CLIENTS = 10;
     const int MAX_REQS = 1000;
     const int MAX_DELAY = 1;
     const int DROP_PROBABILITY = 10; // 1/x
     
     std::vector<VRClient *> clients;
     std::vector<int> lastReq;
     std::vector<Client::continuation_t> upcalls;
     for (int i = 0; i < NUM_CLIENTS; i++) {
         clients.push_back(new VRClient(*config, transport));
         lastReq.push_back(0);
         upcalls.push_back([&, i](const string &req, const string &reply) {
                 EXPECT_EQ("reply: "+RequestOp(lastReq[i]), reply);
                 lastReq[i] += 1;
                 if (lastReq[i] < MAX_REQS) {
                     clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
                 }
             });
         clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
     }
 
     srand(time(NULL));
     
     // Delay messages from nodes by a random amount, and drop some
     // of them
     transport->AddFilter(10, [=](TransportReceiver *src, int srcIdx,
                                 TransportReceiver *dst, int dstIdx,
                                 Message &m, uint64_t &delay) {
                              if (srcIdx > -1) {
                                  delay = rand() % MAX_DELAY;
                              }
                              return ((rand() % DROP_PROBABILITY) != 0);
                          });
     
     // This could take a while; simulate two hours
     transport->Timer(72000000, [&]() {
             transport->CancelAllTimers();
         });
 
     transport->Run();
 
     for (int i = 0; i < config->n; i++) {
         if (IsWitness(i)) {
             continue;
         }
         ASSERT_EQ(NUM_CLIENTS * MAX_REQS, apps[i]->ops.size());
     }
 
     for (int i = 0; i < NUM_CLIENTS*MAX_REQS; i++) {
         for (int j = 0; j < config->n; j++) {
             if (IsWitness(j)) {
                 continue;
             }
             ASSERT_EQ(apps[0]->ops[i], apps[j]->ops[i]);
         }
     }
 
     for (VRClient *c : clients) {
         delete c;
     }
 }
 
 TEST_P(VRTest, StressDropAnyReqs)
 {
     const int NUM_CLIENTS = 10;
     const int MAX_REQS = 1000;
     const int DROP_PROBABILITY = 10; // 1/x
     
     std::vector<VRClient *> clients;
     std::vector<int> lastReq;
     std::vector<Client::continuation_t> upcalls;
     for (int i = 0; i < NUM_CLIENTS; i++) {
         clients.push_back(new VRClient(*config, transport, i));
         lastReq.push_back(0);
         upcalls.push_back([&, i](const string &req, const string &reply) {
                 EXPECT_EQ("reply: "+RequestOp(lastReq[i]), reply);
                 lastReq[i] += 1;
                 if (lastReq[i] < MAX_REQS) {
                     clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
                 }
             });
         clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
     }
     int dropIdx = std::numeric_limits<int>::max();  // Invalid dropIdx means drop nothing
     auto t = time(NULL);
     srand(t);
     Notice("Seed: %lu", t); 
     
     // Delay messages from clients by a random amount, and drop some
     // of them
     transport->AddFilter(10, [&dropIdx](TransportReceiver *src, int srcIdx,
                                 TransportReceiver *dst, int dstIdx,
                                 Message &m, uint64_t &delay) {
                              auto p = rand() % DROP_PROBABILITY;
                              if (dropIdx == std::numeric_limits<int>::max() && p == 0) {
                                  // Maybe drop this src's packets for a bit 
                                 dropIdx = srcIdx; 
                              } else if (dropIdx == srcIdx && p > 0) {
                                  // Stop dropping src's packets
                                 dropIdx = std::numeric_limits<int>::max(); 
                              }
                              return dropIdx != srcIdx;
                          });
     
     // This could take a while; simulate two hours
     transport->Timer(72000000, [&]() {
             transport->CancelAllTimers();
         });
 
     transport->Run();
 
     for (int i = 0; i < config->n; i++) {
         if (IsWitness(i)) {
             continue;
         }
         ASSERT_EQ(NUM_CLIENTS * MAX_REQS, apps[i]->ops.size());
     }
 
     for (int i = 0; i < NUM_CLIENTS*MAX_REQS; i++) {
         for (int j = 0; j < config->n; j++) {
             if (IsWitness(j)) {
                 continue;
             }
             ASSERT_EQ(apps[0]->ops[i], apps[j]->ops[i]);
         }
     }
 
     for (VRClient *c : clients) {
         delete c;
     }
 }
 
 
 
 
 
 INSTANTIATE_TEST_CASE_P(Batching,
                         VRTest,
                         ::testing::Values(1, 8));