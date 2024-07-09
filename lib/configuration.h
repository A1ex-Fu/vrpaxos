// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * configuration.h:
 *   Representation of a replica group configuration, i.e. the number
 *   and list of replicas in the group
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

#ifndef _LIB_CONFIGURATION_H_
#define _LIB_CONFIGURATION_H_

#include "lib/viewstamp.h"

#include <fstream>
#include <stdbool.h>
#include <string>
#include <vector>

using std::string;

namespace specpaxos {

struct ReplicaAddress
{
    string host;
    string port;
    ReplicaAddress(const string &host, const string &port);
    bool operator==(const ReplicaAddress &other) const;

    inline bool operator!=(const ReplicaAddress &other) const {
        return !(*this == other);
    }
};


class Configuration
{
public:
    Configuration(const Configuration &c);
    Configuration(int n, int f, std::vector<ReplicaAddress> replicas,
                  ReplicaAddress *multicastAddress = nullptr);
    Configuration(std::ifstream &file);
    virtual ~Configuration();
    ReplicaAddress replica(int idx) const;
    ReplicaAddress witness(int idx) const;
    ReplicaAddress idle(int idx) const;
    const ReplicaAddress *multicast() const;
    inline int GetLeaderIndex(view_t view) const {
        return (view % n);
    };
    int QuorumSize() const;
    int FastQuorumSize() const;
    bool operator==(const Configuration &other) const;
    inline bool operator!= (const Configuration &other) const {
        return !(*this == other);
    }
    
public:
    int n;                      // number of servers
    int numIdle = 2;            // number of idle servers
    int f;                      // number of failures tolerated

    std::vector<ReplicaAddress> replicaAddresses;
    std::vector<ReplicaAddress> witnessAddresses;
    std::vector<ReplicaAddress> idleAddresses;

private:
    //TODO: check if we need multicast addresses
    // currently planning on ignoring them - seems like multicast is more of an optimization than a necessity
    ReplicaAddress *multicastAddress;
    bool hasMulticast;
};

}      // namespace specpaxos

namespace std {
template <> struct hash<specpaxos::ReplicaAddress>
{
    size_t operator()(const specpaxos::ReplicaAddress & x) const
        {
            return hash<string>()(x.host) * 37 + hash<string>()(x.port);
        }
};
}

// namespace std {
// template <> struct hash<specpaxos::Configuration>
// {
//     size_t operator()(const specpaxos::Configuration & x) const
//         {
//             size_t out = 0;
//             out = x.n * 37 + x.f;
//             for (int i = 0; i < x.n; i++) {
//                 out *= 37;
//                 out += hash<specpaxos::ReplicaAddress>()(x.replica(i));
//             }
//             return out;
//         }
// };
// }

namespace std {
template <> struct hash<specpaxos::Configuration>
{
    size_t operator()(const specpaxos::Configuration & x) const
    {
        size_t out = 0;
        out = x.n * 37 + x.f;
        int numReplicas = 0;
        int numWitness = 0;
        for (int i = 0; i < x.n; i++) {
            out *= 37;
            if(i<x.numIdle){
                out += hash<specpaxos::ReplicaAddress>()(x.idle(i));
            }else{
                if((x.numIdle + (numReplicas + numWitness))%2==(x.numIdle%2)){
                    out += hash<specpaxos::ReplicaAddress>()(x.replica(numReplicas));
                    numReplicas++;
                }else{
                    out += hash<specpaxos::ReplicaAddress>()(x.witness(numWitness));
                    numWitness++;
                }
            }
        }
        return out;
    }
};
}



#endif  /* _LIB_CONFIGURATION_H_ */
