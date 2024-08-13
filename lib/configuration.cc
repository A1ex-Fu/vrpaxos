// // -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// /***********************************************************************
//  *
//  * configuration.cc:
//  *   Representation of a replica group configuration, i.e. the number
//  *   and list of replicas in the group
//  *
//  * Copyright 2013-2016 Dan R. K. Ports  <drkp@cs.washington.edu>
//  *
//  * Permission is hereby granted, free of charge, to any person
//  * obtaining a copy of this software and associated documentation
//  * files (the "Software"), to deal in the Software without
//  * restriction, including without limitation the rights to use, copy,
//  * modify, merge, publish, distribute, sublicense, and/or sell copies
//  * of the Software, and to permit persons to whom the Software is
//  * furnished to do so, subject to the following conditions:
//  *
//  * The above copyright notice and this permission notice shall be
//  * included in all copies or substantial portions of the Software.
//  *
//  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//  * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//  * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//  * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
//  * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
//  * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
//  * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  * SOFTWARE.
//  *
//  **********************************************************************/

// #include "lib/assert.h"
// #include "lib/configuration.h"
// #include "lib/message.h"

// #include <iostream>
// #include <fstream>
// #include <string>
// #include <string.h>

// namespace specpaxos {

// ReplicaAddress::ReplicaAddress(const string &host, const string &port)
//     : host(host), port(port)
// {

// }

// bool
// ReplicaAddress::operator==(const ReplicaAddress &other) const {
//     return ((host == other.host) &&
//             (port == other.port));
// }


// Configuration::Configuration(const Configuration &c)
//     : n(c.n), f(c.f), hasMulticast(c.hasMulticast)
// {
//     multicastAddress = NULL;
//     if (hasMulticast) {
//         multicastAddress = new ReplicaAddress(*c.multicastAddress);
//     }

//     numIdle = c.idleAddresses.size();
//     int totalAddresses = c.replicaAddresses.size() + c.witnessAddresses.size();

//     for (int i = 0; i < totalAddresses; i++) {
//         if (i < numIdle) {
//             idleAddresses.push_back(c.replicaAddresses[i]);
//         } else if ((idleAddresses.size() + replicaAddresses.size() + witnessAddresses.size())%2==(idleAddresses.size()%2)) {
//             replicaAddresses.push_back(c.replicaAddresses[i]);
//         } else {
//             witnessAddresses.push_back(c.replicaAddresses[i]);
//         }
//     }
// }
    
// Configuration::Configuration(int n, int f,
//                              std::vector<ReplicaAddress> replicas,
//                              ReplicaAddress *multicastAddress)
//     : n(n), f(f)
// {
//     if (multicastAddress) {
//         hasMulticast = true;
//         this->multicastAddress =
//             new ReplicaAddress(*multicastAddress);
//     } else {
//         hasMulticast = false;
//         multicastAddress = NULL;
//     }

//     int totalAddresses = replicas.size();

//     for (int i = 0; i < totalAddresses; i++) {
//         if (i < numIdle) {
//             idleAddresses.push_back(replicas[i]);
//         } else if ((idleAddresses.size() + replicaAddresses.size() + witnessAddresses.size())%2==(idleAddresses.size()%2)) {
//             replicaAddresses.push_back(replicas[i]);
//         } else {
//             witnessAddresses.push_back(replicas[i]);
//         }
//     }

// }

// Configuration::Configuration(std::ifstream &file)
// {
//     f = -1;
//     hasMulticast = false;
//     multicastAddress = NULL;
    
//     while (!file.eof()) {
//         // Read a line
//         string line;
//         getline(file, line);;

//         // Ignore comments
//         if ((line.size() == 0) || (line[0] == '#')) {
//             continue;
//         }

//         // Get the command
//         // This is pretty horrible, but C++ does promise that &line[0]
//         // is going to be a mutable contiguous buffer...
//         char *cmd = strtok(&line[0], " \t");

//         if (strcasecmp(cmd, "f") == 0) {
//             char *arg = strtok(NULL, " \t");
//             if (!arg) {
//                 Panic ("'f' configuration line requires an argument");
//             }
//             char *strtolPtr;
//             f = strtoul(arg, &strtolPtr, 0);
//             if ((*arg == '\0') || (*strtolPtr != '\0')) {
//                 Panic("Invalid argument to 'f' configuration line");
//             }
//         } else if (strcasecmp(cmd, "replica") == 0) {
//             //NOTE: we take replica to mean any node - just keeping it this way so we don't have to update testing infra as much
//             char *arg = strtok(NULL, " \t");
//             if (!arg) {
//                 Panic ("'replica' configuration line requires an argument");
//             }

//             char *host = strtok(arg, ":");
//             char *port = strtok(NULL, "");
            
//             if (!host || !port) {
//                 Panic("Configuration line format: 'replica host:port'");
//             }

//             //first numIdle nodes -> idle
//             if(idleAddresses.size() < numIdle){
//                 idleAddresses.push_back(ReplicaAddress(string(host), string(port)));
//                 std::cout << "\nPushed another to idle - " + std::to_string(idleAddresses.size())  + "\n";
//             }else{
//                 //odd nodes -> replica
//                 //even nodes -> witness
//                 if((idleAddresses.size() + replicaAddresses.size() + witnessAddresses.size())%2==(idleAddresses.size()%2)){
//                     replicaAddresses.push_back(ReplicaAddress(string(host), string(port)));    
//                     std::cout << "\nPushed another to replica - " + std::to_string(replicaAddresses.size())  + "\n";
//                 }else{
//                     witnessAddresses.push_back(ReplicaAddress(string(host), string(port)));
//                     std::cout << "\nPushed another to witness - " + std::to_string(witnessAddresses.size())  + "\n";
//                 }
//             }
//         } else if (strcasecmp(cmd, "multicast") == 0) {
//             char *arg = strtok(NULL, " \t");
//             if (!arg) {
//                 Panic ("'multicast' configuration line requires an argument");
//             }

//             char *host = strtok(arg, ":");
//             char *port = strtok(NULL, "");
            
//             if (!host || !port) {
//                 Panic("Configuration line format: 'multicast host:port'");
//             }

//             multicastAddress = new ReplicaAddress(string(host),
//                                                   string(port));
//             hasMulticast = true;
//         } else {
//             Panic("Unknown configuration directive: %s", cmd);
//         }
//     }

//     n = replicaAddresses.size() + witnessAddresses.size();
//     if (n == 0) {
//         Panic("Configuration did not specify any replicas");
//     }

//     if (f == -1) {
//         Panic("Configuration did not specify a 'f' parameter");
//     }
// }

// Configuration::~Configuration()
// {
//     if (hasMulticast) {
//         delete multicastAddress;
//     }
// }

// ReplicaAddress
// Configuration::replica(int idx) const
// {
//     if(idx<replicaAddresses.size()){
//         return replicaAddresses[idx];
//     }
//     throw std::invalid_argument("Index " + std::to_string(idx) + " out of range with " + std::to_string(replicaAddresses.size()) + " replica addresses");
// }

// ReplicaAddress
// Configuration::witness(int idx) const
// {
//     if(idx<witnessAddresses.size()){
//         return witnessAddresses[idx];
//     }
//     throw std::invalid_argument("Index " + std::to_string(idx) + " out of range with " + std::to_string(witnessAddresses.size()) + " witness addresses");
// }

// ReplicaAddress
// Configuration::idle(int idx) const
// {
//     if(idx<idleAddresses.size()){
//         return idleAddresses[idx];
//     }
//     throw std::invalid_argument("Index " + std::to_string(idx) + " out of range with " + std::to_string(idleAddresses.size()) + " idle addresses");
// }

// const ReplicaAddress *
// Configuration::multicast() const
// {
//     if (hasMulticast) {
//         return multicastAddress;
//     } else {
//         return nullptr;
//     }
// }

// int
// Configuration::QuorumSize() const
// {
//     return f+1;
// }

// int
// Configuration::FastQuorumSize() const
// {
//     return f + (f+1)/2 + 1;
// }

// bool
// Configuration::operator==(const Configuration &other) const
// {
//     if ((n != other.n) ||
//         (f != other.f) ||
//         (replicaAddresses != other.replicaAddresses) ||
//         (idleAddresses != other.idleAddresses) ||
//         (witnessAddresses != other.witnessAddresses) ||
//         (hasMulticast != other.hasMulticast)) {
//         return false;
//     }

//     if (hasMulticast) {
//         if (*multicastAddress != *other.multicastAddress) {
//             return false;
//         }
//     }
    
//     return true;
// }

// } // namespace specpaxos



// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * configuration.cc:
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

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"

#include <iostream>
#include <fstream>
#include <string>
#include <string.h>

namespace specpaxos {

ReplicaAddress::ReplicaAddress(const string &host, const string &port)
    : host(host), port(port)
{

}

bool
ReplicaAddress::operator==(const ReplicaAddress &other) const {
    return ((host == other.host) &&
            (port == other.port));
}


Configuration::Configuration(const Configuration &c)
    : n(c.n), f(c.f), replicas(c.replicas), hasMulticast(c.hasMulticast)
{
    multicastAddress = NULL;
    if (hasMulticast) {
        multicastAddress = new ReplicaAddress(*c.multicastAddress);
    }
}
    
Configuration::Configuration(int n, int f,
                             std::vector<ReplicaAddress> replicas,
                             ReplicaAddress *multicastAddress)
    : n(n), f(f), replicas(replicas)
{
    if (multicastAddress) {
        hasMulticast = true;
        this->multicastAddress =
            new ReplicaAddress(*multicastAddress);
    } else {
        hasMulticast = false;
        multicastAddress = NULL;
    }
}

Configuration::Configuration(std::ifstream &file)
{
    f = -1;
    hasMulticast = false;
    multicastAddress = NULL;
    
    while (!file.eof()) {
        // Read a line
        string line;
        getline(file, line);;

        // Ignore comments
        if ((line.size() == 0) || (line[0] == '#')) {
            continue;
        }

        // Get the command
        // This is pretty horrible, but C++ does promise that &line[0]
        // is going to be a mutable contiguous buffer...
        char *cmd = strtok(&line[0], " \t");

        if (strcasecmp(cmd, "f") == 0) {
            char *arg = strtok(NULL, " \t");
            if (!arg) {
                Panic ("'f' configuration line requires an argument");
            }
            char *strtolPtr;
            f = strtoul(arg, &strtolPtr, 0);
            if ((*arg == '\0') || (*strtolPtr != '\0')) {
                Panic("Invalid argument to 'f' configuration line");
            }
        } else if (strcasecmp(cmd, "replica") == 0) {
            char *arg = strtok(NULL, " \t");
            if (!arg) {
                Panic ("'replica' configuration line requires an argument");
            }

            char *host = strtok(arg, ":");
            char *port = strtok(NULL, "");
            
            if (!host || !port) {
                Panic("Configuration line format: 'replica host:port'");
            }

            replicas.push_back(ReplicaAddress(string(host), string(port)));
        } else if (strcasecmp(cmd, "multicast") == 0) {
            char *arg = strtok(NULL, " \t");
            if (!arg) {
                Panic ("'multicast' configuration line requires an argument");
            }

            char *host = strtok(arg, ":");
            char *port = strtok(NULL, "");
            
            if (!host || !port) {
                Panic("Configuration line format: 'multicast host:port'");
            }

            multicastAddress = new ReplicaAddress(string(host),
                                                  string(port));
            hasMulticast = true;
        } else {
            Panic("Unknown configuration directive: %s", cmd);
        }
    }

    n = replicas.size();
    if (n == 0) {
        Panic("Configuration did not specify any replicas");
    }

  	if (n % 2 == 0) {
		Panic("No support (yet) for even number of replicas!"); 
	}

    if (f == -1) {
        Panic("Configuration did not specify a 'f' parameter");
    }
}

Configuration::~Configuration()
{
    if (hasMulticast) {
        delete multicastAddress;
    }
}

ReplicaAddress
Configuration::replica(int idx) const
{
    return replicas[idx];
}

const ReplicaAddress *
Configuration::multicast() const
{
    if (hasMulticast) {
        return multicastAddress;
    } else {
        return nullptr;
    }
}

int
Configuration::QuorumSize() const
{
    return f+1;
}

int
Configuration::FastQuorumSize() const
{
    return f + (f+1)/2 + 1;
}

bool
Configuration::operator==(const Configuration &other) const
{
    if ((n != other.n) ||
        (f != other.f) ||
        (replicas != other.replicas) ||
        (hasMulticast != other.hasMulticast)) {
        return false;
    }

    if (hasMulticast) {
        if (*multicastAddress != *other.multicastAddress) {
            return false;
        }
    }
    
    return true;
}

} // namespace specpaxos