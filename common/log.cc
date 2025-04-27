// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * log.h:
 *   a replica's log of pending and committed operations
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

#include "common/log.h"
#include "common/request.pb.h"
#include "lib/assert.h"

#include <openssl/sha.h>

namespace specpaxos {

const string Log::EMPTY_HASH = string(SHA_DIGEST_LENGTH, '\0');

Log::Log(bool useHash, opnum_t start, string initialHash)
    : useHash(useHash)
{
    this->initialHash = initialHash;
    this->start = start;
    // Notice("start is %d", start);
    if (start == 1) {
        ASSERT(initialHash == EMPTY_HASH);
    }
}


LogEntry &
Log::Append(viewstamp_t vs, const Request &req, LogEntryState state)
{
    // Notice("Appended request with id %d to log", req.clientreqid());
    if (entries.empty()) {
        ASSERT(vs.opnum == start);
    } else {
        // Notice("opnum %d and lastopnum+1 is %d", vs.opnum, LastOpnum()+1);
        ASSERT(vs.opnum == LastOpnum()+1);
    }

    string prevHash = LastHash();
    entries.push_back(LogEntry(vs, state, req));
    if (useHash) {
        entries.back().hash = ComputeHash(prevHash, entries.back());        
    }
    
    return entries.back();
}


LogEntry & Log::PriorityPut(viewstamp_t vs, const Request &req, LogEntryState state)
{
    if (entries.empty()) {
        Warning("vs.opnum: %d; start:%d", vs.opnum, start);
        ASSERT(vs.opnum == start);
    } 
    
    if (vs.opnum == LastOpnum()+1) {
        return Append(vs, req, state);
    } else if (vs.opnum > LastOpnum()+1) {
        Panic("priority put - index %d is not valid - next should've been %d", vs.opnum, LastOpnum()+1);
    }

    LogEntry entry = LogEntry(vs, state, req);
    size_t index = vs.opnum - start;

    if (index < entries.size()) {
        // replace existing element
        entries[index] = entry;
    } else {
        // shoudlnt hit this case?
        Panic("inserted in incorrect spot of index %d, opnum %d, entries size of %d, and lastopnum of %d", index, vs.opnum, entries.size(), LastOpnum());
    }

    return entry;
    //dont really think we need to return anything
}



// This really ought to be const
LogEntry *
Log::Find(opnum_t opnum)
{
    if (entries.empty()) {
        return NULL;
    }

    if (opnum < start) {
        return NULL;
    }

    if (opnum-start > entries.size()-1) {
        return NULL;
    }

    LogEntry *entry = &entries[opnum-start];

    ASSERT(entry->viewstamp.opnum == opnum);
    return entry;
}


bool
Log::SetStatus(opnum_t op, LogEntryState state)
{
    LogEntry *entry = Find(op);
    //made edits so that setstatus also returns false if the state changed
    if (entry == NULL) {
        return false;
    } else if ((entry->state != state)){
        entry->state = state;
        return false;
    }

    entry->state = state;
    return true;
}

bool
Log::SetRequest(opnum_t op, const Request &req)
{
    if (useHash) {
        Panic("Log::SetRequest on hashed log not supported.");
    }
    
    LogEntry *entry = Find(op);
    if (entry == NULL) {
        return false;
    }

    entry->request = req;
    return true;
}

void
Log::RemoveAfter(opnum_t op)
{
#if PARANOID
    // We'd better not be removing any committed entries.
    for (opnum_t i = op; i <= LastOpnum(); i++) {
        ASSERT(Find(i)->state != LOG_STATE_COMMITTED);
    }
#endif

    if (op > LastOpnum()) {
        return;
    }

    Debug("Removing log entries after " FMT_OPNUM, op);

    ASSERT(op-start < entries.size());
    entries.resize(op-start);

    ASSERT(LastOpnum() == op-1);
}


void
Log::RemoveUpTo(opnum_t op)
{
    if (useHash) {
        Panic("Log::RemoveUpTo on hashed log not supported.");
    }

	ASSERT(op >= start);

    // Notice("Removing log entries up to " FMT_OPNUM "; start is " FMT_OPNUM, op, start);
	ASSERT(entries.begin()->viewstamp.opnum == start);
	while (start <= op) {
		// Shouldn't erase uncommitted entries---everyone should have committed
		// these already!
		ASSERT(entries.begin()->state == LOG_STATE_COMMITTED);
		entries.erase(entries.begin());	
		start++;
		ASSERT(entries.empty() || entries.begin()->viewstamp.opnum == start);
	}

	ASSERT(entries.empty() || entries.begin()->viewstamp.opnum == op + 1);
	// Notice("New log size: " FMT_OPNUM, entries.size());
}


/**
 * @brief Checks whether a request is contained and returns the opnum, if applicable (only compares request, not entire entry)
 * 
 * @param req - client request
 * @returns -1 if the req is not in the log and the opnum of the request otherwise
 */
int Log::Contains(const Request &req){
    for (auto &entry : entries) {
        if (req.clientid() == entry.request.clientid() &&
                    req.clientreqid() == entry.request.clientreqid() &&
                    req.op() == entry.request.op()) {
            return entry.viewstamp.opnum;
        }
    }
    // Warning("no match for request with clientid: %d; clientreqid: %d; op: %d", req.clientid(), req.clientreqid(), req.op());
    return -1;
}



LogEntry *
Log::Last()
{
    if (entries.empty()) {
        return NULL;
    }
    
    return &entries.back();
}

viewstamp_t
Log::LastViewstamp() const
{
    if (entries.empty()) {
        return viewstamp_t(0, start-1);
    } else {
        return entries.back().viewstamp;
    }
}

opnum_t
Log::LastOpnum() const
{
    if (entries.empty()) {
        return start-1;
    } else {
        return entries.back().viewstamp.opnum;
    }
}

opnum_t
Log::FirstOpnum() const
{
    // XXX Not really sure what's appropriate to return here if the
    // log is empty
    return start;
}

bool
Log::Empty() const
{
    return entries.empty();
}

const string &
Log::LastHash() const
{
    if (entries.empty()) {
        return initialHash;
    } else {
        return entries.back().hash;
    }
}

string
Log::ComputeHash(string lastHash, const LogEntry &entry)
{
    SHA_CTX ctx;
    unsigned char out[SHA_DIGEST_LENGTH];

    SHA1_Init(&ctx);
    
    SHA1_Update(&ctx, lastHash.c_str(), lastHash.size());
    //SHA1_Update(&ctx, &entry.viewstamp, sizeof(entry.viewstamp));
    uint64_t x[2];
    x[0] = entry.request.clientid();
    x[1] = entry.request.clientreqid();
    SHA1_Update(&ctx, x, sizeof(uint64_t)*2);
    // SHA1_Update(&ctx, entry.request.op().c_str(),
    //             entry.request.op().size());

    SHA1_Final(out, &ctx);

    return string((char *)out, SHA_DIGEST_LENGTH);
}

size_t Log::Size(){
    return entries.size();
}

} // namespace specpaxos
