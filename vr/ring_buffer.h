#ifndef _VR_RING_BUFFER_H_
#define _VR_RING_BUFFER_H_

#include <array>
#include <cstdint>
#include <iostream>
#include "vr/vr-proto.pb.h"

namespace specpaxos {
namespace vr {

template<typename T, size_t N>
class RingBuffer {
    static_assert((N & (N - 1)) == 0, "Buffer size must be a power of 2");
    static_assert(N > 0, "Buffer size must be positive");

private:
    struct Entry {
        T data;
        bool valid;
        uint64_t timestamp;
    };

    std::array<Entry, N> buffer;
    size_t mask = N - 1;
    uint64_t current_timestamp = 0;
    size_t num_entries = 0;
    size_t head = 0;
    size_t tail = 0;

    void printBuffer(const std::string &label) const {
        std::cout << "[" << label << "] Buffer contents:\n";
        for (size_t i = 0; i < N; ++i) {
            if (buffer[i].valid) {
                std::cout << "  [" << i << "] ";
                printEntry(buffer[i].data);
                std::cout << " (ts=" << buffer[i].timestamp << ")\n";
            }
        }
        std::cout << std::endl;
    }

    void printEntry(const T&) const {
        std::cout << "(unprintable entry)";
    }

    size_t wrap(size_t pos) const {
        return pos & mask;
    }

    size_t findLRU() const {
        size_t lru_pos = head;
        uint64_t oldest_ts = buffer[head].timestamp;

        for (size_t i = 0; i < N; ++i) {
            if (buffer[i].valid && buffer[i].timestamp < oldest_ts) {
                oldest_ts = buffer[i].timestamp;
                lru_pos = i;
            }
        }
        return lru_pos;
    }

public:
    RingBuffer() {
        for (auto &entry : buffer) {
            entry.valid = false;
            entry.timestamp = 0;
        }
    }

    void insert(const T &item) {
        // If buffer is full, evict the least recently used (LRU) entry
        if (num_entries == N) {
            size_t lru_pos = findLRU();
            buffer[lru_pos].valid = false;
            num_entries--;
            if (lru_pos == head) {
                head = wrap(head + 1);
            }
        }
    
        // Find the first invalid (free) slot to insert into
        size_t pos = tail;
        for (size_t i = 0; i < N; ++i) {
            if (!buffer[pos].valid) {
                break;
            }
            pos = wrap(pos + 1);
        }
    
        buffer[pos].data = item;
        buffer[pos].valid = true;
        buffer[pos].timestamp = current_timestamp++;
        num_entries++;
    
        // Move tail to the next slot after pos for the next insertion
        tail = wrap(pos + 1);
    
        // printBuffer("insert");
    }
    

    const T* get(uint64_t clientid, uint64_t clientreqid) const {
        for (const auto &entry : buffer) {
            if (entry.valid && entry.data.clientid() == clientid && entry.data.clientreqid() == clientreqid) {
                return &entry.data;
            }
        }
        return nullptr;
    }

    void remove(uint64_t clientid, uint64_t clientreqid) {
        for (auto &entry : buffer) {
            if (entry.valid && entry.data.clientid() == clientid && entry.data.clientreqid() == clientreqid) {
                entry.valid = false;
                num_entries--;
                if (&entry - &buffer[0] == static_cast<ptrdiff_t>(head)) {
                    head = wrap(head + 1);
                }
                break;
            }
        }
    }

    const T* getSlotNum(uint64_t slotnum) const {
        for (const auto &entry : buffer) {
            if (entry.valid && entry.data.opnum() == slotnum) {
                return &entry.data;
            }
        }
        return nullptr;
    }

    size_t size() const {
        return num_entries;
    }
};

template<>
inline void RingBuffer<Request, 256>::printEntry(const Request &req) const {
    std::cout << "Request(cid=" << req.clientid()
              << ", rid=" << req.clientreqid() << ")";
}

template<>
inline void RingBuffer<proto::WitnessDecision, 256>::printEntry(const proto::WitnessDecision &wd) const {
    std::cout << "WitnessDecision(cid=" << wd.clientid()
              << ", rid=" << wd.clientreqid()
              << ", op=" << wd.opnum() << ")";
}

class ClientRequestBuffer : public RingBuffer<Request, 256> {
public:
    using RingBuffer::RingBuffer;
};

class WitnessDecisionBuffer : public RingBuffer<proto::WitnessDecision, 256> {
public:
    using RingBuffer::RingBuffer;
};

} // namespace vr
} // namespace specpaxos

#endif  /* _VR_RING_BUFFER_H_ */
