#include "clock.h"
#include <cpuid.h>
#include <map>
#include <mutex>
#include <cstdio>
#include <x86intrin.h>
#include "lib/message.h"

#define RNotice(fmt, ...) Notice("[%d] " fmt, myIdx, ##__VA_ARGS__)

namespace Clock {

struct StatEntry {
    uint64_t sum = 0;
    uint64_t count = 0;
    uint64_t min = UINT64_MAX;
    uint64_t max = 0;
};

std::map<std::string, StatEntry> cycleStats;

uint64_t rdtsc_clock() {
    unsigned int eax, ebx, ecx, edx;
    uint64_t t = __rdtsc();
    __cpuid(0, eax, ebx, ecx, edx); 
    return t;
}

void logCycleMeasurement(const std::string &opId, uint64_t start, uint64_t end) {
    if (start > end) {
        Notice("\n\n\nINTERLEAVING ON CLOCK CALCULATION\n\n\n");
        return;
    }

    uint64_t diff = end - start;

    auto &entry = cycleStats[opId];
    entry.sum += diff;
    entry.count += 1;
    entry.min = std::min(entry.min, diff);
    entry.max = std::max(entry.max, diff);

    double avg = static_cast<double>(entry.sum) / entry.count;

    // Notice("[%s] Cycle count: %llu | Avg: %.2f | Min: %llu | Max: %llu\n",
        //    opId.c_str(), diff, avg, entry.min, entry.max);
}

} // namespace Clock
