#pragma once

#include <cstdint>
#include <string>

namespace Clock {

uint64_t rdtsc_clock();
uint64_t rdtsc_clock();
void logCycleMeasurement(const std::string &opId, uint64_t start, uint64_t end);

} // clocking namespace
