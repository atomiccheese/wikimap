#pragma once
#include <stdint.h>

inline bool isBigEndian() {
	uint16_t x = 0xff00;
	if(*((char*)(&x)) == 0) return false;
	return true;
}

uint16_t swap16(uint16_t x) {
	return (isBigEndian()) ? x : __builtin_bswap16(x);
}

uint32_t swap32(uint32_t x) {
	return (isBigEndian()) ? x : __builtin_bswap32(x);
}

uint64_t swap64(uint64_t x) {
	return (isBigEndian()) ? x : __builtin_bswap64(x);
}
