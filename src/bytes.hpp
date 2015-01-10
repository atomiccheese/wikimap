#pragma once
#include <stdio.h>
#include <stdint.h>

inline bool isBigEndian() {
	uint16_t x = 0xff00;
	if(*((char*)(&x)) == 0) return false;
	return true;
}

uint16_t swap16(uint16_t x);
uint32_t swap32(uint32_t x);
uint64_t swap64(uint64_t x);
uint16_t readInt16(FILE* f);
uint32_t readInt32(FILE* f);
uint64_t readInt64(FILE* f);
void writeInt16(uint16_t d, FILE* f);
void writeInt32(uint32_t d, FILE* f);
void writeInt64(uint64_t d, FILE* f);
