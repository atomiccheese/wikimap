#include "bytes.hpp"

uint16_t swap16(uint16_t x) {
	return (isBigEndian()) ? x : __builtin_bswap16(x);
}

uint32_t swap32(uint32_t x) {
	return (isBigEndian()) ? x : __builtin_bswap32(x);
}

uint64_t swap64(uint64_t x) {
	return (isBigEndian()) ? x : __builtin_bswap64(x);
}

uint16_t readInt16(FILE* f) {
	uint16_t out;
	fread(&out, sizeof(out), 1, f);
	out = swap16(out);
	return out;
}

uint32_t readInt32(FILE* f) {
	uint32_t out;
	fread(&out, sizeof(out), 1, f);
	out = swap32(out);
	return out;
}

uint64_t readInt64(FILE* f) {
	uint64_t out;
	fread(&out, sizeof(out), 1, f);
	out = swap64(out);
	return out;
}

void writeInt16(uint16_t d, FILE* f) {
	d = swap16(d);
	fwrite(&d, sizeof(d), 1, f);
}

void writeInt32(uint32_t d, FILE* f) {
	d = swap32(d);
	fwrite(&d, sizeof(d), 1, f);
}

void writeInt64(uint64_t d, FILE* f) {
	d = swap64(d);
	fwrite(&d, sizeof(d), 1, f);
}

