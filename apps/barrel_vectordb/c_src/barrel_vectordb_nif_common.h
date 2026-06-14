#ifndef BARREL_VECTORDB_NIF_COMMON_H
#define BARREL_VECTORDB_NIF_COMMON_H

#include <erl_nif.h>
#include <math.h>
#include <stdint.h>
#include <string.h>

/* SIMD detection */
#if defined(__AVX2__)
    #include <immintrin.h>
    #define USE_AVX2 1
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
    #include <arm_neon.h>
    #define USE_NEON 1
#endif

/* Shared helpers */
static inline float dequantize_radius(uint16_t quant) {
    if (quant == 0) return 0.0f;
    float t = (float)quant / 65535.0f * logf(11.0f);
    return expf(t) - 1.0f;
}

static inline void unpack_bits(const uint8_t* packed, int bits, int count, uint32_t* out) {
    uint32_t buffer = 0;
    int buffer_bits = 0, byte_idx = 0;
    uint32_t mask = (1 << bits) - 1;
    for (int i = 0; i < count; i++) {
        while (buffer_bits < bits) {
            buffer = (buffer << 8) | packed[byte_idx++];
            buffer_bits += 8;
        }
        int extra = buffer_bits - bits;
        out[i] = (buffer >> extra) & mask;
        buffer_bits = extra;
        buffer &= (1 << buffer_bits) - 1;
    }
}

#endif
