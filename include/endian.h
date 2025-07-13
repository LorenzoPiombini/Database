#ifndef ENDIAN_H
#define ENDIAN_H

#include <stdint.h>
/*assign a value to the double to get the corrisponding value int*/
typedef union
{
    uint32_t i;
    float f;
} real_number_interpreter_f;

/*assign a value to the double to get the corrisponding value int*/
typedef union
{
    uint64_t i;
    double d;
} real_number_interpreter_d;

typedef union
{
    uint16_t i;
    unsigned char c;
} one_byte_interpreter;

uint16_t htonb(unsigned char c);
unsigned char ntohb(uint16_t ui);
uint32_t htonf(float f);
float ntohf(uint32_t u_i);
uint64_t htond(double d);
double ntohd(uint64_t u_i);
uint64_t swap64(uint64_t n);
uint32_t swap32(uint32_t n);
uint16_t swap16(uint16_t n);

#endif
