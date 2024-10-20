#include <stdio.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <byteswap.h>
#include "float_endian.h"

uint32_t htonf(float f)
{
    real_number_interpreter_f rnf;
    rnf.f = f;
    return htonl((uint32_t)rnf.i);
}
float ntohf(uint32_t u_i)
{
    real_number_interpreter_f rnf;
    rnf.i = ntohl(u_i);
    return rnf.f;
}

uint16_t htonb(unsigned char c)
{
    one_byte_interpreter ubi;
    ubi.c = c;
    return htons(ubi.i);
}
unsigned char ntohb(uint16_t ui)
{
    one_byte_interpreter ubi;
    ubi.i = ntohs(ui);
    return ubi.c;
}

uint64_t htond(double d)
{
    real_number_interpreter_d rnd;
    rnd.d = d;
    return bswap_64(rnd.i);
}

double ntohd(uint64_t u_i)
{
    real_number_interpreter_d rnd;
    rnd.i = bswap_64(u_i);
    return rnd.d;
}
