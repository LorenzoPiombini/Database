#include "endian.h"

uint32_t htonf(float f)
{
    real_number_interpreter_f rnf;
    rnf.f = f;
    return swap32((uint32_t)rnf.i);
}

float ntohf(uint32_t u_i)
{
    real_number_interpreter_f rnf;
    rnf.i = swap32(u_i);
    return rnf.f;
}


unsigned char ntohb(uint16_t ui)
{
    one_byte_interpreter ubi;
    ubi.i = swap16(ui);
    return ubi.c;
}

uint64_t htond(double d)
{
    real_number_interpreter_d rnd;
    rnd.d = d;
    return swap64(rnd.i);
}

double ntohd(uint64_t u_i)
{
    real_number_interpreter_d rnd;
    rnd.i = swap64(u_i);
    return rnd.d;
}

uint64_t swap64(uint64_t n)
{
        if(n == 0) return 0;
        uint64_t b0 = (n & 0x00000000000000ff);
        uint64_t b1 = (n & 0x000000000000ff00);
        uint64_t b2 = (n & 0x0000000000ff0000);
        uint64_t b3 = (n & 0x00000000ff000000);
        uint64_t b4 = (n & 0x000000ff00000000);
        uint64_t b5 = (n & 0x0000ff0000000000);
        uint64_t b6 = (n & 0x00ff000000000000);
        uint64_t b7 = (n & 0xff00000000000000);
 
        uint64_t n_n =  (b0 << 56) |
                        (b1 << 40) |
                        (b2 << 24) |
                        (b3 << 8)  |
                        (b4 >> 8)  |
                        (b5 >> 24) |
                        (b6 >> 40) |
                        (b7 >> 56);
 
        return n_n;
}

uint32_t swap32(uint32_t n)
{

	if(n == 0) return 0;
	uint32_t b0 = (n & 0x000000ff);
	uint32_t b1 = (n & 0x0000ff00);
	uint32_t b2 = (n & 0x00ff0000);
	uint32_t b3 = (n & 0xff000000);

	uint32_t n_n = (b0 << 24) |
		 	(b1 << 8) |
			(b2 >> 8) |
			(b3 >> 24);

	return n_n;
}

uint16_t swap16(uint16_t n)
{

	if(n == 0) return 0;
	uint16_t b0 = (n & 0x00ff);
	uint16_t b1 = (n & 0xff00);

	uint16_t n_n = (b0 << 8) |
		 	(b1 >> 8) ;

	return n_n;
}


