#ifndef _KEY_H
#define _KEY_H

#include <stdint.h>

uint32_t generate_numeric_key(int *fds, int end_point);
char *get_all_keys_for_file(int *fds,int index);

#endif
