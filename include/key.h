#ifndef _KEY_H
#define _KEY_H 1

#include "db_types.h"
#include "file.h"
#define MAKE_KEY_JS_STRING 1 /*used in get_ all_keys_for_file*/

#define KEY_GEN_DISK_MODE 	0x0000100
#define KEY_GEN_CACHE_MODE 	0x0000200
#define IS_KEY_CACHE_MODE(n) (((n) & KEY_GEN_CACHE_MODE) != 0 )
#define IS_KEY_DISK_MODE(n) (((n) & KEY_GEN_DISK_MODE) != 0)
enum mode{
	REG,
	BASE,
	INCREM
};

i64 generate_numeric_key(int *fds, int mode,int base,struct Cache *c);
char *get_all_keys_for_file(int *fds,int index,int mode);

#endif
