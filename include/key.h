#ifndef _KEY_H
#define _KEY_H 1

enum mode{
	REG,
	BASE,
	INCREM
};

i64 generate_numeric_key(int *fds, int mode,int base);
char *get_all_keys_for_file(int *fds,int index);

#endif
