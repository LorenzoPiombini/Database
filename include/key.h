#ifndef _KEY_H
#define _KEY_H 1

#define MAKE_KEY_JS_STRING 1 /*used in get_ all_keys_for_file*/
enum mode{
	REG,
	BASE,
	INCREM
};

i64 generate_numeric_key(int *fds, int mode,int base);
char *get_all_keys_for_file(int *fds,int index,int mode);

#endif
