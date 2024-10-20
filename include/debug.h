#ifndef DEBUG_H
#define DEBUG_H

#define L __LINE__
#define F __FILE__

void loop_str_arr(char **str, int len);
void __er_file_pointer(char *file, int line);
void __er_calloc(char *file, int line);
#endif
