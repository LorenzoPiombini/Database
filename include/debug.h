#ifndef DEBUG_H
#define DEBUG_H

#include "record.h"
#include "parse.h"

#define L __LINE__
#define F __FILE__

void loop_str_arr(char **str, int len);
void print_record(int count, Record_f **recs);
void print_schema(Schema sch);
void print_header(Header_d hd);
size_t compute_size_header(Header_d hd);
void __er_file_pointer(char *file, int line);
#endif
