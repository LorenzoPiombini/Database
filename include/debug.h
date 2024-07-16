#ifndef DEBUG_H
#define DEBUG_H

#include "record.h"
#include "parse.h"

void print_record(int count, Record_f **recs);
void print_schema(Schema sch);
void print_header(Header_d hd);
size_t compute_size_header(Header_d hd);
#endif
