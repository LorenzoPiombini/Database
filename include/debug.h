#ifndef DEBUG_H
#define DEBUG_H

#include "record.h"
#include "parse.h"

void print_record(Record_f *rec);
void print_schema(Schema sch);
void print_header(Header_d hd);
void print_size_header(Header_d hd);

#endif
