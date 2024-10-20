#ifndef RECORD_H
#define RECORD_H

#include <sys/types.h>

typedef unsigned char byte;
typedef struct HashTable HashTable;

typedef struct Record Record;

typedef enum
{
	TYPE_INT,
	TYPE_LONG,
	TYPE_FLOAT,
	TYPE_STRING,
	TYPE_BYTE,
	TYPE_OFF_T,
	TYPE_DOUBLE
} ValueType;

typedef struct
{
	char *field_name;
	ValueType type;

	union
	{
		int i;
		long l;
		float f;
		char *s;
		unsigned char b;
		off_t offset;
		double d;
	} data;
} Field;

typedef struct
{
	char *file_name;
	int fields_num;
	Field *fields;
} Record_f;

Record_f *create_record(char *file_name, int fields_num);
void set_field(Record_f *rec, int index, char *field_name, ValueType type, char *value);
void clean_up(Record_f *rec, int fields_num);
void print_record(int count, Record_f **recs);

#endif
