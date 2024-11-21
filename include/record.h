#ifndef RECORD_H
#define RECORD_H

#include <sys/types.h>

typedef unsigned char byte;

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
unsigned char set_field(Record_f *rec, int index, char *field_name, ValueType type, char *value);
void free_record(Record_f *rec, int fields_num);
void print_record(int count, Record_f **recs);
void free_record_array(int len, Record_f ***recs);
void free_array_of_arrays(int len, Record_f ****array, int *len_ia, int size_ia);
unsigned char copy_rec(Record_f *src, Record_f **dest);
unsigned char get_index_rec_field(char *field_name, Record_f **recs, int recs_len,
								  int *field_i_r, int *rec_index);

#endif /*record.h*/
