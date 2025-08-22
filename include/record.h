#ifndef RECORD_H
#define RECORD_H

#include "types.h"
#include <sys/types.h>
#define MAX_FILE_NAME_LEN 1024

/*default dynamic array size*/
#define DEF_SIZE 1

#define HEADER_ID_SYS 0x657A3234
#define VS 1
#define MAX_HD_SIZE 7232
#define MAX_FIELD_NR 200 /*max field nr in a file */
#define MAX_FIELD_LT 32	 /*max char length for a field name*/

struct Schema {
	uint16_t fields_num;
	char **fields_name;
	int *types;
};/*20 b*/

struct Header_d
{
	uint32_t id_n;
	uint16_t version;
	struct Schema *sch_d;
};/*16 b*/



enum ValueType
{
	TYPE_INT,
	TYPE_LONG,
	TYPE_FLOAT,
	TYPE_STRING,
	TYPE_BYTE,
	TYPE_PACK,
	TYPE_DOUBLE,
	TYPE_ARRAY_INT,
	TYPE_ARRAY_LONG,
	TYPE_ARRAY_FLOAT,
	TYPE_ARRAY_STRING,
	TYPE_ARRAY_BYTE,
	TYPE_ARRAY_DOUBLE,
	TYPE_FILE
};

struct Record_f;	
struct array
{
	union {
		int *i;
		long *l;
		float *f;
		char **s;
		unsigned char *b;
		double *d;
	} elements;
	int size;
	int (*insert)(void *, struct array *, enum ValueType);
	void (*destroy)(struct array *, enum ValueType);
};/*28 b*/


struct File {
	struct Record_f *recs;
	uint32_t count;	
};/*?? bytes*/
	
struct Field {
	char field_name[MAX_FIELD_LT];
	int type;

	union {
		int i;
		long l;
		float f;
		char *s;
		uint32_t p;
		unsigned char b;
		double d;
		struct array v;
		struct File file;
	}data;
};/*64 b*/

struct Record_f {
	char file_name[MAX_FILE_NAME_LEN];
	off_t offset;
	int fields_num;
	uint8_t *field_set;
	struct Field *fields;
	uint32_t count;
	struct Record_f *next;
};


int init_array(struct array **v, enum ValueType type);
int insert_element(void *element, struct array *v, enum ValueType type);
void free_dynamic_array(struct array *v, enum ValueType type);
int create_record(char *file_name, struct Schema sch, struct Record_f *rec);
unsigned char set_field(struct Record_f *rec, int index, char *field_name, enum ValueType type, char *value,uint8_t field_bit);
void free_record(struct Record_f *rec, int fields_num);
void print_record(int count, struct Record_f recs);
void free_record_array(int len, struct Record_f **recs);
void free_array_of_arrays(int len, struct Record_f ****array, int *len_ia, int size_ia);
unsigned char copy_rec(struct Record_f *src, struct Record_f *dest, struct Schema *sch);
unsigned char get_index_rec_field(char *field_name, struct Record_f **recs, int recs_len,int *field_i_r, int *rec_index);
int schema_has_type(struct Header_d *hd);
int compare_rec(struct Record_f *src, struct Record_f *dest);
int set_schema(char names[][MAX_FIELD_LT], int *types_i, struct Schema *sch, int fields_c);
int free_schema(struct Schema *sch);
void free_type_file(struct Record_f *rec,int optimized);
int parse_record_to_json(struct Record_f *rec,char **buffer);

#endif /*record.h*/
