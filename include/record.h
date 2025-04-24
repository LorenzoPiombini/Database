#ifndef RECORD_H
#define RECORD_H

#include <sys/types.h>
#include <stdint.h>
#define MAX_FILE_NAME_LEN 1024

/*default dynamic array size*/
#define DEF_SIZE 1

#define HEADER_ID_SYS 0x657A3234
#define VS 1
#define MAX_HD_SIZE 7232
#define MAX_FIELD_NR 200 /*max field nr in a file */
#define MAX_FIELD_LT 32	 /*max char length for a field name*/
#define MAX_RECS_OLD_CAP 100/*maximum capacity for recs old array */
struct Schema {
	unsigned short fields_num;
	char fields_name[MAX_FIELD_NR][MAX_FIELD_LT];
	int types[MAX_FIELD_NR];
};

struct Header_d
{
	unsigned int id_n;
	unsigned short version;
	struct Schema sch_d;
};



enum ValueType
{
	TYPE_INT,
	TYPE_LONG,
	TYPE_FLOAT,
	TYPE_STRING,
	TYPE_BYTE,
	TYPE_OFF_T,
	TYPE_DOUBLE,
	TYPE_ARRAY_INT,
	TYPE_ARRAY_LONG,
	TYPE_ARRAY_FLOAT,
	TYPE_ARRAY_STRING,
	TYPE_ARRAY_BYTE,
	TYPE_ARRAY_DOUBLE,
	TYPE_FILE
};

struct array
{
	union {
		int **i;
		long **l;
		float **f;
		char **s;
		unsigned char **b;
		double **d;
	} elements;
	int size;
	int (*insert)(void *, struct array *, enum ValueType);
	void (*destroy)(struct array *, enum ValueType);
};

struct Record_f;	

struct File_field{
	off_t offset;
	union{
		uint32_t n;
		char *s;
	}key;
	struct Record_f *rec;
};

struct Field {
	char field_name[MAX_FIELD_LT];
	enum ValueType type;

	union {
		int i;
		long l;
		float f;
		char *s;
		unsigned char b;
		double d;
		struct array v;
		struct File_field *file;
	}data;
};

struct Record_f {
	char file_name[MAX_FILE_NAME_LEN];
	int fields_num;
	uint8_t field_set[MAX_FIELD_NR];
	struct Field fields[MAX_FIELD_NR];
};

/*
 * used to update records
 * this will optimize the process,
 * the effort here is to avoid unneccesary memory allocations
 * so here the memory will be allocated on the heap only if the Record
 * is in more than 500 different locations in the file.
 * */
struct Recs_old	{
	struct Record_f recs[MAX_RECS_OLD_CAP];
	off_t pos_u[MAX_RECS_OLD_CAP];
	int capacity;
	struct Record_f *recs_r;
	off_t *pos_u_r;
	int dynamic_capacity;
};

int insert_rec(struct Recs_old *buffer, struct Record_f *rec, off_t pos);
void free_recs_old(struct Recs_old *buffer);
int init_array(struct array **v, enum ValueType type);
int insert_element(void *element, struct array *v, enum ValueType type);
void free_dynamic_array(struct array *v, enum ValueType type);
void create_record(char *file_name, struct Schema sch, struct Record_f *rec);
unsigned char set_field(struct Record_f *rec, int index, char *field_name, enum ValueType type, char *value,uint8_t field_bit);
void free_record(struct Record_f *rec, int fields_num);
void print_record(int count, struct Record_f *recs);
void free_record_array(int len, struct Record_f **recs);
void free_records(int len,struct Record_f *recs);
void free_array_of_arrays(int len, struct Record_f ****array, int *len_ia, int size_ia);
unsigned char copy_rec(struct Record_f *src, struct Record_f *dest, struct Schema sch);
unsigned char get_index_rec_field(char *field_name, struct Record_f **recs, int recs_len,int *field_i_r, int *rec_index);

#endif /*record.h*/
