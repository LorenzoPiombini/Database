#ifndef RECORD_H
#define RECORD_H

#include <sys/types.h>

/*default dynamic array size*/
#define DEF_SIZE 5

/*
 * macro to initialized the dynamic array type
 * element must bu a struct Field
 * */
#define ARRAY_INIT(element)                                             \
	do                                                                  \
	{                                                                   \
		struct array v = {NULL, 0, insert_element, free_dynamic_array}; \
		if (v.insert(element, &v) == -1)                                \
		{                                                               \
			fprintf(stderr, "insert dynamic array failed",              \
					F, L - 3);                                          \
			return -1;                                                  \
		}                                                               \
		while (0)

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
	TYPE_ARRAY_DOUBLE
};

struct array
{
	union
	{
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

struct Field
{
	char *field_name;
	enum ValueType type;

	union
	{
		int i;
		long l;
		float f;
		char *s;
		unsigned char b;
		off_t offset;
		double d;
		struct array v;
	} data;
};

struct Record_f
{
	char *file_name;
	int fields_num;
	struct Field *fields;
};

int init_array(struct array **v, enum ValueType type);
int insert_element(void *element, struct array *v, enum ValueType type);
void free_dynamic_array(struct array *v, enum ValueType type);
struct Record_f *create_record(char *file_name, int fields_num);
unsigned char set_field(struct Record_f *rec, int index, char *field_name, enum ValueType type, char *value);
void free_record(struct Record_f *rec, int fields_num);
void print_record(int count, struct Record_f **recs);
void free_record_array(int len, struct Record_f ***recs);
void free_array_of_arrays(int len, struct Record_f ****array, int *len_ia, int size_ia);
unsigned char copy_rec(struct Record_f *src, struct Record_f **dest);
unsigned char get_index_rec_field(char *field_name, struct Record_f **recs, int recs_len,
								  int *field_i_r, int *rec_index);

#endif /*record.h*/
