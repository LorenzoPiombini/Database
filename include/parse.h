#ifndef PARSE_H
#define PARSE_H

#include "record.h"

#define HEADER_ID_SYS 0x657A3234
#define VS 1
#define MAX_FIELD_LT 32
#define MAX_HD_SIZE 7232
#define MAX_FIELD_NR 200

typedef struct
{
	unsigned short fields_num;
	char **fields_name;
	ValueType *types;

} Schema;

typedef struct
{
	unsigned int id_n;
	unsigned short version;
	Schema sch_d;
} Header_d;

Record_f *parse_d_flag_input(char *file_path, int fields_num, char *buffer, char *buf_t,
							 char *buf_v, Schema *sch, int check_sch);
void free_schema(Schema *sch);
int create_header(Header_d *hd);
int write_empty_header(int fd, Header_d *hd);
int write_header(int fd, Header_d *hd);
int read_header(int fd, Header_d *hd);
unsigned char ck_input_schema_fields(char **names, ValueType *types_i, Header_d hd);
unsigned char check_schema(int fields_n, char *buffer, char *buf_t, Header_d hd);
int sort_input_like_header_schema(int schema_tp, int fields_num, Schema *sch, char **names, char **values, ValueType *types_i);
unsigned char ck_schema_contain_input(char **names, ValueType *types_i, Header_d hd, int fields_num);
unsigned char add_fields_to_schema(int fields_num, char *buffer, char *buf_tm, Schema *sch);
int create_file_definition_with_no_value(int fields_num, char *buffer, char *buf_t, Schema *sch);
unsigned char perform_checks_on_schema(char *buffer, char *buf_t, char *buf_v, int fields_count, int fd_data, char *file_path, Record_f **rec, Header_d *hd);
unsigned char compare_old_rec_update_rec(Record_f **rec_old, Record_f *rec, Record_f **new_rec, char *file_path,
										 unsigned char check, char *buffer, int fields_num);
void find_fields_to_update(Record_f **recs_old, char *positions, Record_f *rec, int index);
unsigned char create_new_fields_from_schema(Record_f **recs_old, Record_f *rec, Schema *sch,
											int index, Record_f **new_rec, char *file_path);
void print_schema(Schema sch);
void print_header(Header_d hd);
size_t compute_size_header(void *header);
#endif
