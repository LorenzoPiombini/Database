#ifndef PARSE_H
#define PARSE_H

/* for Record_f, ValueTypes */
#include "record.h"

#define HEADER_ID_SYS 0x657A3234
#define VS 1
#define MAX_HD_SIZE 7232
#define MAX_FIELD_NR 200 /*max field nr in a file */
#define MAX_FIELD_LT 32	 /*max char length for a field name*/

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

struct Record_f *parse_d_flag_input(char *file_path, int fields_num, char *buffer, char *buf_t,
									char *buf_v, struct Schema *sch, int check_sch);
void free_schema(struct Schema *sch);
int create_header(struct Header_d *hd);
int write_empty_header(int fd, struct Header_d *hd);
int write_header(int fd, struct Header_d *hd);
int read_header(int fd, struct Header_d *hd);
unsigned char ck_input_schema_fields(char **names, enum ValueType *types_i, struct Header_d hd);
unsigned char check_schema(int fields_n, char *buffer, char *buf_t, struct Header_d hd);
int sort_input_like_header_schema(int schema_tp, int fields_num, struct Schema *sch, char **names, char **values,
								  enum ValueType *types_i);
unsigned char ck_schema_contain_input(char **names, enum ValueType *types_i, struct Header_d hd, int fields_num);
unsigned char add_fields_to_schema(int fields_num, char *buffer, char *buf_tm, struct Schema *sch);
int create_file_definition_with_no_value(int fields_num, char *buffer, char *buf_t, struct Schema *sch);
unsigned char perform_checks_on_schema(char *buffer, char *buf_t, char *buf_v, int fields_count, int fd_data, char *file_path, struct Record_f **rec, struct Header_d *hd);
unsigned char compare_old_rec_update_rec(struct Record_f **rec_old, struct Record_f *rec,
										 struct Record_f **new_rec, char *file_path,
										 unsigned char check, char *buffer, int fields_num);

void find_fields_to_update(struct Record_f **recs_old, char *positions, struct Record_f *rec, int index);
unsigned char create_new_fields_from_schema(struct Record_f **recs_old, struct Record_f *rec, struct Schema *sch,
											int index, struct Record_f **new_rec, char *file_path);
void print_schema(struct Schema sch);
void print_header(struct Header_d hd);
size_t compute_size_header(void *header);
unsigned char create_data_to_add(struct Schema *sch, char data_to_add[][500]);
#endif
