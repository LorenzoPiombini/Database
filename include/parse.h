#ifndef PARSE_H
#define PARSE_H

/* for Record_f, ValueTypes */
#include "record.h"

#define NO_TYPE 0

int parse_d_flag_input(char *file_path, 
			int fields_num, 
			char *buffer, 
			char *buf_t, 
			char *buf_v,
			struct Schema *sch, 
			int check_sch,
			struct Record_f *rec);

unsigned char perform_checks_on_schema(char *buffer, 
					char *buf_t, 
					char *buf_v, 
					int fields_count,
					char *file_path, 
					struct Record_f *rec, 
					struct Header_d *hd);

int parse_input_with_no_type(char *file_path, int fields_num, 
							char names, 
							int *types_i, 
							char **values,
							struct Schema *sch, 
							int check_sch,
							struct Record_f *rec);
int create_header(struct Header_d *hd);
int write_empty_header(int fd, struct Header_d *hd);
int write_header(int fd, struct Header_d *hd);
int read_header(int fd, struct Header_d *hd);
unsigned char ck_input_schema_fields(char names[][MAX_FIELD_LT], int *types_i, struct Header_d hd);
unsigned char check_schema(int fields_n, char *buffer, char *buf_t, struct Header_d hd);
int check_schema_with_no_types(char names[][MAX_FILED_LT], struct Header_d hd);
int sort_input_like_header_schema(int schema_tp, 
					int fields_num, 
					struct Schema *sch, 
					char names[][MAX_FIELD_LT], 
					char **values,
					int *types_i);

unsigned char ck_schema_contain_input(char names[][MAX_FIELD_LT], int *types_i, struct Header_d hd, int fields_num);
unsigned char add_fields_to_schema(int fields_num, char *buffer, char *buf_tm, struct Schema *sch);
int create_file_definition_with_no_value(int fields_num, char *buffer, char *buf_t, struct Schema *sch);
unsigned char compare_old_rec_update_rec(struct Recs_old *rec_old, 
						struct Record_f *rec,
						struct Record_f *new_rec,
						char *file_path,
						unsigned char check, 
						struct Header_d hd);

void find_fields_to_update(struct Recs_old *recs_old, char *positions, struct Record_f *rec);
int create_new_fields_from_schema(struct Recs_old *recs_old, 
						struct Record_f *rec, 
						struct Schema *sch,
						struct Record_f *new_rec, 
						char *file_path);
void print_schema(struct Schema sch);
void print_header(struct Header_d hd);
size_t compute_size_header(void *header);
unsigned char create_data_to_add(struct Schema *sch, char data_to_add[][500]);
char **extract_fields_value_types_from_input(char *buffer, char names[][MAX_FILED_LT], int *types_i, struct Header_d *hd);
#endif
