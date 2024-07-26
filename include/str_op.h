#ifndef STR_OP_H
#define STR_OP_H
#include "record.h"

#define TYPE_ "TYPE_"
#define T_ "t_"

char **two_file_path(char *file_path);
int count_fields(char *fields, const char *target);
int get_type(char *s);
char **get_fileds_name(char *fields_name, int fields_count, int steps);
ValueType *get_value_types(char *fields_input, int fields_count, int steps);
void free_strs(int count, int fields_num, ...);
char **get_values(char *fields_input, int fields_count);
int is_file_name_valid(char *str);
int key_generator(Record_f rec);
#endif
