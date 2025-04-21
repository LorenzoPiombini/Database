#ifndef STR_OP_H
#define STR_OP_H
#include "record.h"

#define TYPE_ "TYPE_"
#define T_ ":t_"

#define MAX_KEY 4294967296

#define MAX_FILE_PATH_LENGTH 1024
#define MAX_FILED_LT 32


#define ASCII_INT_MAX 526            /*the sum of ASCII values for every char in INT_MAX*/
#define ASCII_INT_MIN 572            /*the sum of ASCII values for every char in INT_MIN*/
#define ASCII_LONG_MAX 1000          /*the sum of ASCII values for every char in LONG_MAX*/
#define ASCII_LONG_MIN 1573          /*the sum of ASCII values for every char in LONG_MIN*/
#define ASCII_FLT_MAX 2377           /*the sum of ASCII values for every char in FLT_MAX*/
#define ASCII_FLT_MIN 382            /*the sum of ASCII values for every char in FLT_MIN*/
#define ASCII_FLT_MAX_NEGATIVE 2422  /*the sum of  ASCII values for every char in FLT_MAX butwith the sign -*/
#define ASCII_DBL_MAX 16599          /*the sum of the ASCII values for every char in DBL_MAX*/
#define ASCII_DBL_MAX_NEGATIVE 16644 /*the sum of  ASCII values for every char in DBL_MAX butwith the sign -*/
#define ASCII_DBL_MIN 382            /*the sum of the ASCII values for every char in DBL_MIN*/

/*these value are used as return values to identify which range the software should store
the number in e.i (int or long)*/
#define IN_LONG 100
#define IN_INT 101
#define IN_FLOAT 102
#define IN_DOUBLE 103





int get_array_values(char *src, char ***values);
int is_num(char *key);
void *key_converter(char *key, int *key_type);
int three_file_path(char *file_path, char files[][MAX_FILE_PATH_LENGTH]);
int count_fields(char *fields, const char *user_target);
int get_type(char *s);
int get_fileds_name(char *fields_name, int fields_count, int steps, char names[][MAX_FILED_LT]);
int get_fields_name_with_no_type(char *fields_name, char names[][MAX_FILED_LT]);
unsigned char check_fields_integrity(char names[][MAX_FILED_LT], int fields_count);
int get_value_types(char *fields_input, int fields_count, int steps, int *types);
void free_strs(int fields_num, int count, ...);
char **get_values(char *fields_input, int fields_count);
unsigned char create_blocks_data_to_add(int fields, char dta_src[][500], char dta_blocks[][500]);
int is_file_name_valid(char *str);
void strip(const char c, char *str);
void replace(const char c, const char with, char *str);
char return_first_char(char *str);
char return_last_char(char *str);
size_t digits_with_decimal(float n);
unsigned char is_floaintg_point(char *str);
unsigned char is_integer(char *str);
size_t number_of_digit(int n);
float __round_alt(float n);
unsigned char is_number_in_limits(char *value);
int find_last_char(const char c, char *src);
int assign_type(char *value);

#endif /* STR_OP_H */
