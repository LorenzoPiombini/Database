#ifndef INPUT_H
#define INPUT_H

#include "record.h"
#include "str_op.h"

#define ALL_OP "all" /*use to delete the all file content*/
#define ADD_AR_OP "aar" /* specificly append to the field array*/

void print_usage(char *argv[]);
int check_input_and_values(struct String file_path, struct String data_to_add, struct String key, char *argv[],
                           unsigned char del, unsigned char list_def, unsigned char new_file,
                           unsigned char update, unsigned char del_file, unsigned char build,
                           unsigned char create, unsigned char options, unsigned char index_add,
			   unsigned char file_field, unsigned char import_from_data);

void print_types(void);
int convert_options(char *options);
typedef enum
{
    ALL,
    AAR /*append to array*/
} Option_t;

#endif
