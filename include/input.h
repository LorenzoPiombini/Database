#ifndef INPUT_H
#define INPUT_H

#include "record.h"

#define ALL_OP "all"
void print_usage(char *argv[]);
int check_input_and_values(char *file_path, char *data_to_add, char *key, char *argv[],
                           unsigned char del, unsigned char list_def, unsigned char new_file,
                           unsigned char update, unsigned char del_file, unsigned char build,
                           unsigned char create, unsigned char options,
                           unsigned char index_add);

void print_types(void);
int convert_options(char *options);
typedef enum
{
    ALL
} Option_t;

#endif
