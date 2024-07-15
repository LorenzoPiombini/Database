#ifndef INPUT_H
#define INPUT_H

#include "record.h"

void print_usage(char *argv[]);
int check_input_and_values(char *file_path, char *data_to_add, char *fileds_and_type, char *key, char *argv[],
                           unsigned char del, unsigned char list_def, unsigned char new_file,
                           unsigned char update);

void print_types();

#endif
