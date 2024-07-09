#ifndef INPUT_H
#define INPUT_H

#include "record.h"

void print_usage(char *argv[]);
int check_input_and_values(char* file_path,char* data_to_add, char*fileds_and_type, char* key, char *argv[], int del);


void print_types();

#endif
