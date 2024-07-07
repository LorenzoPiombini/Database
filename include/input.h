#ifndef INPUT_H
#define INPUT_H

#include "record.h"

void print_usage(char *argv[]);
int check_input_and_values(char* file_path,char* data_to_add, char*fileds_and_type, char* key, char *argv[], int del);


int get_input_from_user(Record *rec);

char* read_input();

int is_input_a_number(char *input);

#endif
