#ifndef BUILD_H
#define BUILD_H

#include <stdio.h>

#define SFT_B 5 /* extend of 5 segments (char*) the memory to store the file content*/
unsigned char build_from_txt_file(char *file_path, char *txt_f);
int get_number_value_from_txt_file(FILE *fp);

#endif
