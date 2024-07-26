#ifndef HELPER_H
#define HELPER_H


unsigned char create_empty_file(int fd_data, int fd_index,int bucket_ht);
unsigned char append_to_file(int fd_data, int fd_index,char* file_path, char* k, char* data_to_add, char** files);

#endif
