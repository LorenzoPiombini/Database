#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include "build.h"
#include "helper.h"
#include "file.h"
#include "str_op.h"

unsigned char build_from_txt_file(char* file_path, char* txt_f){
	int fd_index = -1, fd_data = -1;
	/*creates two name from the file_path  "str_op.h" */
	char** files = two_file_path(file_path);

	fd_index = create_file(files[0]);
	fd_data  = create_file(files[1]);

	FILE *fp = fopen(txt_f,"r"); 
	if(!fp){
		printf("file open failed, build.c l %d.\n", __LINE__ -2);
		return 0;
	}
	
	/*get record number and buffer for the longest line*/
	int line_buf = get_number_value_from_txt_file(fp);	
	int recs = get_number_value_from_txt_file(fp);
	if(line_buf == 0 || recs == 0 )
	{
		printf("no number in file txt, build.c:%d.\n", __LINE__ - 4);
		return 0;
	}
		
	/* create target file */	
	if(!create_empty_file(fd_data,fd_index, recs)){
		printf("create empty file failed, build.c:%d.\n", __LINE__ - 1);
                close_file(2,fd_index,fd_data);
		delete_file(2, files[0], files[1]);
                free(files[0]), free(files[1]), free(files);
		fclose(fp);
		return 0;
	}

	/*read the file and create the file in the sys*/
	char buf[line_buf];
	char* sv_b = NULL;
	printf("copying file system...\nthis might take a few minutes\n"); 
	while(fgets(buf,sizeof(buf),fp)){
		buf[strcspn(buf,"\n")] = '\0';
		char* str = strtok_r(buf,"|",&sv_b);
		char* k =strtok_r(NULL,"|",&sv_b);
		

//		printf("%s\n%s\n", str, k);
//		getchar();
		if(append_to_file(fd_data, fd_index, file_path, k, str,files) == 0){
                	close_file(2,fd_index,fd_data);
                	free(files[0]), free(files[1]), free(files);
			fclose(fp);
			return 0;
		}
	}
	
	fclose(fp);
       	free(files[0]), free(files[1]), free(files);
	close_file(2, fd_index,fd_data);		
	return 1;
}


int get_number_value_from_txt_file(FILE* fp){	

	char buffer[250];
	unsigned short i = 0;
	unsigned char is_num = 1;
	if(fgets(buffer,sizeof(buffer),fp)){
		buffer[strcspn(buffer, "\n")] = '\0';

		for(i = 0; i < buffer[i] ==  '\0'; i++)
			if(!isdigit(buffer[i])){
				is_num = 0;
				break;
			}
			
	} 	
	

	if(!is_num){
		printf("first line is not a number, build.c:%d", __LINE__ - 1);
		return 0;
	}
	
	return atoi(buffer);	
}
