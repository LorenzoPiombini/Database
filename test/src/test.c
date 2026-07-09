#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "test.h"
#include "file.h"

int create_file_test(){
	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};


	if(open_files("./test",fds,files,CREATE_FILE) == -1){
		return -1;
	}

	FILE *fp = popen("ls -l","r");
	if(!fp){
		close_file(3,fds[0],fds[1],fds[2]);
		return -1;
	}

	char buffer[1024*4];
	int count = 0;
	while(fgets(buffer,1024*4,fp) != NULL){
		if(strstr(buffer, "test.sch") 
			|| strstr(buffer,"test.inx") 
			|| strstr(buffer,"test.dat"))
			count++;
	}

	pclose(fp);
	close_file(3,fds[0],fds[1],fds[2]);
	if(count == 3) {
		if(delete_file_test(files) == -1)
			return -1;

		return 0;
	}

	return -1;
}

int delete_file_test(char files_name[3][MAX_FILE_PATH_LENGTH]){

	delete_file(3,files_name[0],files_name[1],files_name[2]);
	FILE *fp = popen("ls -l","r");
	if(!fp){
		return -1;
	}

	char buffer[1024*4];
	int count = 0;
	while(fgets(buffer,1024*4,fp) != NULL){
		if(strstr(buffer, "test.sch") 
			|| strstr(buffer,"test.inx") 
			|| strstr(buffer,"test.dat"))
			count++;
	}

	pclose(fp);
	if(count == 0) return 0;

	return -1;
}
