#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <string.h>
#include <stdarg.h>
#include "lock.h"
#include "debug.h"
#include "str_op.h"
#include "parse.h"
#include "common.h"


int lock(int fd, int flag){

	struct stat st;
	if(fstat(fd,&st) != 0){
		fprintf(stderr,"can't aquire lock on file.");
		return -1;
	}
	
	size_t l = number_of_digit(st.st_ino) + strlen(".lock")+1;
	char file_name[l];
	memset(file_name,0,l);
	if(snprintf(file_name,l,"%ld.lock",st.st_ino) < 0){
		fprintf(stderr,"can't aquire lock on file.");
		return -1;
	}


	FILE *fp = fopen(file_name,"r");
	if(fp && (flag == WLOCK || flag == RLOCK)) {
		char line[50] = {0};
		while(fgets(line,50,fp)){
			char *endptr;
			pid_t p_on_file = (pid_t) strtol(line,&endptr,10);
			if(*endptr == '\0' || *endptr == '\n'){
				if(p_on_file == getpid()){
					fclose(fp);
					return 0;
				}
				return -1;
			}
		}	
		fclose(fp);
		return WTLK; 
	} else if(fp && UNLOCK){
		char line[50] = {0};
		while(fgets(line,50,fp));

		char *endptr;
		pid_t p_on_file = (pid_t) strtol(line,&endptr,10);
		if(*endptr == '\0' || *endptr == '\n'){
			if(p_on_file == getpid()){
				fclose(fp);
				unlink(file_name);
				return 0;
			}
			return -1;
		}

		fprintf(stderr,"cannot compare pids\n");
		fclose(fp);
		return -1;
	}else if (!fp && flag == WLOCK){
		fp = fopen(file_name,"w");
		if(!fp){
			fprintf(stderr,"can't aquire lock on file.\n");
			return -1;
		}

		pid_t pid = getpid();
		size_t pid_str_l = number_of_digit(pid)+2;
		char strpid[pid_str_l];
		memset(strpid,0,pid_str_l);

		if(snprintf(strpid,pid_str_l,"%d\n",pid) < 0){
			fprintf(stderr,"can't aquire lock on file.\n");
			return -1;
		}

		fputs(strpid,fp);
		fclose(fp);
		return 0;
	}else if(!fp && flag == RLOCK) return 0;

	return -1;
}

int is_locked(int files, ...)
{
	
	va_list args;
	va_start(args, files);
	
	for(int i = 0; i < files; i++){
		int fd = va_arg(args,int);
		struct stat st;
		if(fstat(fd,&st) != 0)
			continue;
		
		size_t l = number_of_digit(st.st_ino)+ strlen(".lock")+1;
		char file_name[l];
		memset(file_name,0,l);
			
		if(snprintf(file_name,l,"%ld.lock",st.st_ino) < 0){
			fprintf(stderr,"can't verify lock status\n");
			return -1;
		}

		FILE *fp = fopen(file_name,"r");
		if (fp) {
			char line[50] = {0};
			while(fgets(line,50,fp)){
				
				char *endptr;
				pid_t p_on_file = (pid_t) strtol(line,&endptr,10);
				if(*endptr == '\0' || *endptr == '\n'){
					if(p_on_file == getpid()){
						fclose(fp);
						return 0;
					}
					return -1;
				}
			}

			fclose(fp);	
			return LOCKED;
		} 
			
	}
	return 0;
}
