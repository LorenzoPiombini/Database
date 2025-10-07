#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <errno.h>
#include "lock.h"
#include "debug.h"
#include "str_op.h"
#include "parse.h"
#include "common.h"
#include "log.h"

static FILE *log = NULL;
#define _LOG_ !log ? stderr : log
static int log_open = 0;

int lock(int fd, int flag){

	if(!log_open){
		log = open_log_file("log_lock");
		if(log) log_open = 1;
	}

	struct stat st;
	errno = 0;
	if(fstat(fd,&st) == -1){
		fprintf(_LOG_,"can't aquire lock on file %s:%d.\n",__FILE__,__LINE__-1);
		fprintf(_LOG_,"%s\n",strerror(errno));
		return -1;
	}
	
	size_t l = number_of_digit(st.st_ino) + strlen(".lock")+1;
	char file_name[l];
	memset(file_name,0,l);
	if(copy_to_string(file_name,l,"%ld.lock",st.st_ino) == -1){
		fprintf(_LOG_,"can't aquire lock on file %s:%d.\n",__FILE__,__LINE__-1);
		return -1;
	}


	FILE *fp = fopen(file_name,"r");
	if(fp && (flag == WLOCK || flag == RLOCK)) {
		char line[50];
		memset(line,0,50);
		while(fgets(line,50,fp)){
			errno = 0;
			pid_t p_on_file = (pid_t) string_to_long(line);
			if(errno == EINVAL){ 
				fprintf(_LOG_,"string_to_long failed, %s:%d.\n",__FILE__,__LINE__-2);
				fclose(fp);
				return -1;
			}

			fprintf(_LOG_,"process number %d, try to lock file locked by %d\n",getpid(),p_on_file);
			if(p_on_file == getpid()){
				fclose(fp);
				fprintf(_LOG_,"process number %d already has lock on the file\n",getpid());
				return 0;
			}
			fprintf(_LOG_,"process number %d did not lock the file because locked by %d\n",getpid(),p_on_file);
		}	
		fclose(fp);
		return WTLK; 
	} else if(fp && UNLOCK){
		char line[50];
		memset(line,0,50);
		while(fgets(line,50,fp));

		errno = 0;
		pid_t p_on_file = (pid_t) string_to_long(line);
		if(errno == EINVAL){ 
			fclose(fp);
			return -1;
		}

		if(p_on_file == getpid()){
			fclose(fp);
			unlink(file_name);
			return 0;
		}

		fprintf(_LOG_,"cannot compare pids\n");
		fclose(fp);
		return -1;

	}else if (!fp && flag == WLOCK){
		fp = fopen(file_name,"w");
		if(!fp){
			fprintf(_LOG_,"can't aquire lock on file %s:%d.\n",__FILE__,__LINE__-1);
			return -1;
		}

		pid_t pid = getpid();
		size_t pid_str_l = number_of_digit(pid)+2;
		char strpid[pid_str_l];
		memset(strpid,0,pid_str_l);

		if(copy_to_string(strpid,pid_str_l,"%d\n",pid) < 0){
			fprintf(_LOG_,"can't aquire lock on file %s:%d.\n",__FILE__,__LINE__-1);
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
	
	int i;
	for(i = 0; i < files; i++){
		int fd = va_arg(args,int);
		struct stat st;
		if(fstat(fd,&st) == -1) continue;
		
		size_t l = number_of_digit(st.st_ino)+ strlen(".lock")+1;
		char file_name[l];
		memset(file_name,0,l);
			
		if(copy_to_string(file_name,l,"%ld.lock",st.st_ino) == -1){
			fprintf(_LOG_,"can't verify lock status\n");
			return -1;
		}

		FILE *fp = fopen(file_name,"r");
		if (fp) {
			char line[50];
			memset(line,0,50);
			while(fgets(line,50,fp)){
				
				errno = 0;
				pid_t p_on_file = (pid_t) string_to_long(line);
				if(errno == EINVAL){
					fprintf(_LOG_,"string_to_long failed %s:%d.\n",__FILE__,__LINE__-2);
					return -1;
				}

				if(p_on_file == getpid()){
					fclose(fp);
					return 0;
				}
			}

			fclose(fp);	
			return LOCKED;
		} 
			
	}
	return 0;
}
