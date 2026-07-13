#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include "test.h"
#include "file.h"
#include "lock.h"
#include "lua_start.h"

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

int lock_file_test()
{

	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};


	if(open_files("./test",fds,files,CREATE_FILE) == -1){
		return -1;
	}
	

	if(acquire_lock(fds,-1) == -1){
		close_file(3,fds[0],fds[1],fds[2]);
		delete_file(3,files[0],files[1],files[2]);
		return -1;
	}	

	
	int wstatus;
	pid_t child = fork(), w;
	if(child == -1){
		release_lock(fds,-1);
		close_file(3,fds[0],fds[1],fds[2]);
		delete_file(3,files[0],files[1],files[2]);
		return -1;
	}else if(child == 0){
		/*try to acquire a lock on the same file*/
		if(acquire_lock(fds,-1) == -1){
			/*THIS HAS TO FAIL!!*/
			close_file(3,fds[0],fds[1],fds[2]);
			exit(0);
		}
	}

	/* PARENT 
	 * wait for the child
	 * */	
   do {
	   w = waitpid(child, &wstatus, WUNTRACED | WCONTINUED);
	   if (w == -1) {
		   release_lock(fds,-1);
		   close_file(3,fds[0],fds[1],fds[2]);
		   delete_file(3,files[0],files[1],files[2]);
		   perror("waitpid");
		   return -1;
	   }

	   if (WIFEXITED(wstatus)) {
		   if(WEXITSTATUS(wstatus) == 0){
			   release_lock(fds,-1);
			   close_file(3,fds[0],fds[1],fds[2]);
			   delete_file(3,files[0],files[1],files[2]);
			   return 0;
		   }

		   release_lock(fds,-1);
		   close_file(3,fds[0],fds[1],fds[2]);
		   delete_file(3,files[0],files[1],files[2]);
		   return -1;
	   }
   } while (!WIFEXITED(wstatus));
   release_lock(fds,-1);
   close_file(3,fds[0],fds[1],fds[2]);
   delete_file(3,files[0],files[1],files[2]);
   return 0;
}

int LUA_test_w_rec()
{
	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};
	struct Schema sch = {0};

	/*this creates a file named test, and give a definitions*/
	if(open_files("./test",fds,files,CREATE_FILE) == -1){
		return -1;
	}

	if(!create_file_definition_with_no_value(TYPE_DF,1,"field:t_s", &sch)) goto clean_on_failure;

	struct Header_d hd = {0, 0, &sch};
	if (!create_header(&hd)) goto clean_on_failure;

	if (!write_header(fds[2], &hd)) goto clean_on_failure;

	

	close_file(3,fds[0],fds[1],fds[2]);
	memset(fds,-1,sizeof(int)*3);

	/*the lua function w_rec will open and close the file*/
	char *func = "w_rec";
	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	lua_pushstring(L,"field:This is a field"); /*Arg 2*/

	if(lua_pcall(L,2,2,0) != LUA_OK) goto clean_on_failure;
	

	struct Record_f rec = {0};
	if(port_table_to_record(L, &rec,&sch) == -1) goto clean_on_failure;

	free_schema(&sch);
	sch.types = NULL;

	/*
		w_rec() function return two results the key and the table(record)
		the key is at position -3 from the top of the lua stack
	*/

	int is_num;
	uint32_t k = lua_tonumberx(L, -3, &is_num); 
	if(!is_num) goto clean_on_failure;
	
	clear_lua_stack();

	if(k == 0 && (strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0))) goto clean_on_failure;
	
	free_record(&rec,rec.fields_num);
	rec.fields = NULL;

	delete_file(3,files[0],files[1],files[2]);
	return 0;

clean_on_failure:
	if(sch.types != NULL)
		free_schema(&sch);
	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	if(fds[0] != -1)
		close_file(3,fds[0],fds[1],fds[2]);

	delete_file(3,files[0],files[1],files[2]);
	clear_lua_stack();
	return -1;
}

