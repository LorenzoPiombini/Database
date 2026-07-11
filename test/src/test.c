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
	if(luaL_loadfile(L,"test/lua/lua_test.lua") || lua_pcall(L,0,0,0)){
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		return -1;
	}

	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};

	if(open_files("./test",fds,files,CREATE_FILE) == -1){
		return -1;
	}

	char *func = "w_rec";
	lua_getglobal(L,func);
	lua_pushstring(L,"test");
	lua_pushstring(L,"field:This is a field");

	if(lua_pcall(L,2,2,0) != LUA_OK){
		close_file(3,fds[0],fds[1],fds[2]);
		delete_file(3,files[0],files[1],files[2]);
		clear_lua_stack();
		return -1;
	}
	
	
	int is_num;
	uint32_t k = lua_tonumberx(L, -1, &is_num); 
	if(!is_num){
		close_file(3,fds[0],fds[1],fds[2]);
		delete_file(3,files[0],files[1],files[2]);
		clear_lua_stack();
		return -1;
	}
	
	lua_pop(L,1);

	struct Record_f rec = {0};
	if(port_table_to_record(L, &rec) == -1){
		delete_file(3,files[0],files[1],files[2]);
		clear_lua_stack();
		return -1;
	}

	if(k == 0 && (strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0))){
		clear_lua_stack();
		close_file(3,fds[0],fds[1],fds[2]);
		delete_file(3,files[0],files[1],files[2]);
		return -1;
	}
	
	free_record(&rec,rec.fields_num);
	close_file(3,fds[0],fds[1],fds[2]);
	delete_file(3,files[0],files[1],files[2]);
	return 0;
}

