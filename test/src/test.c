#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include "test.h"
#include "common.h"
#include "key.h"
#include "file.h"
#include "export_db_lua.h"
#include "crud.h"
#include "str_op.h"
#include "lock.h"
#include "lua_start.h"

int create_file_for_test(char *file_name,char *file_definition, struct Schema *sch, char files[3][MAX_FILE_PATH_LENGTH])
{
	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	struct Schema sch_in = {0};
	/*this creates a file named test, and give a definitions*/
	if(open_files(file_name,fds,files,CREATE_FILE) == -1){
		return -1;
	}

	int field_count = count_fields(file_definition,":");
	if(!create_file_definition_with_no_value(TYPE_DF,field_count,file_definition,&sch_in)) goto clean_on_failure;


	struct Header_d hd = {0, 0, &sch_in};
	if(!create_header(&hd)) goto clean_on_failure;

	if(!write_header(fds[2], &hd)) goto clean_on_failure;

	*sch = sch_in;
	close_file(3,fds[0],fds[1],fds[2]);
	return 0;

clean_on_failure:
	if(sch->fields_name)
		free_schema(sch);

	close_file(3,fds[0],fds[1],fds[2]);
	delete_file(3,files[0],files[1],files[2]);
	return -1;
}
int CRUD_test_check_data()
{
	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};
	struct Record_f rec = {0};
	struct Schema sch = {0};
	char *file_name = "customer";
    char *data_to_add = "name:Test LLC:addr:1B W 8th St:csz:New York NY 10011 :price_level_id:STND";

	/*this creates a file named test, and give a definitions*/
	if(open_files(file_name,fds,files,CREATE_FILE) == -1){
		return -1;
	}

	if(!create_file_definition_with_no_value(TYPE_DF,7,
				"name:t_s:addr:t_s:csz:t_s:phone:t_s:fax:t_s:email:t_s:price_level_id:t_s", 
				&sch)) goto clean_on_failure;


	struct Header_d hd = {0, 0, &sch};
	if (!create_header(&hd)) goto clean_on_failure;

	if (!write_header(fds[2], &hd)) goto clean_on_failure;

	int lock_f = STD_LOCK, check = 0;
	if((check = check_data(file_name,data_to_add,fds,files, &rec,&hd,&lock_f,-1,0)) == -1) goto clean_on_failure;

	if(check != SCHEMA_CT) goto clean_on_failure;

	free_record(&rec,rec.fields_num);
	free_schema(&sch);
	close_file(3,fds[0],fds[1],fds[2]);
	delete_file(3,files[0],files[1],files[2]);
	return 0;
	
clean_on_failure:
	if(rec.fields)
		free_record(&rec,rec.fields_num);
	if(sch.fields_name)
		free_schema(&sch);

	close_file(3,fds[0],fds[1],fds[2]);
	delete_file(3,files[0],files[1],files[2]);
	return -1;
}

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
		delete_file(3,files[0],files[1],files[2]);
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

int LUA_test_save_key_at_index_chache(struct Schema *sch)
{
	lua_pushboolean(L,0);
	lua_setglobal(L,"TEST");

	char *func = "create_rec";

	lua_getglobal(L,func);
	lua_pushstring(L,"test");
	lua_pushstring(L,"field:testKEY");

	if(lua_pcall(L,2,1,0) != LUA_OK) goto clean_on_failure;

	struct Record_f rec = {0};
	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	func = "indexing";
	lua_getglobal(L,func);
	lua_pushstring(L,"test");
	lua_pushstring(L,"keySTRcache");
	lua_pushinteger(L,2);
	lua_pushinteger(L,rec.offset);

	if(lua_pcall(L,4,2,0) != LUA_OK) goto clean_on_failure;


	int is_num;
	uint32_t ix = lua_tonumberx(L, -2, &is_num); 
	if(!is_num) goto clean_on_failure;

	if(ix != 2) goto clean_on_failure;
	char *m = (char*)lua_tostring(L,-1);
	if(!m) goto clean_on_failure;
	if(strncmp(m,"index write succeed cache",strlen(m)) != 0) goto clean_on_failure;


	/*WE NEED TO CLEAN THE CACHE*/
	int i;
	for(i = 0;i < 30; i++){
		if(!dbcache_ptr[i].index_file) continue;

		char* k= "keySTRcache";
		if(get(k,&dbcache_ptr[i].index_file[2],STR) == -1){
			free_cache(&dbcache_ptr[i]);
			goto clean_on_failure;
		}

		Node *r = ht_delete((void*)dbcache_ptr[i].file_name,cache_r_ptr,STR);
		if(!r){
			fprintf(stderr,"!!! SOMENTHIG WRONG WITH THE CACHE!!!%s:%d\n",__FILE__,__LINE__);
			goto clean_on_failure;
		}
		free_ht_node(r);
		free_cache(&dbcache_ptr[i]);
	}

	free_record(&rec,rec.fields_num);
	lua_pushboolean(L,1);
	lua_setglobal(L,"TEST");
	clear_lua_stack();
	return 0;

clean_on_failure:
	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	lua_pushboolean(L,1);
	lua_setglobal(L,"TEST");
	clear_lua_stack();
	return -1;

}
int LUA_test_save_key_at_index(struct Schema *sch)
{
	char *func = "create_rec";

	lua_getglobal(L,func);
	lua_pushstring(L,"test");
	lua_pushstring(L,"field:testKEY");

	if(lua_pcall(L,2,1,0) != LUA_OK) goto clean_on_failure;

	struct Record_f rec = {0};
	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	func = "indexing";
	lua_getglobal(L,func);
	lua_pushstring(L,"test");
	lua_pushstring(L,"keySTR");
	lua_pushinteger(L,2);
	lua_pushinteger(L,rec.offset);

	if(lua_pcall(L,4,2,0) != LUA_OK) goto clean_on_failure;

	int is_num;
	uint32_t ix = lua_tonumberx(L, -2, &is_num); 
	if(!is_num) goto clean_on_failure;

	if(ix != 2) goto clean_on_failure;
	char *m = (char*)lua_tostring(L,-1);
	if(!m) goto clean_on_failure;
	if(strncmp(m,"index write succeed",strlen(m)) != 0) goto clean_on_failure;

	free_record(&rec,rec.fields_num);
	return 0;

clean_on_failure:
	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	clear_lua_stack();
	return -1;
}

int LUA_test_create_record(struct Schema *sch)
{
	char *func = "create_rec";

	lua_getglobal(L,func);
	lua_pushstring(L,"test");
	lua_pushstring(L,"field:This is another field! with a lot of spaces and weird stuf@");

	if(lua_pcall(L,2,1,0) != LUA_OK) goto clean_on_failure;

	struct Record_f rec = {0};
	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	clear_lua_stack();
	
	if(strncmp(rec.fields[0].data.s,"This is another field! with a lot of spaces and weird stuf@",
				strlen("This is another field! with a lot of spaces and weird stuf@")) != 0) goto clean_on_failure;

	free_record(&rec,rec.fields_num);
	return 0;

clean_on_failure:
	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	clear_lua_stack();
	return -1;
}

int LUA_port_table_to_record_test()
{
	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};
	struct Schema sch = {0};

	/*this creates a file named test, and give a definitions*/
	if(open_files("./test_pt",fds,files,CREATE_FILE) == -1){
		return -1;
	}

	if(!create_file_definition_with_no_value(TYPE_DF,3,"name:t_s:last_name:t_s:age:t_b", &sch)) goto clean_on_failure;

	struct Header_d hd = {0, 0, &sch};
	if (!create_header(&hd)) goto clean_on_failure;

	if (!write_header(fds[2], &hd)) goto clean_on_failure;

	close_file(3,fds[0],fds[1],fds[2]);
	memset(fds,-1,sizeof(int)*3);

	/*the lua function w_rec will open and close the file*/
	char *func = "w_rec";

	/*BEHAVIOUR 1*/
	lua_getglobal(L,func);
	lua_pushstring(L,"test_pt"); /*Arg 1*/
	lua_pushstring(L,"name:Lorenzo:last_name:Piombini:age:39"); /*Arg 2*/

	if(lua_pcall(L,2,2,0) != LUA_OK) goto clean_on_failure;

	struct Record_f rec = {0};
	if(tbl_to_rec(L, &rec,&sch) == -1) goto clean_on_failure;

	/*
		w_rec() function return two results the key and the table(record)
		the key is at position -3 from the top of the lua stack
	*/

	int is_num;
	uint32_t k = lua_tonumberx(L, -3, &is_num); 
	if(!is_num) goto clean_on_failure;
	
	clear_lua_stack();

	if(k != 0 && (
				(strncmp(rec.fields[0].data.s,"Lorenzo",strlen("Lorenzo") != 0))
				||(strncmp(rec.fields[1].data.s,"Piombini",strlen("Piombini") != 0))
				|| rec.fields[2].data.b != 39)) goto clean_on_failure;

	free_record(&rec,rec.fields_num);
	free_schema(&sch);
	delete_file(3,files[0],files[1],files[2]);
	return 0;

clean_on_failure:
	if(sch.fields_name != NULL)
		free_schema(&sch);

	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	if(fds[0] != -1)
		close_file(3,fds[0],fds[1],fds[2]);

	delete_file(3,files[0],files[1],files[2]);
	clear_lua_stack();
	return -1;
}

int LUA_test_w_rec_cache(struct Schema *sch)
{
	/*desable the test mode and use the cache system*/
	lua_pushboolean(L,0);
	lua_setglobal(L,"TEST");

	/*using w_rec now, will use the cache */
	char *func = "w_rec";

	/*BEHAVIOUR 1*/
	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	lua_pushstring(L,"field:This is a field"); /*Arg 2*/

	if(lua_pcall(L,2,2,0) != LUA_OK) goto clean_on_failure;

	struct Record_f rec = {0};
	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	/*
		w_rec() function return two results the key and the table(record)
		the key is at position -3 from the top of the lua stack
	*/

	int is_num;
	uint32_t k = lua_tonumberx(L, -3, &is_num); 
	if(!is_num) goto clean_on_failure;
	
	clear_lua_stack();

	if(strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0)) goto clean_on_failure;
	
	free_record(&rec,rec.fields_num);
	rec.fields = NULL;
	memset(&rec,0,sizeof(struct Record_f));
	
	
	/*expand the test here if needed*/

	/*NOTE: here the operation should have succeed, so we need to clean up the cache*/

	
	int i;
	for(i = 0;i < 30; i++){
		if(!dbcache_ptr[i].index_file) continue;

		if(write_cache_to_disk(&dbcache_ptr[i]) == -1) goto clean_on_failure;

		Node *r = ht_delete((void*)dbcache_ptr[i].file_name,cache_r_ptr,STR);
		if(!r){
			fprintf(stderr,"!!! SOMENTHIG WRONG WITH THE CACHE!!!%s:%d\n",__FILE__,__LINE__);
			goto clean_on_failure;
		}
		free_ht_node(r);
		free_cache(&dbcache_ptr[i]);
	}
	
	/*VERIFY THE ACTUAL FILE HAS THE DATA!*/

	char command[100] = {0};
	if(snprintf(command,100,"SHOW test %u",k) == -1) goto clean_on_failure;

	FILE *fp = popen(command,"r");
	if(!fp){
		return -1;
	}

	char buffer[1024*4];
	int found = 0;
	while(fgets(buffer,1024*4,fp) != NULL){
		if(strstr(buffer, "field")) found++;
		if(strstr(buffer,"This is a field")) found++;
	}

	if(found != 2){
		pclose(fp);
		return -1;
	}

	pclose(fp);
	/*enable back the test mode*/
	lua_pushboolean(L,1);
	lua_setglobal(L,"TEST");
	return 0;

clean_on_failure:
	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	/*enable back the test mode*/
	lua_pushboolean(L,1);
	lua_setglobal(L,"TEST");
	clear_lua_stack();
	return -1;

}

/* w_rec has 4 different behaviour
	we test all of them here */
int LUA_test_w_rec(struct Schema *sch)
{
	/*the lua function w_rec will open and close the file*/
	char *func = "w_rec";

	/*BEHAVIOUR 1*/
	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	lua_pushstring(L,"field:This is a field"); /*Arg 2*/

	if(lua_pcall(L,2,2,0) != LUA_OK) goto clean_on_failure;

	struct Record_f rec = {0};
	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	/*
		w_rec() function return two results the key and the table(record)
		the key is at position -3 from the top of the lua stack
	*/

	int is_num;
	uint32_t k = lua_tonumberx(L, -3, &is_num); 
	if(!is_num) goto clean_on_failure;
	
	clear_lua_stack();

	if(k != 0 || (strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0))) goto clean_on_failure;
	
	free_record(&rec,rec.fields_num);
	rec.fields = NULL;
	memset(&rec,0,sizeof(struct Record_f));

	/*BEHAVIOUR 2*/
	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	lua_pushstring(L,"field:This is a field"); /*Arg 2*/
	lua_pushstring(L,"key_1"); /*Arg 2*/
	if(lua_pcall(L,3,2,0) != LUA_OK) goto clean_on_failure;

	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	char *k_s = (char*)lua_tostring(L, -3); 
	if(!k_s) goto clean_on_failure;
	
	if((strncmp(k_s,"key_1",strlen(k_s)) != 0) || (strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0))) 
		goto clean_on_failure;

	clear_lua_stack();

	free_record(&rec,rec.fields_num);
	rec.fields = NULL;
	memset(&rec,0,sizeof(struct Record_f));

	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	lua_pushstring(L,"field:This is a field"); /*Arg 2*/
	lua_pushinteger(L,32); /*Arg 3*/
	if(lua_pcall(L,3,2,0) != LUA_OK) goto clean_on_failure;

	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	is_num = 0;
	k = lua_tonumberx(L, -3, &is_num); 
	if(!is_num) goto clean_on_failure;
	
	clear_lua_stack();
	if(k != 32 || (strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0))) goto clean_on_failure;

	free_record(&rec,rec.fields_num);
	rec.fields = NULL;
	memset(&rec,0,sizeof(struct Record_f));
	
	/*BEHAVIOUR 3*/
	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	lua_pushstring(L,"field:This is a field"); /*Arg 2*/
	lua_pushstring(L,"base"); /*Arg 3*/
	lua_pushinteger(L,100); /*Arg 4*/
	if(lua_pcall(L,4,2,0) != LUA_OK) goto clean_on_failure;

	if(tbl_to_rec(L, &rec,sch) == -1) goto clean_on_failure;

	is_num = 0;
	k = lua_tonumberx(L, -3, &is_num); 
	if(!is_num) goto clean_on_failure;
	
	clear_lua_stack();
	if(k != 103 || (strncmp(rec.fields[0].data.s,"This is a field",strlen("This is a field") != 0))) goto clean_on_failure;

	free_record(&rec,rec.fields_num);
	memset(&rec,0,sizeof(struct Record_f));


	
	/*this has to fail*/
	lua_getglobal(L,func);
	lua_pushstring(L,"test"); /*Arg 1*/
	if(lua_pcall(L,1,2,0) == LUA_OK) goto clean_on_failure;

	clear_lua_stack();

	return 0;

clean_on_failure:
	if(rec.fields != NULL)
		free_record(&rec,rec.fields_num);

	clear_lua_stack();
	return -1;
}

int LUA_test_write_customer_cache()
{
	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};
	struct Record_f rec = {0};
	struct Schema sch = {0};
	char *file_name = "customer";
    char *data_to_add = "name:Test LLC:addr:1B W 8th St:csz:New York NY 10011 :price_level_id:STND";

	/*this creates a file named test, and give a definitions*/
	if(open_files(file_name,fds,files,CREATE_FILE) == -1){
		return -1;
	}

	if(!create_file_definition_with_no_value(TYPE_DF,7,
				"name:t_s:addr:t_s:csz:t_s:phone:t_s:fax:t_s:email:t_s:price_level_id:t_s", 
				&sch)) goto clean_on_failure;


	struct Header_d hd = {0, 0, &sch};
	if (!create_header(&hd)) goto clean_on_failure;

	if (!write_header(fds[2], &hd)) goto clean_on_failure;

	close_file(3,fds[0],fds[1],fds[2]);
	memset(fds,-1,sizeof(int)*3);

	lua_pushstring(L,"./customer");
	lua_setglobal(L,"customers");

	lua_pushboolean(L,0);
	lua_setglobal(L,"TEST");

	char *func = "write_customers";
	lua_getglobal(L,func);
	lua_pushstring(L,"name:Test LLC:addr:1B W 8th St:csz:New York NY 10011 :price_level_id:STND");

	if(lua_pcall(L,1,2,0) != LUA_OK) goto clean_on_failure;

	int is_num;
	uint32_t k = lua_tonumberx(L, -2, &is_num); 
	if(!is_num) goto clean_on_failure;

	if(k != 0) goto clean_on_failure;

	func = "g_all_key";
	lua_getglobal(L,func);
	lua_pushstring(L,"./customer");
	lua_pushinteger(L,2);
	lua_pushinteger(L,MAKE_KEY_JS_STRING);

	if(lua_pcall(L,3,1,0) != LUA_OK) goto clean_on_failure;

	char *result = (char *)lua_tostring(L,-1);
	if(!result) goto clean_on_failure;

	if(!strstr(result,"Test LLC")) goto clean_on_failure;


	/*WE NEED TO CLEAN THE CACHE*/
	int i;
	for(i = 0;i < CACHE_SIZE; i++){
		if(!dbcache_ptr[i].index_file) continue;

		Node *r = ht_delete((void*)dbcache_ptr[i].file_name,cache_r_ptr,STR);
		if(!r){
			fprintf(stderr,"!!! SOMENTHIG WRONG WITH THE CACHE!!!%s:%d\n",__FILE__,__LINE__);
			goto clean_on_failure;
		}
		free_ht_node(r);
		free_cache(&dbcache_ptr[i]);
	}

	free_schema(&sch);

	lua_pushstring(L,"/root/db/customer");
	lua_setglobal(L,"customers");
	lua_pushboolean(L,1);
	lua_setglobal(L,"TEST");
	clear_lua_stack();

	delete_file(3,files[0],files[1],files[2]);
	return 0;

clean_on_failure:
	lua_pushstring(L,"/root/db/customer");
	lua_setglobal(L,"customers");
	lua_pushboolean(L,1);
	lua_setglobal(L,"TEST");
	clear_lua_stack();

	if(sch.types != NULL)
		free_schema(&sch);

	if(fds[0] != -1)
		close_file(3,fds[0],fds[1],fds[2]);

	delete_file(3,files[0],files[1],files[2]);
	return -1;
}
