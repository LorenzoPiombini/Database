#include <stdio.h>
#include <string.h>
#include "test.h"

extern int luaopen_db(lua_State *L);

int LUA_test_init(char *file_config){
	L = luaL_newstate();
	luaL_openlibs(L);

	// Force Lua to use the C-linked version of the module!
	// When the Lua script calls `require "db"`, it will use this instead of the .so file.
	luaL_requiref(L, "db", luaopen_db, 1);
	lua_pop(L, 1);
	if(luaL_loadfile(L,file_config) || lua_pcall(L,0,0,0)){
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		return -1;
	}
	return 0;
}

#define GREEN_TEXT "\033[32m"
#define RED_TEXT "\033[31m"
#define REGULAR_TEXT "\033[0m"

static char prog[] = "test_suite";
int main()
{
	char failed_test[200][256] = {0};
	int count = 0, passed = 0, failed = 0;
	fprintf(stdout,"======== Running test for database ===========\n");
	count++;
	if(create_file_test() == -1){
		strncpy(failed_test[failed],"create_file_test()",strlen("create_file_test()"));
		failed++;
		fprintf(stderr,"(%s): create_file_test() failed\n",prog);
	} else{
		passed++;
	}
	
	
	count++;
	if(lock_file_test() == -1){
		strncpy(failed_test[failed],"lock_file_test()",strlen("lock_file_test()"));
		failed++;
	}else{
		passed++;
	}

	count++;
	if(CRUD_test_check_data() == -1){
		strncpy(failed_test[failed],"CRUD_test_check_data()",strlen("CRUD_test_check_data()"));
		failed++;
	}else{
		passed++;
	}

	LUA_test_init("test/lua/old_test.lua");

	/*create a file once for the tests!*/
	struct Schema sch = {0};
	char files[3][256] = {0};
	if(create_file_for_test("./test","field:t_s", &sch,files) == -1){
		fprintf(stderr,"(%s): cannot create file for testing...aborting\n",prog);
		return -1;
	}

	count++;
	if(LUA_port_table_to_record_test() == -1){
		strncpy(failed_test[failed],"LUA_port_table_to_record_test()",strlen("LUA_port_table_to_record_test()"));
		failed++;
	}else{
		passed++;
	}

	count++;
	if(LUA_test_w_rec(&sch) == -1){
		strncpy(failed_test[failed],"LUA_test_w_rec()",strlen("LUA_test_w_rec()"));
		failed++;
	}else{
		passed++;
	}
		
	count++;
	if(LUA_test_w_rec_cache(&sch) == -1){
		strncpy(failed_test[failed],"LUA_test_w_rec_cache()",strlen("LUA_test_w_rec_cache()"));
		failed++;
	}else{
		passed++;
	}

	count++;
	if(LUA_test_create_record(&sch) == -1){
		strncpy(failed_test[failed],"LUA_test_create_record()",strlen("LUA_test_create_record()"));
		failed++;
	}else{
		passed++;
	}

	count++;
	if(LUA_test_save_key_at_index(&sch) == -1){
		strncpy(failed_test[failed],"LUA_test_save_key_at_index()",strlen("LUA_test_save_key_at_index()"));
		failed++;
	}else{
		passed++;
	}

	count++;
	if(LUA_test_save_key_at_index_chache(&sch) == -1){
		strncpy(failed_test[failed],"LUA_test_save_key_at_index_chache()",strlen("LUA_test_save_key_at_index_chache()"));
		failed++;
	}else{
		passed++;
	}

	count++;
	if(LUA_test_write_customer_cache() == -1){
		strncpy(failed_test[failed],"LUA_test_write_customer_cache()",strlen("LUA_test_write_customer_cache()"));
		failed++;
	}else{
		passed++;
	}
	/*PASTE NEW TEST FUNCTIONS BETWEEN THESE LINES*/


	/*===========================================*/

	count++;
	if(delete_file_test(files) == -1){
		strncpy(failed_test[failed],"delete_file_test()",strlen("delete_file_test()"));
		failed++;
	}else{
		passed++;
	}
	free_schema(&sch);
	fprintf(stdout,"======== END test for database ===========\n");
	fprintf(stdout,"(%s): %d tests executed, %s%d tests passed%s, %s%d tests failed%s.\n",
														prog,
														count,
														GREEN_TEXT,
														passed,
														REGULAR_TEXT,
														RED_TEXT,failed,REGULAR_TEXT);
	if(failed != 0){
		fprintf(stderr,"(%s): %sFAILED%s!\n",prog,RED_TEXT,REGULAR_TEXT);
		fprintf(stderr,"(%s): ",prog);
		for(int i= 0;i < failed;i++){
			if((failed - i) > 1)
				fprintf(stderr,"%s%s%s, ",RED_TEXT,failed_test[i],REGULAR_TEXT);
			else
				fprintf(stderr,"%s%s%s.\n",RED_TEXT,failed_test[i],REGULAR_TEXT);
		}
		close_lua();
		return 0;
	}
	fprintf(stderr,"(%s): %sPASSED%s!\n",prog,GREEN_TEXT,REGULAR_TEXT);
	close_lua();
	return 0;
}
