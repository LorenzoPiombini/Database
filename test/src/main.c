#include <stdio.h>
#include <string.h>
#include "test.h"


#define GREEN_TEXT "\033[32m"
#define RED_TEXT "\033[31m"
#define REGULAR_TEXT "\033[0m"

static char prog[] = "test_suite";
int main()
{
	int count = 0, passed = 0, failed = 0;
	fprintf(stdout,"======== Running test for database ===========\n");
	count++;
	if(create_file_test() == -1){
		failed++;
		fprintf(stderr,"(%s): create_file_test() failed\n",prog);
	} else{
		passed++;
	}

	count++;
	FILE *fp = popen("touch test_one.inx test_one.dat test_one.sch","r");
	if(!fp){
		failed++;
		fprintf(stderr,"(%s): fopen() failed\n",prog);
	}
	
	pclose(fp);
	char files[3][256] = {0};
	strncpy(files[0],"test_one.inx",strlen("test_one.inx"));
	strncpy(files[1],"test_one.dat",strlen("test_one.inx"));
	strncpy(files[2],"test_one.sch",strlen("test_one.inx"));
	if(delete_file_test(files) == -1){
		failed++;
	}else{
		passed++;
	}

	count++;
	if(lock_file_test() == -1){
		failed++;
	}else{
		passed++;
	}

	if(init_lua("test/lua/old_test.lua") == -1) return -1;

	count++;
	if(LUA_port_table_to_record_test() == -1){
		failed++;
	}else{
		passed++;
	}

	count++;
	if(LUA_test_w_rec() == -1){
		failed++;
	}else{
		passed++;
	}
		
	count++;
	if(LUA_test_w_rec_cache() == -1){
		failed++;
	}else{
		passed++;
	}

	/*PASTE NEW TEST FUNCTIONS BETWEEN THESE LINES*/


	/*===========================================*/

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
		close_lua();
		return 0;
	}
	fprintf(stderr,"(%s): %sPASSED%s!\n",prog,GREEN_TEXT,REGULAR_TEXT);
	close_lua();
	return 0;
}
