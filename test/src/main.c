#include <stdio.h>
#include <string.h>
#include "test.h"


#define GREEN_TEXT "\033[32m"
#define REGULAR_TEXT "\033[0m"

static char prog[] = "test_suite";
int main(){

	int count = 0, passed = 0, failed = 0;
	fprintf(stdout,"======== Running test for database ===========\n");
	count++;
	if(create_file_test() == -1){
		failed++;
		fprintf(stderr,"(%s): create_file_test() failed\n",prog);
		fprintf(stdout,"(%s): %d tests executed, %d tests passed, %d tests failed",prog,count,passed,failed);
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
	/*PASTE NEW TEST FUNCTIONS BETWEEN THESE LINES*/


	/*===========================================*/

	fprintf(stdout,"======== END test for database ===========\n");
	fprintf(stdout,"(%s): %d tests executed, %d tests passed, %d tests failed\n",prog,count,passed,failed);
	if(failed != 0){
		fprintf(stderr,"(%s): FAILED!\n",prog);
		return 0;
	}
	fprintf(stderr,"(%s): %sPASSED%s!\n",prog,GREEN_TEXT,REGULAR_TEXT);
	return 0;
}
