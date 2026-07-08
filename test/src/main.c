#include <stdio.h>
#include "test.h"


static char prog[] = "test_suite";
int main(){

	int count = 0, passed = 0, failed = 0;
	fprintf(stdout,"======== Running test for database ===========\n");
	count++;
	if(create_file_test() == -1){
		failed++;
		fprintf(stderr,"(%s): create_file_test() failed\n",prog);
		fprintf(stdout,"(%s): %d, tests executed, %d tests passed, %d tests failed",prog,count,passed,failed);
	} else{
		passed++;
	}
	/*PASTE NEW TEST FUNCTIONS BETWEEN THESE LINES*/


	/*===========================================*/

	fprintf(stdout,"======== END test for database ===========\n");
	fprintf(stdout,"(%s): %d, tests executed, %d tests passed, %d tests failed\n",prog,count,passed,failed);
	if(failed != 0){
		fprintf(stderr,"(%s): FAILED!\n",prog);
		return 0;
	}
	fprintf(stderr,"(%s): PASSED !\n",prog);
	return 0;
}
