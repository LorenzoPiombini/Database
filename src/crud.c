#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "file.h"
#include "record.h"
#include "lock.h"
#include "str_op.h"
#include "common.h"
#include "debug.h"


static char prog[] = "db";
static int open_files(char *file_name,int *fds, char files_name[3][1024]);
static int is_db_file(struct Header_d *hd, int *fds);

int get_record(char *file_name,struct Record_f *rec, void *key)
{
	/*open the file*/
	int fds[3];
	memset(fds,-1,sizeof(int)*3);

	char files[3][MAX_FILE_PATH_LENGTH] = {0};  
	if(open_files(file_name,fds,files) == -1) return STATUS_ERROR;

	/* init the Schema structure*/
	struct Schema sch = {0};
	memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);

	struct Header_d hd = {0, 0, sch};
	if(is_db_file(&hd,fds) == -1) return STATUS_ERROR;




}

static int is_db_file(struct Header_d *hd, int *fds)
{

	while((is_locked(3,fds[0],fds[1],fds[2])) == LOCKED);


	if (!read_header(fds[2], hd)) return STATUS_ERROR;

}
static int open_files(char *file_name, int *fds, char files[3][MAX_FILE_PATH_LENGTH])
{
	if(three_file_path(file_name, files) == EFLENGTH){
		fprintf(stderr,"(%s): file name or path '%s' too long",prog,file_name);
		return STATUS_ERROR;
	}
	
	int err = 0;
	int fd_index = open_file(files[0], 0);
	int fd_data = open_file(files[1], 0);
	int fd_schema = open_file(files[2], 0);

	/* file_error_handler will close the file descriptors if there are issues */
	if ((err = file_error_handler(2, fd_index, fd_data)) != 0) {
		if(err == ENOENT)
			fprintf(stderr,"(%s): File '%s' doesn't exist.\n",prog,file_name);
		else
			printf("(%s): Error in creating or opening files, %s:%d.\n",prog, F, L - 2);

		return STATUS_ERROR;
	}

	fds[0] = fd_index;
	fds[1] = fd_data;
	fds[2] = fd_schema;
	return 0;
}
