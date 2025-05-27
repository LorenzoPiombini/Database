#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include "build.h"
#include "file.h"
#include "helper.h"
#include "str_op.h"
#include "debug.h"


/*this functionality is not implemented yet*/

unsigned char build_from_txt_file(char *file_path, char *txt_f)
{
	FILE *fp = fopen(txt_f, "r");
	if (!fp)
	{
		printf("file open failed, build.c l %d.\n", __LINE__ - 2);
		return 0;
	}

	/*get record number and buffer for the longest line*/
	int line_buf = get_number_value_from_txt_file(fp);
	int recs = get_number_value_from_txt_file(fp);
	if (line_buf == 0 || recs == 0)
	{
		printf("no number in file txt, build.c:%d.\n", __LINE__ - 4);
		return 0;
	}

	recs += SFT_B;
	/*load the txt file in memory */
	char buf[line_buf];
	char **lines = calloc(recs, sizeof(char *));
	if (!lines)
	{
		printf("calloc failed, %s:%d", __FILE__, __LINE__ - 2);
		return 0;
	}

	printf("copying file system...\nthis might take a few minutes.\n");
	int i = 0;
	printf("position in text file is %ld\n.", ftell(fp));
	while (fgets(buf, sizeof(buf), fp))
	{
		buf[strcspn(buf, "\n")] = '\0';
		lines[i] = strdup(buf);
		i++;
	}

	fclose(fp);

	/*creates three files for the system */
	int fd_index = -1;
	int fd_data = -1;
	int fd_schema = -1;

	char files[3][MAX_FILE_PATH_LENGTH]= {0};
	three_file_path(file_path, files);

	fd_index = create_file(files[0]);
	fd_data = create_file(files[1]);
	fd_schema = create_file(files[2]);

	/* create target file */
	if (!create_empty_file(fd_schema, fd_index, recs))
	{
		printf("create empty file failed, build.c:%d.\n", __LINE__ - 1);
		close_file(3,fd_schema, fd_index, fd_data);
		delete_file(3, files[0], files[1],files[2]);
		return 0;
	}

	HashTable ht = {0};
	ht.write = write_ht;

	begin_in_file(fd_index);
	if (!read_index_file(fd_index, &ht))
	{
		printf("file pointer failed, build.c:%d.\n", __LINE__ - 1);
		close_file(3,fd_schema, fd_index, fd_data);
		delete_file(3, files[0], files[1],files[2]);
		return 0;
	}

	char *sv_b = NULL;
	int end = i;
	for (i = 0; i < end; i++)
	{
		char *str = strtok_r(lines[i], "|", &sv_b);
		char *k = strtok_r(NULL, "|", &sv_b);
		// printf("%s\n%s",str,k);
		// getchar();

		if (!k || !str)
		{
			printf("%s\t%s\ti is %d\n", str, k, i);
			continue;
		}

		if (append_to_file(fd_data, &fd_schema, file_path, k, files,str, &ht) == 0)
		{
			printf("%s\t%s\n i is %d\n", str, k, i);

			begin_in_file(fd_index);
			if (!ht.write(fd_index, &ht)) {
				printf("could not write index file. build.c l %d.\n", __LINE__ - 1);
				destroy_hasht(&ht);
				free_strs(recs, 1, lines);
				close_file(3,fd_schema, fd_index, fd_data);
				delete_file(3, files[0], files[1],files[2]);
				return 0;
			}

			close_file(3,fd_schema, fd_index, fd_data);
			destroy_hasht(&ht);
			free_strs(recs, 1, lines);
			return 0;
		}
	}

	begin_in_file(fd_index);
	if (!ht.write(fd_index, &ht))
	{
		printf("could not write index file. build.c l %d.\n", __LINE__ - 1);
		destroy_hasht(&ht);
		free_strs(recs, 1, lines);
		close_file(3,fd_schema, fd_index, fd_data);
		return 0;
	}

	destroy_hasht(&ht);
	free_strs(recs, 1, lines);
	close_file(3,fd_schema, fd_index, fd_data);
	return 1;
}

int get_number_value_from_txt_file(FILE *fp)
{

	char buffer[250];
	unsigned short i = 0;
	unsigned char is_num = 1;
	if (fgets(buffer, sizeof(buffer), fp))
	{
		buffer[strcspn(buffer, "\n")] = '\0';

		for (i = 0; buffer[i] != '\0'; i++)
		{
			if (!isdigit(buffer[i]))
			{
				is_num = 0;
				break;
			}
		}
	}

	if (!is_num)
	{
		printf("first line is not a number, %s:%d.\n", __FILE__, __LINE__ - 1);
		return 0;
	}

	return atoi(buffer);
}

unsigned char create_system_from_txt_file(char *txt_f)
{
	FILE *fp = fopen(txt_f, "r");
	if (!fp)
	{
		printf("failed to open %s. %s:%d.", txt_f, F, L - 3);
		return 0;
	}
	int lines = 0, i = 0;
	int size = return_bigger_buffer(fp, &lines);
	char buffer[size];
	char *save = NULL;
	char **files_n = calloc(lines, sizeof(char *));
	char **schemas = calloc(lines, sizeof(char *));
	if(!files_n || !schemas){
		__er_calloc(F,L-3);
		if(files_n) free(files_n);
		if(schemas) free(schemas);
		return 0;
	}

	int buckets[lines];
	int indexes[lines];
	int file_field[lines];
	memset(buckets,0,lines);
	memset(indexes,0,lines);
	memset(file_field,0,lines);


	while (fgets(buffer, sizeof(buffer), fp))
	{
		buffer[strcspn(buffer, "\n")] = '\0';
		if (buffer[0] == '\0')
			continue;

		files_n[i] = strdup(strtok_r(buffer, "|", &save));
		schemas[i] = strdup(strtok_r(NULL, "|", &save));
		char *endp;
		char *t = strtok_r(NULL, "|",&save);
		long l = strtol(t,&endp,10);
		if(*endp == '\0')
			buckets[i] = (int) l; 
		else 
			buckets[i] = 0;


		t = strtok_r(NULL, "|",&save);
		l = strtol(t,&endp,10);
		if(*endp == '\0')
			indexes[i] = (int) l; 
		else 
			indexes[i] = 0; 

		t = strtok_r(NULL, "|",&save);
		l = strtol(t,&endp,10);
		if(*endp == '\0')
			file_field[i] = (int) l; 
		else 
			file_field[i] = 0; 

		i++;
		save = NULL;
	}

	fclose(fp);

	/* create file */
	int j = 0;
	for (j = 0; j < i; j++)
	{
		char files[3][MAX_FILE_PATH_LENGTH] = {0};
		three_file_path(files_n[j],files);
		int fd_schema = -1;
		int fd_index = -1;
		int fd_data = -1;

		if(file_field[j]){
			fd_schema = create_file(files[2]);

			if (file_error_handler(1,fd_schema) !=0){
				fprintf(stderr, "system already exist!\n");
				free_strs(lines, 2, files_n, schemas);
				return 0;
			}
		}else{
			fd_index = create_file(files[0]);
			fd_data = create_file(files[1]);
			fd_schema = create_file(files[2]);
			if (file_error_handler(3,fd_schema,fd_data,fd_index) !=0){
				fprintf(stderr, "system already exist!\n");
				free_strs(lines, 2, files_n, schemas);
				return 0;
			}
		}


		if (create_file_with_schema(fd_schema, fd_index, schemas[j], buckets[j], indexes[j],file_field[j]) == -1){
			if(file_field[j]){
				delete_file(1,files[2]);
				close_file(1,fd_schema);
			}else{
				delete_file(3, files[0], files[1],files[2]);
				close_file(3, fd_data, fd_index,fd_schema);
			}
			free_strs(lines, 2, files_n, schemas);
			return 0;
		}

		if(file_field[j])
			close_file(1,fd_schema);
		else
			close_file(3, fd_data, fd_index,fd_schema);
	}

	free_strs(lines, 2, files_n, schemas);
	return 1;
}

int return_bigger_buffer(FILE *fp, int *lines)
{
	char buffer[5000];
	int max = 0;
	while (fgets(buffer, sizeof(buffer), fp) != NULL)
	{
		(*lines)++;
		int temp = 0;
		if ((temp = (strcspn(buffer, "\n") + 1)) > max)
			max = temp;
	}
	rewind(fp);
	return max + 10;
}
