#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include "build.h"
#include "hash_tbl.h"
#include "helper.h"
#include "file.h"
#include "str_op.h"
#include "debug.h"

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
	//	printf("%s\n%s\ni is %d\n", lines[i-1], lines[1],i);
	//	getchar();

	/*creates two file for our system */
	int fd_index = -1, fd_data = -1;
	char **files = two_file_path(file_path);

	fd_index = create_file(files[0]);
	fd_data = create_file(files[1]);

	/* create target file */
	if (!create_empty_file(fd_data, fd_index, recs))
	{
		printf("create empty file failed, build.c:%d.\n", __LINE__ - 1);
		close_file(2, fd_index, fd_data);
		delete_file(2, files[0], files[1]);
		free(files[0]), free(files[1]), free(files);
		return 0;
	}

	HashTable ht = {0, NULL, write_ht};

	begin_in_file(fd_index);
	if (!read_index_file(fd_index, &ht))
	{
		printf("file pointer failed, build.c:%d.\n", __LINE__ - 1);
		close_file(2, fd_index, fd_data);
		delete_file(2, files[0], files[1]);
		free(files[0]), free(files[1]), free(files);
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

		if (append_to_file(fd_data, fd_index, file_path, k, str, files, &ht) == 0)
		{
			printf("%s\t%s\n i is %d\n", str, k, i);

			begin_in_file(fd_index);
			if (!ht.write(fd_index, &ht))
			{
				printf("could not write index file. build.c l %d.\n", __LINE__ - 1);
				destroy_hasht(&ht);
				free_strs(recs, 1, lines);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				return 0;
			}

			close_file(2, fd_index, fd_data);
			destroy_hasht(&ht);
			free(files[0]), free(files[1]), free(files);
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
		free(files[0]), free(files[1]), free(files);
		close_file(2, fd_index, fd_data);
		return 0;
	}

	destroy_hasht(&ht);
	free_strs(recs, 1, lines);
	free(files[0]), free(files[1]), free(files);
	close_file(2, fd_index, fd_data);
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

		for (i = 0; i < buffer[i] == '\0'; i++)
			if (!isdigit(buffer[i]))
			{
				is_num = 0;
				break;
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
	char **files_n = calloc(lines, sizeof(char *));
	char **schemas = calloc(lines, sizeof(char *));
	char *save = NULL;

	while (fgets(buffer, sizeof(buffer), fp))
	{
		buffer[strcspn(buffer, "\n")] = '\0';
		if (buffer[0] == '\0')
			continue;

		files_n[i] = strdup(strtok_r(buffer, "|", &save));
		schemas[i] = strdup(strtok_r(NULL, "|", &save));
		i++;
		save = NULL;
	}

	fclose(fp);

	/* create file */
	printf("%s\n", schemas[4]);
	int j = 0;
	for (j = 0; j < i; j++)
	{
		char **files = two_file_path(files_n[j]);
		int fd_index = create_file(files[0]);
		int fd_data = create_file(files[1]);

		if (!create_file_with_schema(fd_data, fd_index, schemas[j], 0, 0))
		{
			delete_file(2, files[0], files[1]);
			free_strs(2, 1, files);
			close_file(2, fd_data, fd_index);
			return 0;
		}

		free_strs(2, 1, files);
		close_file(2, fd_index, fd_data);
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
