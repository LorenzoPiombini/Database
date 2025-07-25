#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#include "input.h"
#include "str_op.h"
#include "dtype.h"

void print_usage(char *argv[])
{
        printf("Usage: %s -f <database file>\n", argv[0]);
        printf("\t -a - add record to a file.\n");
        printf("\t -n - create a new database file\n");
        printf("\t -f - [required] path to file (file name)\n");
        printf("\t -F - create a file to be use as a field\n");
        printf("\t -c - creates the files specified in the txt file.\n");
        printf("\t -D - specify the index where you want to delete the record.\n");
        printf("\t -R - define a file definition witout values.\n");
        printf("\t -k - specify the record id, the program will perform CRUD ops based on this id.\n");
        printf("\t -t - list of available types. this flag will exit the program.\n");
        printf("\t -l - list the file definition specified with -f.\n");
        printf("\t -u - update the file specified by -f .\n");
        printf("\t -e - delete the file specified by -f .\n");
        printf("\t -x - list the keys value for the file specified by -f .\n");
        printf("\t -b - specify the file name (txt,csv,tab delimited file) to build from .\n");
        printf("\t -B - specify the file name (txt,csv,tab delimited file) to import the data from .\n");
        printf("\t -o - add options to a CRUD operation .\n");
        printf("\t -s - specify how many buckets the HashTable (index) will have.\n");
        printf("\t -i - specify how many indexes the file will have.\n");
        printf("\t -A - add indexes -i to the file specified by -f .\n");
        printf("\t -I - create only the .dat file specified by -f .\n");
}

void print_types(void)
{
        printf("Avaiable types:\n");
        printf("\tTYPE_INT, integer number, %ld bytes (%ld bits).\n", sizeof(int), 8 * sizeof(int));
        printf("\tTYPE_FLOAT, floating point number, %ld bytes (%ld bits).\n",
               sizeof(float), 8 * sizeof(float));
        printf("\tTYPE_LONG, large integer number, %ld bytes (%ld bits).\n",
               sizeof(long), 8 * sizeof(long));
        printf("\tTYPE_STRING, text rappresentation, variable length. \"Hello\" is %ld bytes.\n",
               strlen("Hello"));
        printf("\tTYPE_BYTE, small unsigned integer number, %ld bytes (%ld bits).\n",
               sizeof(unsigned char), 8 * sizeof(unsigned char));
        printf("\tTYPE_DOUBLE, floating point number, %ld bytes (%ld bits).\n",
               sizeof(double), 8 * sizeof(double));
}

int check_input_and_values(struct String file_path, struct String data_to_add, struct String key, char *argv[],
                           unsigned char del, unsigned char list_def, unsigned char new_file,
                           unsigned char update, unsigned char del_file, unsigned char build,
                           unsigned char create, unsigned char options, unsigned char index_add,
			   unsigned char file_field, unsigned char import_from_data)
{

        if (create && (!file_path.is_empty(&file_path)  || del || update || del_file || list_def ||
                       new_file || !key.is_empty(&key)  || !data_to_add.is_empty(&data_to_add) || build || index_add || import_from_data))
        {
                printf("option -c must be used by itself.\n");
                printf(" -c <txt-with-files-definitions>.\n");
                return 0;
        }

        if (index_add && (del || update || del_file || list_def ||
                          new_file || !key.is_empty(&key)|| !data_to_add.is_empty(&data_to_add) || build))
        {
                print_usage(argv);
                fprintf(stderr, "\nyou can use option -A only with options -f -i and -x\n");
                return 0;
        }

        if (!file_path.is_empty(&file_path) && (create || import_from_data))
        {
                print_usage(argv);
                return 0;
        }

        if (build && !file_path.is_empty(&file_path)    &&
            (del || update || del_file || list_def || new_file || !key.is_empty(&key) || !data_to_add.is_empty(&data_to_add)  ))
        {
                printf("you must use options -b only with option -f:\n\t-f <filename> -b <filename[txt,csv,tab delimited]> \n");
                print_usage(argv);
                return 0;
        }

	if(import_from_data && (!file_path.is_empty(&file_path)  || del || update || del_file || list_def ||
                       new_file || !key.is_empty(&key)   || !data_to_add.is_empty(&key) || build || index_add || create)){
                printf("option -B must be used by itself.\n");
                printf(" -B <txt with files data to import>.\n");
		fprintf(stderr,"!! the file system MUST exist already !!\n");
                return 0;
	}

        if (  !file_path.is_empty(&file_path)  || file_field){
		if(file_path.allocated){
			if(!is_file_name_valid(file_path.str)){
				printf("file name or path not valid.\n");
				return 0;
			}
		}else{
			if(!is_file_name_valid(file_path.base)){
				printf("file name or path not valid.\n");
				return 0;
			}

		}
		
        }


        if ((!data_to_add.is_empty(&data_to_add) || update) && key.is_empty(&key) )
        {
                printf("option -k is required.\n\n");
                print_usage(argv);
                return 0;
        }

        if (new_file && list_def)
        {
                printf("option -l can`t be used on new file, or at file creation.\n\n");
                print_usage(argv);
                return 0;
        }

        if (del_file && (new_file || list_def || !data_to_add.is_empty(&data_to_add)|| update || !key.is_empty(&key) ))
        {
                printf("you cannot use option -e with other options! Only -ef <fileName>.\n");
                print_usage(argv);
                return 0;
        }

        if ((del && key.is_empty(&key) ) && !options)
        {
                printf("missing record key, option -k.\n");
                print_usage(argv);
                return 0;
        }
        return 1;
}

int convert_options(char *options)
{
        size_t l = strlen(options);
        for (size_t i = 0; i < l; i++)
                options[i] = tolower(options[i]);

        if (strcmp(options, ALL_OP) == 0) return ALL;


        return -1;
}
