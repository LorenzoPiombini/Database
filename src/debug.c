#include <stdio.h>
#include <string.h>
#include "debug.h"
#include "record.h"
#include "str_op.h"

void loop_str_arr(char **str, int len)
{

        int i = 0;
        for (i = 0; i < len; i++)
                printf("%s, ", str[i]);

        printf("\n");
}

void __er_file_pointer(char *file, int line)
{
        perror("file pointer: ");
        printf(" %s:%d.\n", file, line);
}

void __er_calloc(char *file, int line)
{
        printf("calloc failed, %s:%d.\n", file, line);
}
