#include <stdio.h>
#include <string.h>
#include "debug.h"
#include "record.h"

void loop_str_arr(char **str, int len)
{

        int i = 0;
        for (i = 0; i < len; i++)
                printf("%s, ", str[i]);

        printf("\n");
}
void print_record(int count, Record_f **recs)
{

        int i = 0, j = 0, max = 0;
        printf("#################################################################\n\n");
        printf("the Record data are: \n");

        for (j = 0; j < count; j++)
        {
                Record_f *rec = recs[j];
                for (i = 0; i < rec->fields_num; i++)
                {
                        if (max < (int)strlen(rec->fields[i].field_name))
                        {
                                max = (int)strlen(rec->fields[i].field_name);
                        }
                }

                for (i = 0; i < rec->fields_num; i++)
                {
                        printf("%-*s\t", max++, rec->fields[i].field_name);
                        switch (rec->fields[i].type)
                        {
                        case TYPE_INT:
                                printf("%d\n", rec->fields[i].data.i);
                                break;
                        case TYPE_LONG:
                                printf("%ld\n", rec->fields[i].data.l);
                                break;
                        case TYPE_FLOAT:
                                printf("%.2f\n", rec->fields[i].data.f);
                                break;
                        case TYPE_STRING:
                                printf("%s\n", rec->fields[i].data.s);
                                break;
                        case TYPE_BYTE:
                                printf("%u\n", rec->fields[i].data.b);
                                break;
                        case TYPE_DOUBLE:
                                printf("%.2f\n", rec->fields[i].data.d);
                                break;
                        default:
                                break;
                        }
                }
        }
        printf("\n#################################################################\n\n");
}

void print_schema(Schema sch)
{
        if (sch.fields_name && sch.types)
        {
                //	printf("Schema: \n");
                //        printf("number of fields:\t %d.\n", sch.fields_num);
                printf("definition:\n");
                int i = 0, max = 0;

                char c = ' ';
                printf("Field Name%-*cType\n", 11, c);
                printf("__________________________\n");
                for (i = 0; i < sch.fields_num; i++)
                {
                        printf("%s%-*c", sch.fields_name[i], (int)(15 - strlen(sch.fields_name[i])), c);
                        switch (sch.types[i])
                        {
                        case TYPE_INT:
                                printf("int.\n");
                                break;
                        case TYPE_FLOAT:
                                printf("float.\n");
                                break;
                        case TYPE_LONG:
                                printf("long.\n");
                                break;
                        case TYPE_BYTE:
                                printf("byte.\n");
                                break;
                        case TYPE_DOUBLE:
                                printf("double.\n");
                                break;
                        case TYPE_STRING:
                                printf("string.\n");
                                break;
                        default:
                                printf("\n");
                                break;
                        }
                }
        }

        printf("\n");
}

void print_header(Header_d hd)
{
        printf("Header: \n");
        printf("id: %x,\nversion: %d", hd.id_n, hd.version);
        printf("\n");
        print_schema(hd.sch_d);
}

size_t compute_size_header(Header_d hd)
{
        size_t sum = 0;

        sum += sizeof(hd.id_n) + sizeof(hd.version) + sizeof(hd.sch_d.fields_num) + sizeof(hd.sch_d);
        int i = 0;

        for (i = 0; i < hd.sch_d.fields_num; i++)
        {
                if (hd.sch_d.fields_name[i])
                {
                        sum += strlen(hd.sch_d.fields_name[i]);
                }

                sum += sizeof(hd.sch_d.types[i]);
        }

        sum += hd.sch_d.fields_num; // acounting for n '\0'
                                    // printf("\n\n\nSize of Header: %ld\n\n\n", sum);
}
