#include <stdio.h>
#include <string.h>
#include "debug.h"
#include "record.h"


void print_record(Record_f *rec){
	int i = 0;
	printf("#################################################################\n\n");
	printf("the Record data are: \n");
        for(i = 0; i < rec->fields_num; i++){
           printf("%s: \t",rec->fields[i].field_name);
                                        switch(rec->fields[i].type){
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
	printf("\n#################################################################\n\n");

}

void print_schema(Schema sch){
	if(sch.fields_name && sch.types){
		printf("Schema: \n");
                printf("number of fields:\t %d.\n", sch.fields_num);
                printf("fields:\t ");
                                        
		int i = 0;
                
                for(i; i < sch.fields_num; i++)
			printf("%s, ",sch.fields_name[i]);

		printf("\nTypes:\t ");
                for(i = 0; i < sch.fields_num; i++)
                        printf("%d, ",sch.types[i]);
        }
	printf("\n");

}

void print_header(Header_d hd){
	printf("Header: \n");
       	printf("id: %x,\nversion: %d",hd.id_n,hd.version);
        printf("\n");
       	print_schema(hd.sch_d);
}

void print_size_header(Header_d hd){
	size_t sum = 0;

	sum += sizeof(hd.id_n) + sizeof(hd.version) + sizeof(hd.sch_d.fields_num) + sizeof(hd.sch_d);
	int i = 0;

	//for(i ; i < hd.sch_d.fields_num ; i++){
		//sum += strlen(hd.sch_d.fields_name[i]);
		//sum += sizeof(hd.sch_d.types[i]);
	//}

	//sum += hd.sch_d.fields_num; // acounting for n '\0'
	printf("\n\n\nSize of Header: %ld\n\n\n", sum);
} 
