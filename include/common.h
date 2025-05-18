#ifndef COMMON_H
#define COMMON_H

#define STATUS_ERROR -1
#define SCHEMA_ERR 2
#define SCHEMA_EQ 3
#define SCHEMA_NW 4
#define SCHEMA_CT 5
#define UPDATE_OLD 6 // you can overwrite the old record with no worries!
#define UPDATE_OLDN 8
#define ALREADY_KEY 9
#define EFLENGTH 10 /*file path or name too long */
#define SCHEMA_EQ_NT 11 /*schema eq but NO type in the schema file*/
#define SCHEMA_CT_NT 12 /*intput is part of the schema but NO type in the schema file*/
#define SCHEMA_NW_NT 13 /*intput is part of the schema but also new part no part of the schema*/
#define NTG_WR 14 /* nothing to write to the file*/
#define E_RCMP 201 /* record comparison failed*/	
#endif
