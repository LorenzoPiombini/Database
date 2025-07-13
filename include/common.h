#ifndef COMMON_H
#define COMMON_H

/*global variable for importing*/
extern int __IMPORT_EZ;

/* global variable define when the program is running as a 
 * command-line utility program*/
extern int __UTILITY;

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
#define DIF 202 /* new record is different*/	

/* possible refactor for main.c*/
#define NEW_FILE 1 	/*0000 0001*/
#define DEL 2		/*0000 0010*/ 
#define UPDATE 4        /*0000 0100*/
#define LIST_DEF 8      /*0000 1000*/
#define DEL_FILE 16     /*0001 0000*/
#define BUILD 32	/*0010 0000*/
#define LIST_KEYS 64 	/*0100 0000*/
#define CREATE 128 	/*1000 0000*/
#define IMPORT_FROM_DATA 129 /*1000 0001*/
#define OPTIONS 130	/*1000 0010*/
#define INDEX_ADD 132   /*1000 0100*/
#define FILE_FIELD 136 /*1000 1000*/







#endif
