#include <stdio.h>
#include <time.h>
#include <string.h>
#include <ctype.h>
#include "date.h"
#include "debug.h"
#include "str_op.h"
#include "freestand.h"

#define SEC_IN_A_DAY 60*60*24

enum date_delim{
	DOT,
	SLASH,
	DASH
};

static int is_valid_date(char *date);

unsigned char is_date_this_week(char* str_d)
{
	/* take current time to understand which week are we in*/
	struct tm* current_t = get_now(); 
 
	/*put the input date in the tm struct from time.h*/
	struct tm input_date;
	memset(&input_date,0,sizeof(struct tm));

	if(!convert_date_str(str_d, &input_date)){
		printf("error date convert. %s:%d.\n",F,L-2);
		return 0;
	}	

	if(mktime(&input_date) == -1)
	{
		printf("mktime failed. %s:%d.\n",F,L-2);
		return 0;
	}

	return get_week_number(current_t) == get_week_number(&input_date);
}

int get_week_number(struct tm* time_in)
{
	int day_y = time_in->tm_yday + 1;
	int day_w = time_in->tm_wday;
	int week_n;
	
	/*ISO week the first day is monday */
	/*this way the program knows that 0 is monday and 6 is sunday*/
	if(day_w == 0)
		day_w = 6;
	else
		day_w -= 1;


	week_n = (day_y - day_w + ISO_W_ADJ) / 7;
	return week_n;
}


int convert_date_str(char *str, struct tm* input_date)
{
	int year = 0, day = 0, month = 0;
	if(is_valid_date(str) == -1) return -1;

	if(extract_numbers_from_string(str,strlen(str),"%d-%d-%d",&month, &day, &year) == -1)
	{
		printf("date convert failed, %s:%d.\n", F,L-2);
		return -1;
	}

	input_date->tm_mday = day;
	input_date->tm_mon = month - 1;
	if (year < 99)
		year += 2000;
	
	input_date->tm_year = year - 1900;

	return 0;
}

unsigned char extract_date(char* key, char *date)
{
	
	size_t first_d = strcspn(key,"-");
	size_t end = strcspn(key,"e");
	
	if(first_d == strlen(key)) {
		printf("no date in the key. %s:%d.\n",F,L-2);
		return 0;
	}
	
	size_t start = first_d - 2;	

	size_t i = 0;
	int j = 0;
	size_t size = end - start + 1;
	char cpy_dt[size];
	memset(cpy_dt,0,size);

	for(i = start, j = 0; i < end; i++) {
		cpy_dt[j] = key[i];
		j++;
	}
		
	strncpy(date,cpy_dt,size);
	return 0;
}

unsigned char w_day(void)
{
	time_t now = time(NULL);
	struct tm* curr_t = localtime(&now); 
	unsigned char day = 0;
	if(curr_t->tm_wday == 0) {
		day = 6;
	}else {
		day = (unsigned char)curr_t->tm_wday - 1;
	}

	return day;
}	

struct tm* get_now(void)
{
	time_t now = time(NULL);
	return localtime(&now);
}

struct tm* mk_tm_from_seconds(long time)
{
	time_t t = (time_t)time;
	return localtime(&t);
}

unsigned char is_today(long time)
{
	struct tm*  time_on_file = mk_tm_from_seconds(time);

	/*-- this lines are needed because the way localtime() function works	 --*/
	/*-- every time you use localtime() the static allocated struct tm *  	 --*/
	/*-- will be overwritten, if we do not store the data of time_on_file    --*/
	/*-- now and time_on_file will be pointing at the same memory location   --*/
	/*-- returning always 1 (TRUE), which will not be ideal for timecard ops --*/
	
	int tof_m = time_on_file->tm_mon;
	int tof_md = time_on_file->tm_mday;

	/*------------------------------------------------------------------------*/

	struct tm* now = get_now();
	return (now->tm_mon == tof_m) && (now->tm_mday == tof_md);
}

long now_seconds()
{
	return(long)time(NULL);
}

int create_string_date(long time, char* date_str)
{
	time_t time_tm = (time_t) time + (60*60*5);
	struct tm *date_t = localtime(&time_tm);
	
	memset(date_str,0,9); /* 9 is the size of "mm-gg-yy" plus the '\0'*/

	char day[3];
	char month[3];
	char year[3];

	memset(day,0,3);
	memset(month,0,3);
	memset(year,0,3);
	if(date_t->tm_mday < 10 && date_t->tm_mon < 9){
		/*create string day*/
		day[0] = '0';	
		day[1] = date_t->tm_mday + '0';
		month[0] = '0';
		month[1] = (date_t->tm_mon + 1) + '0';
		if(copy_to_string(date_str,9,"%s-%s-%d",
									day,
									month,
									(date_t->tm_year + 1900) - 2000) == -1){
			printf("copy_to_string failed, %s:%d.\n",F,L-2);
			return -1;
		}
	}else if(date_t->tm_mday < 10){
		day[0] = '0';	
		day[1] = date_t->tm_mday + '0';
		if(copy_to_string(date_str,9,"%d-%s-%d",date_t->tm_mon + 1,
												day,
												date_t->tm_year - 100) == -1){
                        printf("copy_to_string failed, %s:%d.\n",F,L-2);
                        return -1;
		}
		return 0;
	}else if(date_t->tm_mon < 9){
		month[0] = '0';
		month[1] = (date_t->tm_mon + 1) + '0';
		if(copy_to_string(date_str,9,"%s-%d-%d",month,
											date_t->tm_mday,
											date_t->tm_year -100) == -1){
                        printf("copy_to_string failed, %s:%d.\n",F,L-2);
                        return -1;
		}
		return 0;
	}


	if(copy_to_string(date_str,9,"%d-%d-%d",date_t->tm_mon+1,
											date_t->tm_mday,
											date_t->tm_year-100) == -1){
		printf("copy_to_string failed, %s:%d.\n",F,L-2);
		return -1;
	}
	return 0;
}

long convert_str_date_to_seconds(char* date)
{
	struct tm input_date;
	memset(&input_date,0,sizeof(input_date));
	if(convert_date_str(date, &input_date) == -1) return -1;

	return (long) mktime(&input_date);
}

/*this is ment to be used only in timecard operation*/
int get_service()
{
	struct tm *now = get_now();
	if(now->tm_hour > 11 && now->tm_hour < 16){
		if(now->tm_hour == 3 && now->tm_min > 30)
			return 2; /*DINNER*/
		else
			return 1; /*LUNCH*/
		
	}else if(now->tm_hour >= 16){
		return 2; /*DINNER*/
	}

	return 0; /*BREAKFAST*/
}

static int is_valid_date(char *date)
{
	char *p = date;
	ui8 dash = 0;
	ui8 slash = 0;
	ui8 dot = 0;
	for(; *p != '\0';p++){
		if(isalpha(*p)) return -1;
		if(*p == ' ' || *p == '\t') return -1;
		if(*p == '\\' || *p == '/') slash++;
		if(*p == '.') dot++;
		if(*p == '-') dash++;
		if(isdigit(*p)) continue;
	}

	if (slash == 2) return SLASH;
	if (dot == 2) return DOT;
	if (dash == 2) return DASH;
	return -1;

}


ui32 convert_date_to_number(char *date)
{
	
	long seconds = 0;
	if((seconds = convert_str_date_to_seconds(date)) == -1){
		fprintf(stderr,"convert_str_date_to_seconds failed, %s:%d\n",__FILE__,__LINE__-1);
		return 0;
	}
	return seconds / (SEC_IN_A_DAY);
}

int convert_number_to_date(char *date, int date_number)
{
	long second = date_number * SEC_IN_A_DAY;		
	/*date MUST be 10 char long (MM-DD-YYYY\0)*/
	if(create_string_date(second,date) == -1 ) return -1;
	return 0;
}
