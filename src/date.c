#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "date.h"
#include "debug.h"
#include "str_op.h"

unsigned char is_date_this_week(char* str_d)
{
	/* take current time to understand which week are we in*/
	struct tm* current_t = get_now(); 
 
	/*put the input date in the tm struct from time.h*/
	struct tm input_date = {0};
	if(!convert_date_str(str_d, &input_date))
	{
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
	{
		day_w = 6;
	}else
	{
		day_w -= 1;
	}


	week_n = (day_y - day_w + ISO_W_ADJ) / 7;
	return week_n;
}


unsigned char convert_date_str(char *str, struct tm* input_date)
{
	int year = 0, day = 0, month = 0;
	
	if(sscanf(str,"%d-%d-%d",&month, &day, &year) < 3)
	{
		printf("date convert failed, %s:%d.\n", F,L-2);
		return 0;
	}

	input_date->tm_mday = day;
	input_date->tm_mon = month - 1;
	if (year < 99)
		year += 2000;
	
	input_date->tm_year = year - 1900;

	return 1;
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
		cp_dt[j] = key[i];
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
	time_t time_tm = (time_t) time;
	struct tm *date_t = localtime(&time_tm);
	
	memset(date_str,0,9); /* 9 is the size of "mm-gg-yy" plus the '\0'*/

	char day[3], month[3], year[3];

	if(number_of_digit(date_t->tm_mday) < 2)
	{
		if(snprintf(day,3,"%d%d",0,date_t->tm_mday) < 0)
		{
			printf("snprintf failed, %s:%d.\n",F,L-2);
			return -1;
		}
	}else
	{
		if(snprintf(day,3,"%d",date_t->tm_mday) < 0)
                {
                        printf("snprintf failed, %s:%d.\n",F,L-2);
                        return -1;
                }
	}
	

	if(number_of_digit(date_t->tm_mon) < 2)
	{
		unsigned char mon = date_t->tm_mon + 1;
		if(snprintf(month,3,"%d%d",0,mon) < 0)
		{
			printf("snprintf failed, %s:%d.\n",F,L-2);
			return -1;
		}
	}else
	{
		unsigned char mon = date_t->tm_mon + 1;
		if(snprintf(month,3,"%d",mon) < 0)
                {
                        printf("snprintf failed, %s:%d.\n",F,L-2);
                        return -1;
                }
	}

	/* this will be valid till year 2899 */
	if(number_of_digit(date_t->tm_year) == 3)
	{
		unsigned char y = date_t->tm_year > 200 ? date_t->tm_year - 200 : date_t->tm_year - 100;
		if(snprintf(year,3,"%d",y) < 0)
		{
			printf("snprintf failed, %s:%d.\n",F,L-2);
			return -1;
		}
	}else
	{
		printf("you have to refactor the years formula %s:%d.\n",F,L-11);
		return -1;
	}

	if(snprintf(date_str,9,"%s-%s-%s",month,day,year) < 0) {
		printf("snprintf failed, %s:%d.\n",F,L-2);
		return -1;
	}

	return 1;
}

long convert_str_date_to_seconds(char* date)
{
	struct tm input_date = {0};
	if(!convert_date_str(date, &input_date)) return -1;

	return (long) mktime(&input_date);
}

/*this is ment to be used only in timecard operation*/
int get_service()
{
	struct tm *now = get_now();
	if(now->tm_hour > 11 && now->tm_hour < 16)
	{
		if(now->tm_hour == 3 && now->tm_min > 30)
			return 2; /*DINNER*/
		else
			return 1; /*LUNCH*/
		
	}else if(now->tm_hour >= 16)
	{
		return 2; /*DINNER*/
	}

	return 0; /*BREAKFAST*/
}

