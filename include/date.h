#ifndef _DATE_H
#define _DATE_H

#include <time.h>
#include "types.h"

#define ISO_W_ADJ 10
#define DD_MM_YYYY 0
#define YYYY_MM_DD 1

unsigned char is_date_this_week(char* str_d,int format);
int get_week_number(struct tm* time_in);
int convert_date_str(int format, char *str, struct tm* input_date);
unsigned char extract_date(char* key, char *date);
unsigned char w_day(void);
struct tm* get_now(void);
struct tm* mk_tm_from_seconds(long time);
unsigned char is_today(long time);
long now_seconds();
int create_string_date(long time, char* date_str, int format);
long convert_str_date_to_seconds(char* date,int format);
long third_friday_of_the_month();
int convert_number_to_date(char *date, int date_number);
ui32 convert_date_to_number(int format,char *date);
long next_friday(long seconds);
char *display_today();
int is_past(int h,int m);
int is_date_today(char *date, int format);

#endif /* date.h */
