#ifndef _DATE_H
#define _DATE_H

#include <time.h>

unsigned char is_date_this_week(char* str_d);
int get_week_number(struct tm* time_in);
unsigned char convert_date_str(char *str, struct tm* input_date);
unsigned char extract_date(char* key, char *date);
unsigned char w_day(void);
struct tm* get_now(void);
struct tm* mk_tm_from_seconds(long time);
unsigned char is_today(long time);
long now_seconds();
int create_string_date(long time, char* date_str);
long convert_str_date_to_seconds(char* date);
int get_service();

#endif /* date.h */
