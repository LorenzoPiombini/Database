#ifndef SORT_H
#define SORT_H

#include "record.h"
 
void swap(ValueType* array, int first_index, int second_index);
void swap_str(char** array, int first_index, int second_index);
int pivot(ValueType* array, int pivot_index, int end_index);
int pivot_str(char** array, int pivot_index, int end_index);
void quick_sort(ValueType* array, int left_index, int right_index);
void quick_sort_str(char** array, int left_index, int right_index);

#endif
