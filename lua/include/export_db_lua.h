#ifndef _EXPORT_DB_LUA_
#define _EXPORT_DB_LUA 1

#include "crud.h"
#include "record.h"

int port_record(lua_Status *L, struct Record_f *rec);
int port_table_to_record(lua_Status *L, struct Record_f *rec);
#endif
