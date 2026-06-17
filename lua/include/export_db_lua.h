#ifndef _EXPORT_DB_LUA_
#define _EXPORT_DB_LUA 1

#include "lua5.4/lua.h"
#include "lua5.4/lauxlib.h"
#include "crud.h"
#include "file.h"
#include "record.h"

#define CACHE_SIZE 30
struct Cache cache[CACHE_SIZE] = {0};

int port_record(lua_State *L, struct Record_f *rec);
int port_table_to_record(lua_State *L, struct Record_f *rec);
#endif
