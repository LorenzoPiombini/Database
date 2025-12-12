#ifndef _START_LUA_H
#define _START_LUA_H 1



#include "lua5.4/lua.h"
#include "lua5.4/lauxlib.h"
#include "lua5.4/lualib.h"
#include "start_lua.h"

#define LUA_NOT_INIT -2

extern lua_State *L;

int lua_init(void);
int execute_lua(const char *buff);
void close_lua(void);

#endif
