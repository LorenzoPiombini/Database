#ifndef LUA_START
#define LUA_START 1

#include "lua5.4/lua.h"
#include "lua5.4/lauxlib.h"
#include "lua5.4/lualib.h"

extern lua_State *L;

void init_lua();
int execute_lua_script(char *buf);


#endif
