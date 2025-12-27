#ifndef LUA_START
#define LUA_START 1

#include <stdarg.h>
#include "lua5.4/lua.h"
#include "lua5.4/lauxlib.h"
#include "lua5.4/lualib.h"

extern lua_State *L;

int init_lua();
void clear_lua_stack();
int execute_lua_script(char *buf);
int execute_lua_function(char *func_name, char *func_sig, ...);
void close_lua();
void check_config_file();


#endif
