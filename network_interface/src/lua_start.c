#include <stdio.h>
#include "lua_start.h"

lua_State *L = NULL;

void init_lua()
{
	L = luaL_newstate();
	luaL_openlibs(L);
}


void close_lua()
{
	lua_close(L);
}

int execute_lua_script(char *buf)
{
	int err = luaL_loadstring(L,buf) || lua_pcall(L,0,0,0);
	if(err){
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		lua_pop(L,1);
		return -1;
	}

	return 0;
}
