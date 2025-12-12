#include <stdio.h>


lua_State *L = NULL;
int lua_init(void)
{
	L = luaL_newstate();	
	if(!L) return -1;
	luaL_openlibs(L);
	return 0;
}


int execute_lua(const char *buff)
{
	if(!L) return LUA_NOT_INIT;

	int error = luaL_loadstring(L,(char*)buff) || lua_pcall(L,0,0,0);
	if (error){
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		lua_pop(L,1);
		return -1;
	}
	
	return 0;
}

void close_lua(void)
{
	if (L){
		lua_close(L);
	}
}
