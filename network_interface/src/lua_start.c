#include <stdio.h>
#include <stdarg.h>
#include "lua_start.h"

lua_State *L = NULL;

static int load(lua_State *L, char *file_config);
int init_lua()
{
	L = luaL_newstate();
	luaL_openlibs(L);
	if(load(L,"db_config.lua") == -1) return -1;
}


void close_lua()
{
	lua_close(L);
}

int execute_lua_function(char *func_name, char *func_sig, ...)
{
	va_list vl;
	int nargs, nres;

	va_start(vl,func_sig)
	lua_getglobal(L,func_name);

	for(narg = 0; *sig != '\0'; narg++,sig++){
		luaL_checkstack(L,1,"too many arguments");
		switch(*sig){
		case 'r':/* Record_f */	
			break;
		case 'd':/* double */	
			break;
		case 'i':/* integer*/	
			break;
		case 'r':/* Record_f */	
			break;
		}
	}

	int err = luaL_loadstring(L,buf) || lua_pcall(L,0,0,0);
	if(err){
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		lua_pop(L,1);
		return -1;
	}

	return 0;
}

static int load(lua_State *L, char *file_config)
{
	if(luaL_loadfile(L,file_config) || lua_pcall(L,0,0,0)){
		return -1;
	}
	return 0;
}
