#ifndef _START_LUA_H
#define _START_LUA_H 1

#define LUA_NOT_INIT -2

int lua_init(void);
int execute_lua(const char *buff);
void close_lua(void);

#endif
