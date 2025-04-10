#if defined(_WIN32) || (defined(__APPLE__) && defined(__MACH__))
#include "win_file.c"
#elif defined(__linux__)
#include "linux_file.c"
#else 
#error "system not supported"
#endif /*file.c*/
