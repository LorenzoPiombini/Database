#ifndef __TYPES_H_
#define __TYPES_H_


# if __WORDSIZE == 64
typedef unsigned char		uint8_t;
typedef unsigned short		uint16_t;
typedef 	 short		int16_t;
typedef unsigned int		uint32_t;
typedef 	 int		int32_t;
typedef unsigned long		uint64_t;
typedef 	 long		int64_t;
# else
typedef unsigned char		uint8_t;
typedef unsigned short		uint16_t;
typedef 	 short		int16_t;
typedef unsigned int		uint32_t;
typedef 	 int		int32_t;
typedef unsigned long		uint32_t;
typedef 	 long		int32_t;
#endif

/*signed short max and min*/
#  define SHRT_MIN	(-32768)
#  define SHRT_MAX	32767

/* unsigned short int max.  (Minimum is 0.)  */
#  define USHRT_MAX	65535

/* signed int min and max  */
#  define INT_MIN	(-INT_MAX - 1)
#  define INT_MAX	2147483647

/* unsigned int' max  (Minimum is 0.)  */
#  define UINT_MAX	4294967295

/* signed long int min and max */
#  if __WORDSIZE == 64
#   define LONG_MAX	9223372036854775807L
#else
#   define LONG_MAX	2147483647L
#endif

#  define LONG_MIN	(-LONG_MAX - 1L)

/* unsigned long max  (Minimum is 0.)  */
#  if __WORDSIZE == 64
#   define ULONG_MAX	18446744073709551615UL
#  else
#   define ULONG_MAX	4294967295UL
#  endif


#endif /*types.h*/
