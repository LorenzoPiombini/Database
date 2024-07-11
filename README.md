# ISAM Database in C

This ISAM database program enables dynamic creation of file definitions at runtime,  without the need to hardcode each type.

## Features

- **Dynamic File Definition**: Easily define file fields dynamically at runtime by specifying field names, types, and initial values.
- **Supports Multiple Data Types**: Supports a variety of data types including integers, long, floats, strings, bytes, and doubles.

## Get Started

you can pull the repo, or dowload the files directly on your manchine.

the Make file has some test case commented out, you can delete the # and try them out, when you run

```plaintext
-$ make clean
```

this is the output you'll see:

```plaintext
rm -f obj/*.o
rm -f bin/*
rm *.dat *.inx
rm *core*
rm: cannot remove '*core*': No such file or directory
make: *** [Makefile:22: clean] Error 1
```

to compile the program you just run the make command inside of the project directory:

```plaintext
project-$ make
```

output:

``````plaintext
cc -c src/debug.c -o obj/debug.o -Iinclude -g
gcc -c src/file.c -o obj/file.o -Iinclude -g
gcc -c src/hash_tbl.c -o obj/hash_tbl.o -Iinclude -g
gcc -c src/input.c -o obj/input.o -Iinclude -g
gcc -c src/lock.c -o obj/lock.o -Iinclude -g
gcc -c src/main.c -o obj/main.o -Iinclude -g
gcc -c src/parse.c -o obj/parse.o -Iinclude -g
gcc -c src/record.c -o obj/record.o -Iinclude -g
gcc -c src/sort.c -o obj/sort.o -Iinclude -g
gcc -c src/str_op.c -o obj/str_op.o -Iinclude -g
gcc -o bin/isam.db obj/debug.o obj/file.o obj/hash_tbl.o obj/input.o obj/lock.o obj/main.o obj/parse.o obj/record.o obj/sort.o obj/str_op.o

``````

keep in mind that your project structure should be like this:

``````plaintext
.
\bin
\include
  |_______common.h
  |_______debug.h
  |_______file.h
  |_______hash_tbl.h
  |_______input.h
  |_______lock.h
  |_______ parse.h
  |_______record.h
  |_______sort.h
  |_______str_op.h
\Makefile
\obj
\src
  |_______debug.c
  |_______ file.c
  |_______ hash_tbl.c
  |_______ input.c
  |_______lock.c
  |_______main.c
  |_______ parse.c
  |_______record.c
  |_______sort.c
  |_______str_op.c

``````

create the folder **obj** and **bin** if you haven't done it yet

This C software has been developed on a Ubuntu 22.04.4 LTS Jammy Jellifish using gcc version 11.4.0, it has not been tested on Windows or MacOS and other Linux distros.

now you have the program is compiled and we can run it.

## How It Works

Users can dynamically specify the attributes of a business entity by defining each field's name, type, and initial value using a simple format.

### Example Usage

Here's how you can define a file dinamically:
if you want to create a file definition, you can simply do it without providing values, the following example will create a file named person.dat(and a file person.inx), with only the variables name, last name and age with the type, in the header of the file:

```plaintext
bin/isam.db -nf person -R name:TYPE_STRING:"last name":TYPE_STRING:age:TYPE_BYTE
```

note the "last name" field, if you want to write field with spaces you have to put the "" around the field name, or you can do the follwing(last_name underscore without ""):

```plaintext
bin/isam.db -nf person -R name:TYPE_STRING:last_name:TYPE_STRING:age:TYPE_BYTE
```

now we have an empty file with a definiton, and we can write data to it later.

you can also create file and provide values to the fields at the same time, let's break it in two step:

```plaintext
code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0

```

The line above, sets up three fields:

code: A string (char*) with the value "man78-g-hus".

price: A float with the value 33.56.

discount: A float with the value 0.0.

to put it all together this will create a new file:

```plaintext
bin/isam.db -nf item -d code:TYPE_STRING:man78-g-hus:price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0 -k jj6
```

note the flag -k, this provide an id for the record that you are adding or creating, you will use this key for CRUD operation.

## IMPORTANT

as for now you have to supply the key manually, it would make more sense if the program took care of the key authomatically, I am working on this.
___________________________________________________________________________________

the flag that you can provide as for now, are:

```plaintext
         -a - add record to a file.
         -n - create a new database file
         -f - [required] path to file (file name)
         -r - look for the record key provided in the specified file. 
         -d - variables name and type <variableName>:TYPE_INT:12.
         -D - delete the record  provided for specified file.
         -k - specify the record id, the program will save, retrive and delete the record based on this id.
         -R - define a file definitions witout values.
         -t - list of available types. this flag will exit the program.
```

now the  file is created, and the base definition would be made by 3 variables code, price and discount.

this base definition cannot change anymore, you can add to it (up to 200 fields foor each file), but everytime you add a record to a file, you need to provide at least the correct definition. if you try to do the follwing ( notes the different flag -a for adding to an existing file):

```plain text
bin/isam.db -f item -a code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_STRING:0.0 -k jj7
```

this will fail, the program check the new input against the header, thus, it  knows that discount is supposed to be a float, the message will be:

```plain text
Schema different than file definition.
Files Closed successfully!
```

now you have a file called item.dat and a file named item.inx which will be the index file (an Hash table).
to retrive the data you run the follwing:

```plain text
bin/isam.db -f item -r jj6
```

this will display the data to the stdout, if found.

## STILL DEVELOPING

I am making this project to better learn C, it is based on a system that i work with, which is very poerfull but it is build with a custom lenguage and it is a barrier for new developers, so I've decided to create a system in C, a very well known lenguage, this is still a work in porgress, and help is very much appreciated. THANKS!
