# ISAM Database in C

This ISAM database program enables dynamic creation of file definitions at runtime,  without the need to hardcode each type.

## Features

- **Dynamic File Definition**: Easily define file dynamically at runtime by specifying field names, types, and initial values, without hard coding each files (fields and variables are the same thing).
- **Supports Multiple Data Types**: Supports integers, long, floats, strings, bytes, and doubles.
- **CRUD**: You can easily add, update and delete records form the files.
  
## Get Started

To get started, you can either clone the repository or download the files directly to your machine.

## Makefile rules

- **default**: will build the source c files and create the executable isam.db.  
- **test**:
  - create new file and provide data to it.
  - checks for memory leaks when creating a new file with data.
  - create new file with no data.
  - adding the wrong data to an existing file
  - adding the correct data to an existing file.
  - adding partial but correct data to an existing file.
  - deleting data from an existing file
- **memory**:
  - checks the memory allocations for all the cases in tests.
- **clean**:
  - delete all the files created.
  - delete all core files dumped for debugging.
  - delete all the objects created and the binary executable isam.db
  - delete the commands created by the install rule, GET and LIST
- **install**:
- creates the follwing commands:
  - GET expects two arguments *file name* and the *record identifier* (key).
    - this command will fetch the specified record *(if found)* and will display it.
    - if you execute GET with no arguments will display the usage instruction.
  - LIST expects one argument *file name*.
    - this command will display the file definition for the specified file.
    - if you execute LIST with no arguments will display the usage instruction.
  - FILE expects two argument *file name* and *paramters string*.
    - this command will create a new file with definition as per the *paramters string*.
    - if you execute FILE with no arguments will display the usage instruction.

you need root privilege to run the install and clean rules.

organize your project structure as shown below:

``````plaintext
your-project-folder:
  |__\bin
  |__\include
  |    |_______common.h
  |    |_______debug.h
  |    |_______file.h
  |    |_______hash_tbl.h
  |    |_______input.h
  |    |_______lock.h
  |    |_______ parse.h
  |    |_______record.h
  |    |_______sort.h
  |    |_______str_op.h
  |__\Makefile
  |__\obj
  |__\src
     |_______debug.c
     |_______ file.c
     |_______ hash_tbl.c
     |_______ input.c
     |_______lock.c
     |_______main.c
     |_______parse.c
     |_______record.c
     |_______sort.c
     |_______str_op.c

``````

when you clone or download this repo, you do not have the folders **bin** and **obj**, you have to create them:

``````plaintext
[isam.db-C-language-main]$ mkdir bin obj
``````

take a look at the Makefile, for the GET and LIST command you will need to change the path for them to work:

``````plaintext
$(BINDIR)/GET:
  @if [ !  -f $@ ]; then \
    echo "Creating $@ . . ."; \
    echo "#!/bin/bash" > $@; \
    echo "#Check if both arguments are provided" >> $@; \
    echo "if [ -z \"\$$1\" ] || [ -z \"\$$2\" ]; then" >> $@; \
    echo "echo \"Usage: GET [file name] [record_id]\"" >> $@; \
    echo "exit 1" >> $@; \
    echo "fi" >> $@; \
    echo "" >> $@; \
    echo "/put/your/target/full/path/here/$(TARGET) -f \"\$$1\" -r \"\$$2\"" >> $@; \
    chmod +x $@; \
  fi
``````

change the line echo ***"/put/your/target/full/path/here/$(TARGET) -f \"\$$1\" -r \"\$$2\"" >> $@; \*** with your path, and the same apply to the python scripts.

if you want you can run all the rules at the same time each time you need it:

``````bash
[isam.db-C-language-main]$ sudo make clean; make; make test ;sudo  make install 
``````

this will clean your current directory, creates the objects, run the tests and create the
commands to browse the filesystem you created.

This C software has been developed on a Ubuntu 22.04.4 LTS Jammy Jellifish using gcc version 11.4.0, tested on:

- **Centos Stream Release 9** kernel: 5.14.0-479.el9.aarch64.
- **Fedora 36** kernel: 5.11.17-300.fc34.aarch64.

it has not been tested on Windows or MacOS and other Linux distros bisides the ones mentioned above.

## How It Works

you can dynamically create **Files** at run time, you will provide either  the field(s)(variable(s)) names with the type and value, or simply the name of the fileds and the type.

### Example Usage

Here's how you can define a file:
if you want to create a file, without providing values, you can choose one ofthe following commands:

```bash
[isam.db-C-language-main]$ bin/isam.db -nf person -R name:TYPE_STRING:"lastname":TYPE_STRING:age:TYPE_BYTE
```

```bash
[isam.db-C-language-main]$ bin/isam.db -nf person -R name:t_s:lastname:t_s:age:t_b
```

```bash
[isam.db-C-language-main]$ FILE person name:t_s:lastname:t_s:age:t_b
```

note the "last name" field, if you want to write field with spaces you have to put the "" around the field name, or you can do the follwing(last_name underscore without ""):

```bash
[isam.db-C-language-main]$ bin/isam.db -nf person -R name:TYPE_STRING:last_name:TYPE_STRING:age:TYPE_BYTE
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

## Note

Currently, you must manually supply a key for CRUD operations. Future versions will aim to automate key management.
___________________________________________________________________________________

the flag that you can provide as for now, are:

```plaintext
    -a - add record to a file.
    -n - create a new database file
    -f - [required] path to file (file name)
    -r - look for the record key provided in the specified file. 
    -d - variables name and type <variableName>:TYPE_INT:12.
    -D - delete the record  provided for specified file.
    -R - define a file definition witout values.
    -k - specify the record id, the program will save, retrice and delete the record based on this id.
    -t - list of available types. this flag will exit the program.
    -l - list the file definition specified with -f.
    -u - update the file specified by -f .
    -e - delete the file specified by -f .

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
