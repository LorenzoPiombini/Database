# Database in C

This database utility enables dynamic creation of file definitions at runtime,  without the need to hardcode each file, every time you need to add one. the software will allow you to browse the file databse that it creates, will also install shared libraries on your machine that will be needed to develop a working system on this databes concept, you can develop pretty much anything on this file db.

as for now, this software is ment to be for **Linux/Unix** systems, and it has been developed on a Ubuntu 22.04.4 LTS Jammy Jellifish using gcc version 11.4.0, tested on:

- **Centos Stream Release 9** kernel: 5.14.0-479.el9.aarch64.
- **Fedora 36** kernel: 5.11.17-300.fc34.aarch64.

## Get Started

clone this repo:
paste this in your terminal

```plaintext
git clone https://github.com/LorenzoPiombini/isam.db-C-language.git
```

then

```plain text
cd isam.db-C-language/
sudo make build
```

now you have all library installed and you can use the isam.db program, or use the commands GET, LIST, FILE, DEL, DELa and KEYS to create files.

if you want to delate the commands and the isab.db program just run

```plaintext
sudo make clean 
```

inside the repo folder.

## How It Works

you can dynamically create **Files** at run time, you will provide either  the field(s)(variable(s)) names with the type and value, or simply the name of the fileds and the type.

### Example Usage

Here's how you can define a file:

```bash
[isam.db-C-language-main]$ isam.db -nf person -R name:TYPE_STRING:"last name":TYPE_STRING:age:TYPE_BYTE
```

```bash
[isam.db-C-language-main]$ bin/isam.db -nf person -R name:t_s:lastname:t_s:age:t_b
```

```bash
[isam.db-C-language-main]$ FILE person name:t_s:lastname:t_s:age:t_b
```

this three commands are correct and each will create a file with the follwing variables:

- name.
- lastname or last name.
- age.

if you want to writes field with spaces you have to put the "" around the field name, or you can do the following(last_name underscore without ""):

```bash
[isam.db-C-language-main]$ bin/isam.db -nf person -a name:TYPE_STRING:last_name:TYPE_STRING:age:TYPE_BYTE
```

the program will replace the underscore in last_name with a space (' ').
you can't use colon (":") in the fields names.

now we have an empty file with a definiton, and we can write data to it.

you can also create files and provide values to the fields in one entry:

```plaintext

[isam.db-C-language-main]$ bin/isam.db -nf item -a code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0 -k ui7

```

This command sets up three fields:

- code: a string with the value "man78-g-hus".

- price: a float with the value 33.56.

- discount: a float with the value 0.0.

the flag -k provides an id for the record that you are adding or creating, you will use this key for CRUD operation, once the record is created you will not able to use that key for new records in the same file.

if you do not specify a number of indexes with option **-i** the index file will have 5 indexes by default, index 0 will contain all the keys for the data file, and the other indexes you can use it as you please.
the indexes are zeored number, meaning if you have five indexes they will be 0, 1, 2, 3, 4.

this base definition cannot change anymore, you can add to it (up to 200 fields for each file), but as for now you cannot change a definition, that means that the field *code*, in our example, will always be and must be a string; new versions will have more flexibility, and will allow the users to change fields type.

now you have a file called item.dat and a file named item.inx which will be the index file (an Hash table).
to retrive the data you can run the follwing:

```plain text
[isam.db-C-language-main]$ GET item ui7
```

this comand will display on the the terminal:

```plain text
#################################################################

the Record data are: 
code    man78-g-hus
price    33.56
discount  0.00

#################################################################

```

the flag that you can provide are:

```plaintext
-a - add record to a file.
-n - create a new database file
-f - [required] path to file (file name)
-c - creates the files specified in the txt file.
-D - specify the index where you want to delete the record.
-R - define a file definition witout values.
-k - specify the record id, the program will perform CRUD ops based on this id.
-t - list of available types. this flag will exit the program.
-l - list the file definition specified with -f.
-u - update the file specified by -f .
-e - delete the file specified by -f .
-x - list the keys value for the file specified by -f .
-b - specify the file name (txt,csv,tab delimited file) to build from .
-o - add options to a CRUD operation .
-s - specify how many buckets the HashTable (index) will have.
-i - specify how many indexes the file will have.
 

```

## Index File structure

the index file is organized in this way:

- number of indexes (int)
- position of index 0 (off_t)
- position of index 1 (off_t)
- position of index 2 (off_t)
- position of index 3 (off_t)
- position of index 4 (off_t)
- index 0 size (int)
- index 0 (HashTable)
- index 1 size (int)
- index 1 (HashTable)
- index 2 size (int)
- index 2 (HashTable)
- index 3 size (int)
- index 3 (HashTable)
- index 4 size (int)
- index 4 (HashTable)

this is the layout of the index file, you can choose the number of indexes, but if you don't specify
a value, the standard number of indexes will be set to 5.
