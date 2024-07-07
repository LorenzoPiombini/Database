# ISAM Database in C

This ISAM database program enables dynamic creation of file definitions at runtime,  without the need to hardcode each type.

## Features

- **Dynamic File Definition**: Easily define file fields dynamically at runtime by specifying field names, types, and initial values.
- **Supports Multiple Data Types**: Supports a variety of data types including integers, longs, floats, strings, bytes, and doubles.

## How It Works

Users can dynamically specify the attributes of a business entity by defining each field's name, type, and initial value using a simple format.

### Example Usage

Here's how you can define a file dinamically:

```plaintext
code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0

```

This configuration sets up three fields:

code: A string (char*) with the value "man78-g-hus".

price: A float with the value 33.56.

discount: A float with the value 0.0.

to put it all together this will create a new file:

```plaintext
bin/isam.db -nf item -d code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0 -k jj6
```

note the flag -k, this provide an id for the record that you are adding or creating, you will use this key for CRUD operation.

## IMPORTANT

as for now you have to supply the key manually, it would make more sense if the program took care of the key authomatically, I am working on this.
___________________________________________________________________________________

now the  file is created, and the base definition would be made by 3 variables code, price and discount. 

this base definition cannot change anymore, you can add to it (up to 200 fields foor each file), but everytime you add a record to a file, you need to provide at least the correct definition. if you try to do the follwing ( notes the different flag -a for adding to an existing file):

```plain text
bin/isam.db -f item -a code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_STRING:0.0 -k jj7
```

this will fail, the program check the new input against the header, thus, it  knows that discount is supposed to be a float, the message will be:

```plain text
Schema is not equal to file definition.
Files Closed successfully!
```

now you have a file called item.dat and a file named item.inx which will be the index file (an Hash table).
to retrive the data you run the follwing:

```plain text
bin/isam.db -f item -r jj6
```

this will display the data to the stdout, if found.

## STILL DEVOLOPING

I am making this project to better learn C, this is still a work in porgress, and help is very much appreciated. THANKS!
