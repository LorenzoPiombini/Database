# ISAM Database in C

This ISAM database program enables dynamic creation of file definitions at runtime, making it ideal for applications that require flexible data structure configurations without the need to hardcode each type.

## Features

- **Dynamic File Definition**: Easily define file fields dynamically at runtime by specifying field names, types, and initial values.
- **Supports Multiple Data Types**: Supports a variety of data types including integers, longs, floats, strings, bytes, and doubles.

## How It Works

Users can dynamically specify the attributes of a business entity by defining each field's name, type, and initial value using a simple format. This flexibility is crucial for managing data structures that may change or need to be customized.

### Example Usage

Here's how you can define a file dinamically:

```plaintext
code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0


This configuration sets up three fields:

code: A string (char*) with the value "man78-g-hus".
price: A float with the value 33.56.
discount: A float with the value 0.0.