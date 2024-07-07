# ISAM Database in C

This ISAM database program enables dynamic creation of file definitions at runtime, making it ideal for applications that require flexible data structure configurations without the need to hardcode each type.

## Features

- **Dynamic File Definition**: Easily define file fields dynamically at runtime by specifying field names, types, and initial values.
- **Supports Multiple Data Types**: Supports a variety of data types including integers, longs, floats, strings, bytes, and doubles.

## How It Works

Users can dynamically specify the attributes of a business entity by defining each field's name, type, and initial value using a simple format. This flexibility is crucial for managing data structures that may change or need to be customized.

## How It Works

Users can define the attributes of a business entity by specifying each field's name, type, and initial value. This is particularly advantageous in scenarios where the structure of data may vary or evolve over time.

### Example Usage

Here's how you can define a data structure dynamically:

```plaintext
code:TYPE_STRING:"man78-g-hus":price:TYPE_FLOAT:33.56:discount:TYPE_FLOAT:0.0