#This is just a sample code, its not a complete code. This is just to understand the flow of handling a complex json achema for creating avro files.
It’s a utility to generate .Avro binary files from pipe-delimited plain text files in JSON schema. The plain text files contain records with 1: n segments. Each segment has 1: n number of fields. For ex. a record can have Name, Address, etc. segments and Name segment can have fields as first name, last name, title, etc.
Challenges: 1. A Plain-text CSV file can contain different types of records for ex. Trade, legal, bankruptcy, collection etc.
2. Complex JSON schema for each type of records (nested items in the JSON schema which
could be an array whereas each item will have multiple fields)
5. Dynamic nature of CSV files i.e. adding new fields or removing fields from any of the
segments.
4. Language used: First, we decided to use scala to code this utility, but since our core
application is written in C++ and could have been flexible to reuse the existing functionalities
of the application, we decided to proceed with C++ which was a challenge for me. In C++,
JSON schema is translated into C++ structure i.e. nested JSON schema will result in nested
structure having its own fields as well as vector of other structure. So, every time we modify a
field name or add/remove a field or item in schema, we need to make code changes as structure
elements are accessed by its name. For ex. changing “ID” field name to “Id”, and if this field is
in all items and record schemas, it would require at least 30 minutes to make the code changes
at all the places. More the number of fields, more time it would require to complete the changes.
Solution: To make the changes dynamically to some extent, I thought of writing a code to create the
updated JSON schemas and generate C++ code for getter and setter methods of each item and
its corresponding fields. I stored all the items and its corresponding fields in database and wrote
a Perl script to generate .json schema files and C++ .h files (having getter and setter methods
of fields) for each item in the schema. Once schemas are ready, run the avrogencpp utility
(provided in C++ - AVRO API) on each JSON schema to generate corresponding CPP header
files for each schema, then compile the whole code. Here, no resource needed to make code
changes manually. Every time a request comes up with the modifications in fields, we update
the database entries and run this Perl script to generate updated JSON schema and CPP code
for those modifications, and just re-compile the code.
This approach provided us the flexibility to carry out changes in the schema with minimal
efforts and ability not only to complete 30 minutes work in 5 minutes but also to tackle the
scenarios where multiple changes would have taken hours to complete could be finished in
much less time.
Instead of writing repetitive code for common blocks such as Name, Address, DOB, SSN, etc.
in all types of record, I used template for such common items while generating as well as
reading Avro files.
