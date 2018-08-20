# DavisBase
This project is to implement a rudimentary database engine that is loosely based on a hybrid between MySQL and SQLite.

This implementation operates entirely from the command line and API calls.

STEPS TO RUN THE CODE:

1. Extract the archive to a local folder
2. Open eclipse and load the project from the extracted folder
3. Build and run the code from eclipse

SYNTAX SUPPORTED:

• HELP; - Displays Supported command

• VERSION; - Displays Version

• SHOW TABLES; – Displays a list of all tables in DavisBase.

• CREATE TABLE table_name (ROW_ID INT PRIMARY KEY, <column_name2> <datatype> [NOT NULL | ], <column_name3> <datatype> [NOT NULL | ],...); – Creates a new table schema, i.e. a new empty table.

• DROP TABLE table_name; – Remove a table schema, and all of its contained data.

• The database catalog (i.e. meta-data) shall be stored in two special tables that exist by default: davisbase_tables and davisbase_columns

• INSERT INTO table_name [column_list] VALUES [value_list]; - Inserts a single record into a table - do not include row_id and its value. Its auto incremented.

• DELETE FROM table_name WHERE ROW_ID = [row_id_value]; - Deletes records from a a table for the give row_id_value

• UPDATE table_name SET column_name = [column_value] WHERE ROW_ID = [row_id_value]; - Modifies record in a table with respect to row_id_value

• SELECT * FROM table_name; - Display contents of the table

• SELECT * FROM table_name WHERE ROW_ID = [row_id_value]; - Display contents of the table for the given row_id

• EXIT; – Cleanly exits the program and saves all table information in non-volatile files to disk Page
 

SUPPORTED COMMANDS EXAMPLES:

help; -- To show supporting command

create table table1 (row_id int primary key, name text not null, number smallint, dob date); -- To create a table

show tables; -- To display list of valid  existing tables

insert into table1 (name, number, dob) values ("john", 101, "1994-05-02"); -- To insert values into a table

select * from table; -- display all contents of table

select * from table1 where row_id = 1; -- display contents of table filtered by row_id

update table1 set number = 201 where row_id = 1; -- update contents of the table with respective to a row

delete from table1 where row_id = 1; delete a row from table

drop table table1; deleting the entire table

version; -- to display version

exit; -- exiting 

ASSUMPTIONS MADE:
• row_id as INT PRIMARY KEY is always required while creating a table
• while inserting row_id and its values should not be inserted as it is being auto incremented from the code
• insert the column name and its values in the order of creation
• spaces between the words in the commands should be followed according to the syntax and examples provided above
• Each page size is 512 Bytes. 
• B PLus Tree with n children nodes is implemented for files created for storing table data. Degree is n as it keeps incresing with the data. 
  This approch is chosen as degree required for B PLus Tree is not mentioned in the requirements.

FILES ARE STORED LIKE BELOW:

/data
|
+-/catalog
| |
| +-/davisbase_tables.tbl
| +-/davisbase_columns.tbl
|
+-/user_data
|
+-/table_name_1.tbl
+-/table_name_2.tbl
+-/table_name_3.tbl

KNOWN BUGS:
We cant create another table with the same name as previously dropped table as the dropped table file still exists in the user_data folder and code to move it to deleted folder has 
not been implemented

Executing multiple commands at a time might miss execution of few


