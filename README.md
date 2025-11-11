[![CircleCI](https://circleci.com/gh/elasticio/jdbc-component.svg?style=svg)](https://circleci.com/gh/elasticio/jdbc-component)
# JDBC-component
## Table of Contents

* [General information](#general-information)
   * [Description](#description)
   * [Completeness Matrix](#completeness-matrix)
* [Credentials](#credentials)
* [Triggers](#triggers)
   * [Select trigger](#select-trigger)
   * [Get Rows Polling trigger](#get-rows-polling-trigger)
* [Actions](#actions)
   * [Execute custom query](#execute-custom-query)
   * [Select action](#select-action)
   * [Lookup Row By Primary Key](#lookup-row-by-primary-key)
   * [Insert action](#insert-action)
   * [Delete Row By Primary Key](#delete-row-by-primary-key)
   * [Execute stored procedure](#execute-stored-procedure)
   * [Upsert Row By Primary Key)](#upsert-row-by-primary-key)
* [Known Limitations](#known-limitations)

Execute stored procedure

## General information
### Description
This is an open source component for working with object-relational database management systems on [elastic.io platform](http://www.elastic.io "elastic.io platform").

### Completeness Matrix
![JDBC Component Completeness Matrix](https://user-images.githubusercontent.com/22715422/67289390-38dad900-f4e7-11e9-9a45-1c7775c9c7d5.png)

[JDBC Component Completeness Matrix](https://docs.google.com/spreadsheets/d/1sZr9ydJbMK8v-TguctmFDiqgjRKcrpbdj4CeFuZEkQU/edit?usp=sharing)

## Credentials
You need to use following properties to configure credentials:

```DB Engine``` - Choose one of existing database types:
![image](https://user-images.githubusercontent.com/40201204/43577772-6f85bdea-9655-11e8-96e1-368493a36c9d.png)

```Connection URI``` - Provide hostname of the server, e.g. ``acme.com``

```Connection port``` - Optional field. Provide port of the server instance, as by default:
- ``3306`` - MySQL
- ``5432`` - PostgreSQL
- ``1521`` - Oracle
- ``1433`` - MSSQL

```Database Name``` - Provide name of database at the instance that you want to interact with.

```User``` - Provide a username that has permissions to interact with the Database.

```Password``` - Provide a password of the user that has permissions to interact with the Database.

```Configuration properties``` - Optional field. Provide a configuration properties for connections to the Database, e.g. ``useUnicode=true&serverTimezone=UTC``

**Limitation:** `Configuration properties` value may not be checked during Credentials Verifacation, so in case of using this field make sure that it contains correct input. 

## Triggers
### Select trigger
You are able to provide SELECT query with last execution timestamp as WHERE clause criteria.
![image](https://user-images.githubusercontent.com/40201204/43591075-2a032dcc-967b-11e8-968d-851355c2646e.png)
Before executing the the statement %%EIO_LAST_POLL%% will be replaced with ISO Date of the last execution or max value of the last pooled datetime, for example ``2018-08-01T00:00:00.000``.
During the first execution, date will be equal to ["start" of Unix Time](https://en.wikipedia.org/wiki/Unix_time) - ``1970-01-01 00:00:00.000``.
Precision of the polling clause can be till milliseconds.
The format of ``Start Polling From (optional)`` field should be like ``yyyy-mm-dd hh:mi:ss[.sss]``, where
- ``yyyy`` - year
- ``mm`` - month
- ``dd`` - day
- ``hh`` - hour
- ``mi`` - minute
- ``ss`` - second
- ``sss`` - millisecond (optional)
- 
### Get Rows Polling trigger
This trigger can polling data from provided table. As WHERE clause you can use column, which has datatype like DATE or TIMESTAMP.
![image](https://user-images.githubusercontent.com/40201204/43591332-c99f6b3e-967b-11e8-8a77-bf8386e83d51.png)
Before executing the the statement %%EIO_LAST_POLL%% will be replaced with ISO Date of the last execution or max value of the last pooled datetime, for example ``2018-08-01T00:00:00.000``.
During the first execution, date will be equal to ["start" of Unix Time](https://en.wikipedia.org/wiki/Unix_time) - ``1970-01-01 00:00:00.000``.
Precision of the polling clause can be till milliseconds.
The format of ``Start Polling From (optional)`` field should be like ``yyyy-mm-dd hh:mi:ss[.sss]``, where
- ``yyyy`` - year
- ``mm`` - month
- ``dd`` - day
- ``hh`` - hour
- ``mi`` - minute
- ``ss`` - second
- ``sss`` - millisecond (optional)

*Please Note: Component Snapshot will not be overwritten in Real-Time flows due to platform behaviour, so we strongly recommend to use Get Rows Polling trigger in Keen Flows only*

#### Input fields description
![image](https://user-images.githubusercontent.com/16806832/67293348-f5836900-f4ec-11e9-8e6a-e91b9417ff9d.png)

##### Tables List

Dropdown list with available table names, required field

##### Timestamp (or similar) Column

Dropdown list with available Column names, that have a type like `java.sql.Date` or `java.sql.Timestamp`, required field

##### Start Polling From (optional)

Optional field, indicates the beginning time to start polling from (defaults to the current time)


### SELECT trigger (Deprecated)
This action exists in JDBC component only for backward compatibility. New [**Select trigger**](#select-trigger) is recommended to use.

## Actions
### Execute custom query
Action to execute custom SQL query from provided request string.

**Note:** SQL request will be executed according to chosen database JDBC specification.

Execution result returns as array of objects. If request contains multiple sql statements - them will execute inside one transaction.
If one of statements fails, transaction will be rollbacked.

#### Input fields description

As input metadata, you will get one field named `query` to provide request string

#### Query Samples:

Select:
```sql
SELECT name, size FROM stars
```

Update: 
```sql
INSERT INTO stars values (1,'Taurus', '2015-02-19 10:10:10.0', 123, 5, 'true', '2015-02-19')
```

Posgresql batch multiple statements request:
```sql
DELETE FROM stars WHERE id = 1;
UPDATE stars SET radius = 5 WHERE id = 2;
```

### Select action
![image](https://user-images.githubusercontent.com/16806832/134408205-04b84670-c976-41e7-b805-faabff4ae1e5.png)

The action will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that can return multiple results, it has limitations on the query and suited only for SELECT type of queries.
In SQL query you can use clause variables with specific data types. 
Internally we use prepared statements, so all incoming data is
validated against SQL injection, however we had to build a connection from JavaScript types to the SQL data types
therefore when doing a prepared statements, you would need to add ``:type`` to **each prepared statement variable**.

**Note:** prepared statement variables name could contain: any characters between a-z or A-Z, a digit and a character `_` (`[a-zA-Z0-9_]`). 

For example if you have a following SQL statement:

```sql
SELECT
FROM users
WHERE userid = @id AND language = @lang
```

you should add ``:type`` to each ``@parameter`` so your SQL query will looks like this:

```sql
SELECT
FROM users
WHERE userid = @id:number AND language = @lang:string
```

You can use aliases too:
```sql
SELECT users.id AS internal_id
FROM internal_users users
```

Following types are supported:
 * ``string``
 * ``number``
 * ``bigint``
 * ``boolean``
 * ``float``
 * ``date``

![image](https://user-images.githubusercontent.com/16806832/134408591-b9faa51c-3b35-4cf2-992d-51dcd07c5cb5.png)

Dropdown **Emit Behaviour** contains following possible options:
 * Fetch all - a single message with an array `results` containing all the objects (rows) will be emitted
 * Emit Individually - multiple messages (one message per one row) will be emitted
 * Expect Single - a single message with one result row will be emitted. If more than one row is returned the error will be thrown. A boolean input "Allow Zero Results" (defaults to `false`) appears at input metadata. If `false` - error will be thrown, else - the empty object will be emitted.

![image](https://user-images.githubusercontent.com/16806832/134408977-d4692d3f-e9fb-48be-9104-c4cb121accaa.png)
 
#### Input fields description
Component supports dynamic incoming metadata - as soon as your query is in place it will be parsed and incoming metadata will be generated accordingly.

### Lookup Row By Primary Key
![image](https://user-images.githubusercontent.com/40201204/43592505-5b6bbfe8-967e-11e8-845e-2ce8ac707357.png)

The action will execute select query from a ``Table`` dropdown field, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique).
Checkbox ``Don't throw Error on an Empty Result`` allows to emit an empty response, otherwise you will get an error on empty response.
#### Input fields description
![image](https://user-images.githubusercontent.com/40201204/43644579-f593d1c8-9737-11e8-9b97-ee9e575a19f7.png)
As an input metadata you will get a Primary Key field to provide the data inside as a clause value.

### Insert action
The action will execute ``INSERT`` command into the table from ``Table`` dropdown list the values specified in the body.

#### List of Expected Config fields
   * `Enable Rebound` if `Yes` in case of deadlocks rebound message using Sailor rebound mechanism, number of rebound can be specified via environment variable: `ELASTICIO_REBOUND_LIMIT` recommended value 3
#### Input fields description
##### Table

Action contains only one configuration field `Table` - dropdown list with available table names.
![image](https://user-images.githubusercontent.com/16806832/65327293-3f122880-dbbc-11e9-8a07-a10131900962.png)

#### Expected input metadata

As input metadata, you will get all fields of the selected table except for fields with `auto-increment` or `auto-calculated` property. 

#### Expected output metadata

As output metadata, you will get execution insert result like:
```json
{
  "result": true
}
```

### Delete Row By Primary Key
![image](https://user-images.githubusercontent.com/40201204/43592505-5b6bbfe8-967e-11e8-845e-2ce8ac707357.png)
The action will execute delete query from a ``Table`` dropdown field, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns count of affected rows.
Checkbox ``Don't throw Error on an Empty Result`` allows to emit an empty response, otherwise you will get an error on empty response.
`Enable Rebound` if `Yes` in case of deadlocks rebound message using Sailor rebound mechanism, number of rebound can be specified via environment variable: `ELASTICIO_REBOUND_LIMIT` recommended value 3
#### Input fields description
![image](https://user-images.githubusercontent.com/40201204/43644579-f593d1c8-9737-11e8-9b97-ee9e575a19f7.png)
As an input metadata you will get a Primary Key field to provide the data inside as a clause value.

### Execute stored procedure
This action calls stored procedure from selected `DB Schema` and `Stored procedure` name
#### Input fields description
- **DB Schema** - a schema that contains a procedure to call. Must be selected from the dropdown list before `Stored procedure` name
- **Stored procedure** - a name of a procedure to call, can be selected from the dropdown list

Metadata generates automatically using `IN` & `IN OUT` procedure parameters for input, and `OUT` & `IN OUT` procedure parameters for output.

As array fields this action now support ONLY:
- CURSOR (as SQL type)
- REF CURSOR (as ORACLE type)
The result for this type of fields would be returned as an array of JSON objects.

This action DOES NOT processing MSSql @RETURN_VALUE.

- For MySQL component same to DATABASE is same to SCHEMA by it's 
[definition](https://dev.mysql.com/doc/refman/8.0/en/getting-information.html), so DB Schema dropdown is empty for MySQL. 

- [MSSQL DB](https://docs.microsoft.com/en-us/sql/t-sql/statements/create-procedure-transact-sql?view=sql-server-2017) stored procedures has only IN and INOUT fields.

#### Usage case example

For Oracle DB procedure:

```
create PROCEDURE "INSERT_EMPLOYEE"(
        i_emp_id IN EMPLOYEE.EMPID%TYPE,
        i_name IN EMPLOYEE.EMPNAME%TYPE,
        i_department IN EMPLOYEE.DEPARTMENT%TYPE)
IS
BEGIN
  INSERT INTO EMPLOYEE (EMPID, EMPNAME, DEPARTMENT)
  VALUES (i_emp_id, i_name, i_department);
END;
```

Component generates next metadata:

![image](https://user-images.githubusercontent.com/22715422/62056735-edd26200-b226-11e9-871e-0efc305d70b2.png)

### Upsert Row By Primary Key
The action will execute ``SELECT`` command from a ``Tables`` dropdown field, as search criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"), and execute ``INSERT`` command by PRIMARY KEY with specified field, if result does not found, else - action will execute ``UPDATE`` command by PRIMARY KEY with specified field. The action returns only one result row (a primary key is unique).
1. Find and select jdbc-component in the component repository
![image](https://user-images.githubusercontent.com/16806832/44981615-c70a9d80-af7b-11e8-8055-3b553abe8212.png)

2. Create new or select existing credentials
![image](https://user-images.githubusercontent.com/16806832/44981652-e86b8980-af7b-11e8-897e-04d1fc9a93cf.png)

3. Select action "Upsert Row By Primary Key" from list
![image](https://user-images.githubusercontent.com/16806832/44981700-0d5ffc80-af7c-11e8-9ac3-aedb16e1d788.png)

4. Select table from ``Table`` dropdown list
![image](https://user-images.githubusercontent.com/16806832/44981754-38e2e700-af7c-11e8-87d3-f029a7fec8fa.png)

5. Specify input data (field with red asterisk is Primary key), and click "Continue"
![image](https://user-images.githubusercontent.com/16806832/44981854-83fcfa00-af7c-11e8-9ef2-8c06e77fed1e.png)

6. Enable rebound mechanism if needed
![image](https://user-images.githubusercontent.com/18464641/67211608-b76e4280-f423-11e9-85f6-f7ec58cc24f1.png)

7. Retrieving sample
![image](https://user-images.githubusercontent.com/16806832/44983059-86f9e980-af80-11e8-8178-77e463488c7a.png)

8. Retrieve sample result
![image](https://user-images.githubusercontent.com/16806832/44982952-2ec2e780-af80-11e8-98b1-58c3adbc15b9.png)

9. Click "Continue"
![image](https://user-images.githubusercontent.com/16806832/44983101-b0b31080-af80-11e8-82d8-0e70e4b4ff97.png)

10. Finish component configuration
![image](https://user-images.githubusercontent.com/16806832/44983365-90378600-af81-11e8-9be4-4dbb39af0fdc.png)

#### Input fields description
* `Enable Rebound` if `Yes` in case of deadlocks rebound message using Sailor rebound mechanism, number of rebound can be specified via environment variable: `ELASTICIO_REBOUND_LIMIT` recommended value 3
As an input metadata you will get all fields of selected table. [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY") is required field (will mark as asterisk) and other input fields are optional.
![image](https://user-images.githubusercontent.com/16806832/44397461-1a76f780-a549-11e8-8247-9a6f9aa3f3b4.png)


### Create or update record (Deprecated)
This action exists in JDBC component only for backward compatibility. 
Please use [**Upsert row by primary key**](#upsert-row-by-primary-key) instead.

### Select (Deprecated)
This action exists in JDBC component only for backward compatibility.
Please use [**Select action**](#select-action) instead.

## Known limitations
1. Only tables with one [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY") is supported. You will see the message ``Table has not Primary Key. Should be one Primary Key
``, if the selected table doesn't have a primary key. Also, you will see the message ``Composite Primary Key is not supported
``, if the selected table has composite primary key.
2. Only following versions of database types are supported:
- ``MySQL`` - compatible with MySQL Server 5.5, 5.6, 5.7 and 8.0.
- ``PostgreSQL`` - compatible with PostgreSQL 8.2 and higher
- ``Oracle`` - compatible with Oracle Database 8.1.7 - 21.3.0
- ``MSSQL`` - compatible with Microsoft SQL Server 2008 R2 and higher
3. The current implementation of the action ``Upsert By Primary Key`` doesn't mark non-nullable fields as required fields at a dynamic metadata. In case of updating such fields with an empty value you will get SQL Exception ``Cannot insert the value NULL into...``. You should manually fill in all non-nullable fields with previous data, if you want to update part of columns in a row, even if data in that fields doesn't change. 
4. The current implementation of the action ``Execute stored procedure`` doesn't support ResultSet MSSQL output.
5. The current implementation of the action ``Execute stored procedure`` doesn't support any array types parameters.
(MySQL does not have schemas by definition)
6. Rebound mechanism only works for this SQL State: 
 - ``MySQL``: 40001, XA102
 - ``Oracle``: 61000
 - ``MSSQL``: 40001
 - ``PostgreSQL``:  40P01
 7. If your database server configured to custom timezone (differ from UTC) JDBC driver may convert time appropriate - for example if you want to use `Insert action` with `MySQL` which is configured to `+2:00` time zone and provide `2022-01-01 15:00:00` as value to some datetime field in database it will be saved as `2022-01-01 17:00:00`.

## License
Apache-2.0 © [elastic.io GmbH](https://www.elastic.io "elastic.io GmbH")
