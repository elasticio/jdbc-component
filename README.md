# JDBC-component
## Description
This is an open source component for working with object-relational database management systems on [elastic.io platform](http://www.elastic.io "elastic.io platform").
### Purpose
With this component you will have following triggers:

``SELECT`` - this trigger will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that returns multiple results, it has limitations on the query and suited only for SELECT type of queries. The trigger will remember last execution timestamp and let you build queries on it.

``GET ROWS POLLING`` - this trigger will execute select query from specified table with simple criteria of selected datetime or timestamp table. The trigger will remember last execution timestamp and let you build queries on it.

Following actions are inside:

``SELECT`` - this action will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that returns multiple results, it has limitations on the query and suited only for SELECT type of queries.

``LOOKUP BY PRIMARY KEY`` - this action will execute select query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique). 

``UPSERT BY PRIMARY KEY`` - this action will execute select query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"), if result is nullable, then action will execute select query by [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"), else - action will execute update query by [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique).

``DELETE BY PRIMARY KEY`` - this action will execute delete query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns an integer value that indicates the number of rows affected, the returned value can be 0 or 1 (a primary key is unique).
### How works

### Requirements
Before you can deploy any code into elastic.io **you must be a registered elastic.io platform user**. Please see our home page at [http://www.elastic.io](http://www.elastic.io) to learn how. 
#### Environment variables
For unit-testing
#### Others
## Credentials
You may use following properties to configure a connection:
![image](https://user-images.githubusercontent.com/40201204/43577550-ce99efe6-9654-11e8-87ed-f3e0839d618a.png)
You can add the authorisation methods during the integration flow design or by going to your Settings > Security credentials > REST client and adding there.
### DB Engine
![image](https://user-images.githubusercontent.com/40201204/43577772-6f85bdea-9655-11e8-96e1-368493a36c9d.png)
You are able to choose one of existing database types
- ``MySQL`` - compatible with MySQL Server 5.5, 5.6, 5.7 and 8.0.
- ``PostgreSQL`` - compatible with PostgreSQL 8.2 and higher
- ``Oracle`` - compatible with Oracle Database 8.1.7 - 12.1.0.2
- ``MSSQL`` - compatible with Microsoft SQL Server 2008 R2 and higher
### Connection URI
In the Connection URI field please provide hostname of the server, e.g. ``acme.com``
### Connection port
In the Connection port field please provide port of the server instance, as by default:
- ``3306`` - MySQL
- ``5432`` - PostgreSQL
- ``1521`` - Oracle
- ``1433`` - MSSQL
### Database Name
In the Database Name field please provide name of database at the instance that you want to interact with.
### User
In the User field please provide a username that has permissions to interact with the Database.
### Password
In the Password field please provide a password of the user that has permissions to interact with the Database.

Validation will start right after click on a Save button. You will be able to continue working with component after validation if all provided credentials will be valid.
## Triggers
### SELECT
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
### GET ROWS POLLING
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
## Actions
### SELECT
![image](https://user-images.githubusercontent.com/40201204/43592439-39ec5738-967e-11e8-8632-3655b08982d3.png)
The action will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that can return multiple results, it has limitations on the query and suited only for SELECT type of queries.
In SQL query you can use clause variables with specific data types. 
Internally we use prepared statements, so all incoming data is
validated against SQL injection, however we had to build a connection from JavaScript types to the SQL data types
therefore when doing a prepared statements, you would need to add ``:type`` to **each prepared statement variable**.

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

Following types are supported:
 * ``string``
 * ``number``
 * ``bigint``
 * ``boolean``
 * ``float``
 * ``date``

![image](https://user-images.githubusercontent.com/40201204/43644974-332f2aa4-9739-11e8-8483-f7395e5d195d.png)

Checkbox ``Don't throw Error on an Empty Result`` allows to emit an empty response, otherwise you will get an error on empty response.
 
#### Input fields description
Component supports dynamic incoming metadata - as soon as your query is in place it will be parsed and incoming metadata will be generated accordingly.

### LOOKUP BY PRIMARY KEY
![image](https://user-images.githubusercontent.com/40201204/43592505-5b6bbfe8-967e-11e8-845e-2ce8ac707357.png)
The action will execute select query from a ``Table`` dropdown field, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique).
Checkbox ``Don't throw Error on an Empty Result`` allows to emit an empty response, otherwise you will get an error on empty response.
#### Input fields description
![image](https://user-images.githubusercontent.com/40201204/43644579-f593d1c8-9737-11e8-9b97-ee9e575a19f7.png)
As an input metadata you will get a Primary Key field to provide the data inside as a clause value.

### DELETE BY PRIMARY KEY
![image](https://user-images.githubusercontent.com/40201204/43592505-5b6bbfe8-967e-11e8-845e-2ce8ac707357.png)
The action will execute delete query from a ``Table`` dropdown field, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns count of affected rows.
Checkbox ``Don't throw Error on an Empty Result`` allows to emit an empty response, otherwise you will get an error on empty response.
#### Input fields description
![image](https://user-images.githubusercontent.com/40201204/43644579-f593d1c8-9737-11e8-9b97-ee9e575a19f7.png)
As an input metadata you will get a Primary Key field to provide the data inside as a clause value.

## Known issues
No known issues are there yet.

## License
Apache-2.0 © [elastic.io GmbH](https://www.elastic.io "elastic.io GmbH")

## <System> API and Documentation links
[elastic.io iPaaS Documentation](https://support.elastic.io/support/home "elastic.io iPaaS Documentation")
