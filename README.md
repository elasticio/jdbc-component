[![CircleCI](https://circleci.com/gh/elasticio/jdbc-component.svg?style=svg)](https://circleci.com/gh/elasticio/jdbc-component)
# JDBC Component

## Table of Contents

* [General Information](#general-information)
   * [Description](#description)
   * [Completeness Matrix](#completeness-matrix)
* [Credentials](#credentials)
* [Triggers](#triggers)
   * [Get Rows Polling Trigger](#get-rows-polling-trigger)
   * [Select Trigger](#select-trigger)
* [Actions](#actions)
   * [Delete Row By Primary Key](#delete-row-by-primary-key)
   * [Execute Custom Query](#execute-custom-query)
   * [Execute Stored Procedure](#execute-stored-procedure)
   * [Insert Action](#insert-action)
   * [Lookup Row By Primary Key](#lookup-row-by-primary-key)
   * [Select Action](#select-action)
   * [Upsert Row By Primary Key](#upsert-row-by-primary-key)
* [Known Limitations](#known-limitations)

---

## General Information

### Description
This is an open-source component designed for interacting with object-relational database management systems (ORDBMS) on the [elastic.io platform](http://www.elastic.io).

### Completeness Matrix
![JDBC Component Completeness Matrix](https://user-images.githubusercontent.com/22715422/67289390-38dad900-f4e7-11e9-9a45-1c7775c9c7d5.png)

[View the full JDBC Component Completeness Matrix here](https://docs.google.com/spreadsheets/d/1sZr9ydJbMK8v-TguctmFDiqgjRKcrpbdj4CeFuZEkQU/edit?usp=sharing).

## Credentials
The following properties are required to configure database credentials:

*   **DB Engine**: Select the database type (MySQL, PostgreSQL, Oracle, or MSSQL).
*   **Connection URI**: The hostname or IP address of the database server (e.g., `acme.com`).
*   **Port**: (Optional) The port number for the server instance. Defaults are:
    *   `3306`: MySQL
    *   `5432`: PostgreSQL
    *   `1521`: Oracle
    *   `1433`: MSSQL
*   **Database Name**: The name of the specific database to interact with.
*   **User**: The username with the necessary permissions.
*   **Password**: The password for the specified user.
*   **Additional configuration properties**: (Optional) Additional connection strings, such as `useUnicode=true&serverTimezone=UTC`.

> [!WARNING]
> Configuration properties may not be validated during the initial "Verify Credentials" step. Ensure all input is accurate to avoid runtime errors.

## Triggers

### Get Rows Polling Trigger
Executes an operation that polls multiple rows from the database since the last record.

The `%%EIO_LAST_POLL%%` placeholder functions similarly to the Select Trigger, tracking the last processed record to ensure only new data is retrieved.

**Initial Execution:**
If no snapshot exists and the `Start Polling From` field is empty, the trigger defaults to the **Unix Epoch** (1970-01-01 00:00:00.000).

> [!NOTE]
> Component snapshots are not overwritten in Real-Time flows due to platform behavior. We strongly recommend using the Get Rows Polling trigger in **Ordinary (Keen) Flows** only.

#### Input Fields
*   **Tables List**: A dropdown of available table names.
*   **Timestamp Column**: A dropdown of columns with `java.sql.Date` or `java.sql.Timestamp` types.
*   **Start Polling From**: (Optional) Manually set the beginning time for polling. Defaults to the Unix Epoch (1970-01-01).

### Select Trigger
Executes a custom SELECT statement for incremental polling.

Before execution, the `%%EIO_LAST_POLL%%` placeholder is replaced with either the ISO date of the last successful execution or the maximum value from the last polled dataset (e.g., `2018-08-01T00:00:00.000`).

**Initial Execution:**
During the first execution (when no snapshot exists), the placeholder defaults to **midnight of the current day** (Today at 00:00:00.000).

*   **Precision**: Polling supports precision up to milliseconds.
*   **Start Polling From**: (Optional) You can manually override the default by providing a value in the format: `yyyy-mm-dd hh:mi:ss[.sss]`.

---

### SELECT Trigger (Deprecated)
Maintained for backward compatibility. We recommend migrating to the modern [Select Trigger](#select-trigger).

## Actions

### Delete Row By Primary Key
Removes a single row from the database using its primary key. Returns the count of affected rows.

### Execute Custom Query
Executes the provided SQL query exactly as provided.
> [!IMPORTANT]
> SQL requests are executed according to the chosen JDBC driver's specification. Multiple statements are executed within a single transaction; if any statement fails, the entire transaction is rolled back.

#### Input Fields
*   **query**: The SQL string to execute.

### Execute Stored Procedure
Executes the selected stored procedure from the specified database schema. Metadata is generated automatically based on `IN`, `OUT`, and `IN OUT` parameters.

*   **Supported Array Types**: `CURSOR` (SQL) and `REF CURSOR` (Oracle). Results are returned as JSON objects.
*   **MSSQL Note**: `@RETURN_VALUE` is currently not processed.

### Insert Action
Inserts a new row into the specified table. Fields with `auto-increment` or `auto-calculated` properties are excluded from the metadata.

### Lookup Row By Primary Key
Fetches a single row from the database using its primary key.

*   **Do not throw error on empty result**: If enabled, emits an empty object instead of an error when no row is found.

### Select Action
Executes a SELECT statement to fetch multiple rows. 

To prevent SQL injection, this action uses **Prepared Statements**. You must explicitly define the type for each variable using the format `@variable_name:type`.

**Example Query:**
```sql
SELECT * FROM users WHERE userid = @id:number AND language = @lang:string
```

**Supported Types:**
*   `string`, `number`, `bigint`, `boolean`, `float`, `date`.

**Emit Behavior:**
*   **Fetch All**: Emits a single message containing an array of all result rows.
*   **Emit Individually**: Emits one message per row.
*   **Expect Single**: Emits one message for one row. Throws an error if multiple rows are found.

### Upsert Row By Primary Key
Updates an existing row or inserts a new one based on the primary key. All non-nullable fields should be provided to ensure success.

---

## Known Limitations
1.  **Primary Keys**: Only tables with a single, non-composite Primary Key are supported.
2.  **Database Versions**: 
    *   **MySQL**: 5.5, 5.6, 5.7, 8.0.
    *   **PostgreSQL**: 8.2 and higher.
    *   **Oracle**: 8.1.7 through 21.3.0.
    *   **MSSQL**: 2008 R2 and higher.
3.  **Upsert Action**: Non-nullable fields are not automatically marked as required in dynamic metadata.
4.  **Stored Procedures**: ResultSet outputs for MSSQL and general array type parameters are not supported.
5.  **Rebound Mechanism**: Supported for specific SQL states:
    *   MySQL: `40001`, `XA102`
    *   Oracle: `61000`
    *   MSSQL: `40001`
    *   PostgreSQL: `40P01`
6.  **Timezones**: JDBC drivers may convert time based on the database server's timezone configuration. Verify offsets (e.g., UTC vs local time) when inserting datetime values.

## License
Apache-2.0 © [elastic.io GmbH](https://www.elastic.io)

