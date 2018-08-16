REM MSSQL CONFIGURATION BLOCK

SETX CONN_URI_MSSQL "jdbc:sqlserver://eio-mssql-fra.c79g081qpeyv.eu-central-1.rds.amazonaws.com:1433;database "Test2"
SETX CONN_USER_MSSQL "john"
SETX CONN_PASSWORD_MSSQL "elastic123"
SETX CONN_DBNAME_MSSQL "Test2"
SETX CONN_HOST_MSSQL "eio-mssql-fra.c79g081qpeyv.eu-central-1.rds.amazonaws.com"


REM MYSQL CONFIGURATION BLOCK

SETX CONN_URI_MYSQL "jdbc:mysql://ec2-18-194-228-22.eu-central-1.compute.amazonaws.com:3306/elasticio_testdb"
SETX CONN_USER_MYSQL "elasticio"
SETX CONN_PASSWORD_MYSQL "lo4hDacS5L"
SETX CONN_DBNAME_MYSQL "elasticio_testdb"
SETX CONN_HOST_MYSQL "ec2-18-194-228-22.eu-central-1.compute.amazonaws.com"


REM ORACLE CONFIGURATION BLOCK

SETX CONN_URI_ORACLE "jdbc:oracle:thin:@//ec2-18-194-228-22.eu-central-1.compute.amazonaws.com:1521/XE"
SETX CONN_USER_ORACLE "elasticio"
SETX CONN_PASSWORD_ORACLE "PeU13cbKtH"
SETX CONN_DBNAME_ORACLE "elasticio_testdb"
SETX CONN_HOST_ORACLE "ec2-18-194-228-22.eu-central-1.compute.amazonaws.com"


REM POSTGRESQL CONFIGURATION BLOCK

SETX CONN_URI_POSTGRESQL "jdbc:postgresql://ec2-18-194-228-22.eu-central-1.compute.amazonaws.com:5432/elasticio_testdb"
SETX CONN_USER_POSTGRESQL "elasticio"
SETX CONN_PASSWORD_POSTGRESQL "2uDyG4hHxR"
SETX CONN_DBNAME_POSTGRESQL "elasticio_testdb"
SETX CONN_HOST_POSTGRESQL "ec2-18-194-228-22.eu-central-1.compute.amazonaws.com"