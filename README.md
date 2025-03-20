# DataManager

A set of utility classes to handle Data Access to JDBC based databases. 
It uses ApiObject to handle data to and from the database.

# DataManager Configuration

DataManager uses [ConfigProvider](https://github.com/kscarr73/ConfigProvider_jre21) to locate the following entries:

- **DB_DRIVER**: *OPTIONAL* The Driver to use for the connection
- **DB_HOST**: *REQUIRED* The Host for the Database
- **DB_NAME**: *REQUIRED* The Database Name for the connection
- **DB_USER**: *OPTIONAL* The UserName to use for the database connection
- **DB_PASSWORD**: *OPTIONAL* The password for the database connection
- **DB_TESTQUERY**: *OPTIONAL* Query to use to verify connection valid.  Only needed for older drivers
- **DB_MAXCONNECTIONS**: *OPTIONAL* Max Connections to use for the Connection Pool.  DEFAULT: 10

## DataManager Default Database

```properties
DB_HOST=jdbc:mariadb://localhost
DB_NAME=TestDb
DB_USER=nobody
DB_PASSWORD=password
```

## DataManager Multiple Databases

You can setup multiple database connection pools by adding `_{Name}` to the configuration entries.

```properties
DB_HOST_TEST=jdbc:mariadb://localhost
DB_NAME_TEST=TestDb2
DB_USER_TEST=nobody
DB_PASSWORD_TEST=password
```

This would setup a database connection pool called `TEST`.

# Example DataManager Usage

```java
private static DataManager db = DataManager.getInstance();

public ApiObject searchTable() {
    ApiObject searchObj = new ApiObject();

    searchObj.setInteger("id", 2);

    return db.getTable(DataManager.DEFAULT, "test_table", searchObj);
}
```
