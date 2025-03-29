package com.progbits.db.dataaccess;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.progbits.api.config.ConfigProvider;
import com.progbits.api.exception.ApiClassNotFoundException;
import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import com.progbits.api.parser.YamlObjectParser;
import com.progbits.api.utils.ApiResources;
import com.progbits.api.utils.service.ApiInstance;
import com.progbits.api.utils.service.ApiService;
import com.progbits.db.SsDbObjects;
import com.progbits.db.SsDbUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * DataManager implementation using HikariCP and ApiObjects
 *
 * @author kscarr73
 */
public class DataManager implements ApiService {

    private static final Logger log = LoggerFactory.getLogger(DataManager.class);

    private static final ApiInstance<DataManager> instance = new ApiInstance<>();
    
    public static DataManager getInstance() {
        return instance.getInstance(DataManager.class);
    }

    @Override
    public void configure() {
        pullConfigs();
        
        for (var entry : dbConfigs.keySet()) {
            ds.put(entry, setupPool(entry));
        }

        loadSqlEntries();
    }

    private void pullConfigs() {
        for (var propName : ConfigProvider.getInstance().getConfig().entrySet()) {
            if (propName.getKey().startsWith(DB_HOST)) {
                if (DB_HOST.equals(propName.getKey())) {
                    dbConfigs.setObject(DEFAULT, pullDbConfig(null));
                } else {
                    String keyName = propName.getKey().substring(8);

                    dbConfigs.setObject(keyName, pullDbConfig(keyName));
                }
            }
        }
    }

    private ApiObject pullDbConfig(String name) {
        ApiObject objRet = new ApiObject();

        String lclName;

        if (name == null) {
            lclName = "";
        } else {
            lclName = "_" + name;
        }

        objRet.setString(DB_HOST, config.getStringProperty(DB_HOST + lclName));
        objRet.setString(DB_DRIVER, config.getStringProperty(DB_DRIVER + lclName));
        objRet.setString(DB_NAME, config.getStringProperty(DB_NAME + lclName));
        objRet.setString(DB_USER, config.getStringProperty(DB_USER + lclName));
        objRet.setString(DB_PASSWORD, config.getStringProperty(DB_PASSWORD + lclName));
        objRet.setString(DB_TESTQUERY, config.getStringProperty(DB_TESTQUERY + lclName));
        objRet.setString(DB_MAXCONNECTIONS, config.getStringProperty(DB_MAXCONNECTIONS + lclName));

        return objRet;
    }

    private void loadSqlEntries() {
        try (InputStream is = apiResources.getResourceInputStream("db/sql.yaml")) {
            YamlObjectParser parser = new YamlObjectParser(true);

            sqlEntries = parser.parseSingle(new InputStreamReader(is));
        } catch (ApiException | ApiClassNotFoundException appx) {
            log.info("DB SQL Entries Read Failed");
        } catch (IOException iex) {
            log.info("DB SQL Entries Not Found");
        }
    }

    public static final String DEFAULT = "default";

    private static final String DB_HOST = "DB_HOST";
    private static final String DB_DRIVER = "DB_DRIVER";
    private static final String DB_NAME = "DB_NAME";
    private static final String DB_USER = "DB_USER";
    private static final String DB_PASSWORD = "DB_PASSWORD";
    private static final String DB_TESTQUERY = "DB_TESTQUERY";
    private static final String DB_MAXCONNECTIONS = "DB_MAXCONNECTIONS";

    private static ConfigProvider config = ConfigProvider.getInstance();
    private static ApiResources apiResources = ApiResources.getInstance();
    
    private Map<String, HikariDataSource> ds = new HashMap<>();

    private ApiObject sqlEntries = null;

    private ApiObject dbConfigs = new ApiObject();

    private HikariDataSource setupPool(String name) {
        ApiObject dbData = dbConfigs.getObject(name);

        HikariConfig dbconfig = new HikariConfig();

        if (!dbData.getString(DB_HOST).startsWith("jdbc:") || (dbData.getString(DB_DRIVER) != null && !dbData.getString(DB_DRIVER).isEmpty())) {
            dbconfig.setDriverClassName(dbData.getString(DB_DRIVER));
        }

        try {
            if (dbData.isSet(DB_DRIVER) && dbData.getString(DB_DRIVER).toLowerCase().contains("sqlserver")) {
                String strUrl = "jdbc:sqlserver://" + dbData.getString(DB_HOST) + ";databaseName=" + dbData.getString(DB_NAME) + ";user=" + dbData.getString(DB_USER) + ";";
                dbconfig.setJdbcUrl(strUrl);
                dbconfig.setPassword(dbData.getString(DB_PASSWORD));
            } else if (!dbData.getString(DB_HOST).startsWith("jdbc:")
                && dbData.isSet(DB_DRIVER)
                && dbData.getString(DB_DRIVER).toLowerCase().contains("jtds")) {
                dbconfig.setJdbcUrl("jdbc:jtds:sqlserver://" + dbData.getString(DB_HOST) + ";DatabaseName=" + dbData.getString(DB_NAME));
                dbconfig.setUsername(dbData.getString(DB_USER));
                dbconfig.setPassword(dbData.getString(DB_PASSWORD));
            } else {
                log.info("Db Settings: {} DbName: {} User: {}", dbData.getString(DB_HOST),
                    dbData.getString(DB_NAME), dbData.getString(DB_USER));

                if (dbData.getString(DB_HOST) != null && dbData.getString(DB_HOST).endsWith("/")) {
                    dbconfig.setJdbcUrl(dbData.getString(DB_HOST) + dbData.getString(DB_NAME));
                } else {
                    dbconfig.setJdbcUrl(dbData.getString(DB_HOST) + "/" + dbData.getString(DB_NAME));
                }

                dbconfig.setUsername(dbData.getString(DB_USER));
                dbconfig.setPassword(dbData.getString(DB_PASSWORD));
            }

            if (dbData.getString(DB_TESTQUERY) != null) {
                dbconfig.setConnectionTestQuery(dbData.getString(DB_TESTQUERY));
            }

            if (dbData.isSet(DB_MAXCONNECTIONS)) {
                dbconfig.setMaximumPoolSize(Integer.parseInt(dbData.getString(DB_MAXCONNECTIONS)));
            }

            return new HikariDataSource(dbconfig);
        } catch (Exception ex) {
            log.error("Database Connection Failed: " + ex.getMessage(), ex);
        }

        return null;
    }

    /**
     * Returns the status of DataManager, true if successfully configured and
     * connected
     *
     * @return true/false did DataManager start connections properly
     */
    public boolean getStatus() {
        boolean bRet = false;

        for (var entry : ds.entrySet()) {
            if (entry.getValue() != null) {
                bRet = entry.getValue().isRunning();
            } else {
                bRet = false;
            }

            if (!bRet) {
                break;
            }
        }

        return bRet;
    }

    /**
     * Return an entry from /resources/db/sql.yaml
     *
     * @param name
     * @return
     */
    public String getSqlEntry(String name) {
        if (sqlEntries != null) {
            return sqlEntries.getString(name);
        } else {
            return null;
        }
    }

    public ApiObject getConfig() {
        return dbConfigs;
    }

    public void setConfig(ApiObject objConfig) {
        dbConfigs = objConfig;
    }

    public HikariDataSource getDataSource() {
        return getDataSource(DEFAULT);
    }

    public HikariDataSource getDataSource(String name) {
        return ds.get(name);
    }

    public Connection getConnection() throws ApiException {
        return getConnection(DEFAULT);
    }

    public Connection getConnection(String name) throws ApiException {
        try {
            if (ds == null) {
                configure();
            }

            return ds.get(name).getConnection();
        } catch (SQLException sqx) {
            throw new ApiException(511, sqx.getMessage());
        }
    }

    public ApiObject getTable(String tableName, ApiObject searchObj) throws ApiException {
        return getTable(DEFAULT, tableName, searchObj);
    }

    public ApiObject getTable(String dbName, String tableName, ApiObject searchObj) throws ApiException {
        try (Connection conn = ds.get(dbName).getConnection()) {
            searchObj.setString("tableName", tableName);

            return SsDbObjects.find(conn, searchObj);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    public ApiObject saveIntegerKey(String dbName, String tableName, String id, ApiObject objSave) throws ApiException {
        try (Connection conn = ds.get(dbName).getConnection()) {
            return SsDbObjects.upsertWithIntegerKey(conn, tableName, id, objSave);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            if (ex.getMessage() != null) {
                if (ex.getMessage().contains("duplicate key")) {
                    throw new ApiException(400, "Duplicate Record", ex);
                } else {
                    throw new ApiException(400, ex.getMessage(), ex);
                }
            }
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    public ApiObject saveStringKey(String dbName, String tableName, String id, ApiObject objSave) throws ApiException {
        try (Connection conn = ds.get(dbName).getConnection()) {
            return SsDbObjects.upsertWithStringKey(conn, tableName, id, objSave);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    /**
     * Delete a Row from a Database
     *
     * @param dbName The configured Db name
     * @param tableName The table name to delete from
     * @param fieldName The field that is the ID
     * @param value The ID to delete
     * @return True if the SQL ran successfully. Not if the ID didn't exist, an
     * error is NOT thrown.
     *
     * @throws ApiException
     */
    public boolean deleteId(String dbName, String tableName, String fieldName, Object value) throws ApiException {
        try (Connection conn = ds.get(dbName).getConnection()) {
            String strSql = "DELETE FROM " + tableName + " WHERE " + fieldName + "=?";

            List<Object> args = new ArrayList<>();

            args.add(value);

            SsDbUtils.update(conn, strSql, args.toArray());

            return true;
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    /**
     * Run SQL with field replacement
     *
     * @param sql SQL with :{field} replaceable values.
     * @param search Values in SQL string with :{field} MUST exist in search
     * @return ApiObject with a root list of returned rows
     * @throws ApiException
     */
    public ApiObject getSQLRows(String dbName, String sql, ApiObject search) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            return SsDbUtils.querySqlAsApiObject(conn, sql, search);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    /**
     * Run SQL with field replacement
     *
     * @param sql SQL with :{field} replaceable values.
     * @param search Values in SQL string with :{field} MUST exist in search
     * @return ApiObject with a root list of returned rows
     * @throws ApiException
     */
    public ApiObject getSQLRows(String dbName, String sql, Object[] search) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            return SsDbUtils.querySqlAsApiObject(conn, sql, search);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    public ApiObject getSQLFirstRow(String dbName, String sql, ApiObject search) throws ApiException {
        ApiObject objResp = getSQLRows(dbName, sql, search);

        if (objResp.isSet("root")) {
            return objResp.getObject("root[0]");
        } else {
            return null;
        }
    }

    public ApiObject getSQLFirstRow(String dbName, String sql, Object[] search) throws ApiException {
        ApiObject objResp = getSQLRows(dbName, sql, search);

        if (objResp.isSet("root")) {
            return objResp.getObject("root[0]");
        } else {
            return null;
        }
    }

    public Integer executeSQL(String dbName, String sql, ApiObject search) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            return SsDbUtils.updateObjectWithCount(conn, sql, search);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    public Integer executeSQL(String dbName, String sql, Object[] args) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            return SsDbUtils.updateWithCount(conn, sql, args);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    public String getSqlString(String dbName, String sql, Object[] args) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            return SsDbUtils.queryForString(conn, sql, args);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    public Integer getSqlInteger(String dbName, String sql, Object[] args) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            return SsDbUtils.queryForInt(conn, sql, args);
        } catch (SQLException sqx) {
            throw new ApiException(500, sqx.getMessage(), sqx);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }

    /**
     * Used to process a manual WHERE clause
     *
     * @param dbName
     * @param sb
     * @param searchObj
     * @throws ApiException
     */
    public void applyOrderAndLimit(String dbName, StringBuilder sb, ApiObject searchObj) throws ApiException {
        try (Connection conn = getConnection(dbName)) {
            SsDbObjects.applyOrderBy(conn, searchObj, sb);

            SsDbObjects.applyLimit(conn, searchObj, sb);
        } catch (Exception ex) {
            throw new ApiException(400, ex.getMessage(), ex);
        }
    }
}
