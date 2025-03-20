package com.progbits.db.dataaccess;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import com.progbits.api.utils.ApiResources;
import com.progbits.db.SsDbUtils;
import io.github.classgraph.ResourceList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scarr
 */
public class DbUpdateApplication {

    private static final Logger log = LoggerFactory.getLogger(DbUpdateApplication.class);

    public static void main(String[] args) {
        DbUpdateApplication app = new DbUpdateApplication();

        if (app.processSql(args)) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    public boolean processSql(String[] args) {
        boolean bRet = false;

        ApiObject sqlFiles = pullSchemaSql();

        try (Connection conn = DataManager.getInstance().getConnection()) {
            String currentVersion = pullDbVersion(conn);

            for (var entry : sqlFiles.keySet()) {
                if (currentVersion.compareTo(entry) < 0) {
                    List<String> sqlEntries = sqlFiles.getStringArray(entry);
                    StringBuilder dbErrors = new StringBuilder();

                    for (var sql : sqlEntries) {
                        try {
                            SsDbUtils.update(conn, sql, new Object[]{});
                        } catch (Exception sqlEx) {
                            dbErrors.append(sqlEx.getMessage()).append("\n");
                        }
                    }

                    SsDbUtils.update(conn, SQL_INSERT_VERSION, new Object[]{entry, dbErrors.toString()});
                }
            }

            bRet = true;
        } catch (Exception ex) {
            log.error("processSql Error", ex);
        }

        return bRet;
    }

    private final Pattern pattern = Pattern.compile(".*schema_(.*?).sql");

    private final ApiResources apiResources = ApiResources.getInstance();

    public ApiObject pullSchemaSql() {
        ApiObject retSql = new ApiObject();

        try {
            ResourceList resources = apiResources.getResourcesWithWildcard("db/schemas/schema_*.sql");

            log.info("Found {} Files", resources.size());

            for (var entry : resources) {
                Matcher match = pattern.matcher(entry.getPath());

                if (match.matches()) {
                    String fileName = match.group(1);

                    if (!retSql.containsKey(fileName)) {
                        retSql.createStringArray(match.group(1));
                    }

                    StringBuilder sb = new StringBuilder();

                    try (InputStream is = entry.open(); BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
                        String fileLine;
                        while ((fileLine = br.readLine()) != null) {
                            if (fileLine.startsWith("--")) {
                                // Ignore this line
                            } else if (fileLine.trim().isBlank()) {
                                retSql.getStringArray(fileName).add(sb.toString());
                                sb = new StringBuilder();
                            } else {
                                sb.append(fileLine).append("\n");
                            }
                        }
                    } catch (IOException ex) {
                        log.error("File Read Failed: {}", ex.getMessage());
                    }

                    if (!sb.isEmpty()) {
                        retSql.getStringArray(fileName).add(sb.toString());
                    }
                }
            }
        } catch (ApiException apx) {
            log.error("pullScemaSql Error: " + apx.getMessage());
        }

        return retSql;
    }

    private static final String CREATE_DB_VERSIONS = """
                                                     CREATE TABLE db_versions (
                                                        db_version VARCHAR(50) PRIMARY KEY,
                                                        db_update TIMESTAMP DEFAULT NOW(),
                                                        db_errors VARCHAR(1000)
                                                     )
                                                     """;

    private static final String SQL_SELECT_VERSION = """
                                                     SELECT MAX(db_version) AS lastUpdate 
                                                     FROM db_versions
                                                     """;

    private static final String SQL_INSERT_VERSION = """
                                                     INSERT INTO db_versions (db_version, db_errors) VALUES (?, ?)
                                                     """;

    private String pullDbVersion(Connection conn) throws Exception {
        String retVal;

        try {
            retVal = SsDbUtils.queryForString(conn, SQL_SELECT_VERSION, new Object[]{});

            if (retVal == null) {
                retVal = "";
            }
        } catch (Exception ex) {
            SsDbUtils.update(conn, CREATE_DB_VERSIONS, new Object[]{});
            retVal = "";
        }

        return retVal;
    }

}
