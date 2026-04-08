package com.jasqlite.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for JaSQLite.
 */
public class JaSQLiteDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new JaSQLiteDriver());
        } catch (SQLException e) {
            // ignore
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        String path = url.substring(com.jasqlite.JaSQLite.DRIVER_PREFIX.length());
        boolean readOnly = Boolean.parseBoolean(info.getProperty("readonly", "false"));
        com.jasqlite.store.Database db = new com.jasqlite.store.Database(path, readOnly);
        return new JaSQLiteConnection(db);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(com.jasqlite.JaSQLite.DRIVER_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() { return 1; }

    @Override
    public int getMinorVersion() { return 0; }

    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
