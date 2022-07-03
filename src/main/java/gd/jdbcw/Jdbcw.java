package gd.jdbcw;

import java.sql.Connection;
import java.sql.SQLException;

public class Jdbcw {
    /** Use for DDL executions only. Using this method for other SQL commands is code smell. */
    public static void runDDL(final Connection con, final String ddl) throws SQLException {
        con.createStatement().execute(ddl);
    }
}
