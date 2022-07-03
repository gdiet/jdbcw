package gd.jdbcw;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Jdbcw {
    /** Use for DDL executions only. Using this method for other SQL commands is code smell. */
    public static void runDDL(final Connection con, final String ddl) throws SQLException {
        try (Statement stat = con.createStatement()) {
            stat.execute(ddl);
        }
    }

    /** Use for data manipulation like INSERT, UPDATE, DELETE. For maximum performance, prefer
      * the Jdbcw.prep* prepared statement methods. */
    public static int runDML(Connection con, String sql, Object... args) throws SQLException {
        try (PreparedStatement prep = con.prepareStatement(sql)) {
            setArgs(prep, args);
            return prep.executeUpdate();
        }
    }

    private static void setArgs(PreparedStatement prep, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            /* Starting with Java 18, the type check below can also be written using patterns in switch statements:
             *
             * switch (args[i]) {
             *     case String s -> prep.setString(i+1, s);
             *     case Long   l -> prep.setLong  (i+1, l);
             *     case default -> throw new RuntimeException("Hey this is bad.");
             * } */
            Object arg = args[i];
            if      (arg instanceof String  a) prep.setString(i+1, a);
            else if (arg instanceof Integer a) prep.setInt   (i+1, a);
            else if (arg instanceof Long    a) prep.setLong  (i+1, a);
            else    throw new SQLException(
                        "Unsupported argument type %s: %s".formatted(arg.getClass().getName(), arg)
                    );
        }
    }
}
