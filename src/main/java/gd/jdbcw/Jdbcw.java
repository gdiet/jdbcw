package gd.jdbcw;

import java.sql.*;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    public interface Mapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    /** Make sure to close the stream after use by wrapping it into a try-resource block. */
    public static <T> Stream<T> runQuery(Connection con, Mapper<T> mapper, String sql, Object... args) throws SQLException {
        PreparedStatement prep = con.prepareStatement(sql);
        setArgs(prep, args);
        ResultSet rs = prep.executeQuery();
        Iterator<T> iter = new Iterator<>() {
            boolean hasNext = rs.next();
            @Override public boolean hasNext() { return hasNext; }
            @Override public T next() {
                if (!hasNext) throw new IllegalStateException("No more results.");
                try { T result = mapper.apply(rs); hasNext = rs.next(); return result; }
                catch (SQLException e) { throw new RuntimeException(e); }
            }
        };
        Spliterator<T> split = Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED);
        return StreamSupport.stream(split, false).onClose(() -> {
            try { prep.close(); }
            catch (SQLException e) { throw new RuntimeException(e); }
        });
    }

    private static void setArgs(PreparedStatement prep, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            /* Java 18+, the type check below can also be written using switch + patterns:
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
