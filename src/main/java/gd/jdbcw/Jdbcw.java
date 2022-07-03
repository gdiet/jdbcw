package gd.jdbcw;

import java.sql.*;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Jdbcw {
    /** Use for DDL executions only. Using this method for other SQL commands is code smell. */
    public static void ddl(final Connection con, final String ddl) throws SQLException {
        try (Statement stat = con.createStatement()) {
            stat.execute(ddl);
        }
    }

    /** Use for one-shot data manipulation like INSERT, UPDATE, DELETE. For better performance, prefer
      * {@link #prepExec(Connection, String)} when running multiple data manipulations of the same type. */
    public static int exec(Connection con, String sql, Object... args) throws SQLException {
        try (PreparedStatement prep = con.prepareStatement(sql)) {
            setArgs(prep, args);
            return prep.executeUpdate();
        }
    }

    /** If possible close the {@link PrepExec} instance after use e.g. by wrapping it into a try-resource block. */
    public static PrepExec prepExec(Connection con, String sql) throws SQLException {
        return new PrepExec(con.prepareStatement(sql));
    }

    public interface Mapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    /** If possible close the stream after use by wrapping it into a try-resource block. */
    public static <T> T queryOne(Connection con, Mapper<T> mapper, String sql, Object... args) throws SQLException {
        try (PreparedStatement prep = con.prepareStatement(sql)) {
            return queryOne(prep, mapper, args);
        }
    }

    static <T> T queryOne(PreparedStatement prep, Mapper<T> mapper, Object... args) throws SQLException {
        setArgs(prep, args);
        try (ResultSet rs = prep.executeQuery()) {
            if (!rs.next()) throw new IllegalStateException("Query returned no results, one required.");
            return mapper.apply(rs);
        }
    }

    /** If possible close the stream after use by wrapping it into a try-resource block. */
    public static <T> Stream<T> query(Connection con, Mapper<T> mapper, String sql, Object... args) throws SQLException {
        // 'prep' is closed when the Stream is closed, see below.
        PreparedStatement prep = con.prepareStatement(sql);
        setArgs(prep, args);
        // No need to close 'rs', it's closed automatically when 'prep' is closed.
        ResultSet rs = prep.executeQuery();
        Iterator<T> it = new Iterator<>() {
            boolean hasNext = rs.next();
            @Override public boolean hasNext() { return hasNext; }
            @Override public T next() {
                if (!hasNext) throw new IllegalStateException("No more results.");
                try { T result = mapper.apply(rs); hasNext = rs.next(); return result; }
                catch (SQLException e) { throw new RuntimeException(e); }
            }
        };
        Spliterator<T> split = Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED);
        return StreamSupport.stream(split, false).onClose(() -> {
            try { prep.close(); }
            catch (SQLException e) { throw new RuntimeException(e); }
        });
    }

    /** If possible close the {@link PrepQuery} instance after use e.g. by wrapping it into a try-resource block. */
    public static <T> PrepQuery<T> prepQuery(Connection con, Mapper<T> mapper, String sql) throws SQLException {
        return new PrepQuery<>(con.prepareStatement(sql), mapper);
    }

    static void setArgs(PreparedStatement prep, Object... args) throws SQLException {
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
