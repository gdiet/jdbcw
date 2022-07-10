package gd.jdbcw;

import java.sql.*;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Jdbcw {

    public interface Mapper<T> { T apply(ResultSet rs) throws SQLException; }

    /** RuntimeException wrapper for an {@link SQLException}. */
    public static final class JDBCWException extends RuntimeException {
        public final SQLException cause;
        public JDBCWException(SQLException cause) { this.cause = cause; }
    }
    public interface Transaction { void run() throws SQLException; }
    public interface TransactionWithResult<T> { T get() throws SQLException; }

    private final Connection con;

    /** Sets setAutoCommit(true). */
    public Jdbcw(final Connection con) throws SQLException { this.con = con; con.setAutoCommit(true); }

    /** Make sure that no concurrent SQL commands are run that are not supposed to be part of this transaction. */
    public void transaction(Transaction t) throws SQLException {
        try { con.setAutoCommit(false); t.run(); con.commit(); }
        catch (Exception e) { con.rollback(); throw e; }
        finally { con.setAutoCommit(true); }
    }

    /** Make sure that no concurrent SQL commands are run that are not supposed to be part of this transaction. */
    public <T> T transaction(TransactionWithResult<T> t) throws SQLException {
        try { con.setAutoCommit(false); T result = t.get(); con.commit(); return result; }
        catch (Exception e) { con.rollback(); throw e; }
        finally { con.setAutoCommit(true); }
    }

    /** Use for DDL executions only. Using this method for other SQL commands is code smell. */
    public void ddl(final String ddl) throws SQLException {
        try (Statement stat = con.createStatement()) { stat.execute(ddl); }
    }

    /** Use for one-shot data manipulation like INSERT, UPDATE, DELETE. For better performance, prefer
      * {@link #prepExec(String)} when running multiple data manipulations of the same type. */
    public int exec(final String sql, final Object... args) throws SQLException {
        try (PreparedStatement prep = con.prepareStatement(sql)) { setArgs(prep, args); return prep.executeUpdate(); }
    }

    /** If possible close the {@link PrepExec} instance after use e.g. by wrapping it into a try-resource block. */
    public PrepExec prepExec(final String sql) throws SQLException {
        return new PrepExec(con.prepareStatement(sql));
    }

    /** If possible close the {@link PrepReturnKey} instance after use e.g. by wrapping it into a try-resource block. */
    public <T> PrepReturnKey<T> prepReturnKey(final Mapper<T> mapper, final String sql) throws SQLException {
        return new PrepReturnKey<>(con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS), mapper);
    }

    /** Use when a generated BIGINT or other Java long type key is returned.
      * If possible close the {@link PrepReturnKey} instance after use e.g. by wrapping it into a try-resource block. */
    public PrepReturnKey<Long> prepReturnLong(final String sql) throws SQLException {
        return prepReturnKey(rs -> rs.getLong(1), sql);
    }

    /** If possible close the stream after use by wrapping it into a try-resource block. */
    public <T> T queryOne(final Mapper<T> mapper, final String sql, final Object... args) throws SQLException {
        try (PreparedStatement prep = con.prepareStatement(sql)) { return queryOne(prep, mapper, args); }
    }

    static <T> T queryOne(PreparedStatement prep, Mapper<T> mapper, Object... args) throws SQLException {
        setArgs(prep, args);
        try (ResultSet rs = prep.executeQuery()) {
            if (!rs.next()) throw new IllegalStateException("Query returned no results, one required.");
            return mapper.apply(rs);
        }
    }

    /** If possible close the stream after use e.g. by wrapping it into a try-resource block.
      * Note that the close method may throw an {@link JDBCWException}. */
    public <T> Stream<T> query(final Mapper<T> mapper, final String sql, final Object... args) throws SQLException {
        PreparedStatement prep = con.prepareStatement(sql);
        return query(prep, mapper, args).onClose(() -> {
            try { prep.close(); } catch (SQLException e) { throw new JDBCWException(e); }
        });
    }

    public static <T> Stream<T> query(PreparedStatement prep, Mapper<T> mapper, Object... args) throws SQLException {
        setArgs(prep, args);
        // The ResultSet is not closed when the end of stream is reached - too little value for too big effort here.
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
        return StreamSupport.stream(split, false);
    }

    /** If possible close the {@link PrepQuery} instance after use e.g. by wrapping it into a try-resource block. */
    public <T> PrepQuery<T> prepQuery(Mapper<T> mapper, String sql) throws SQLException {
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
