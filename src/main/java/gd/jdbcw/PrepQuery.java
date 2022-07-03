package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PrepQuery<T> implements AutoCloseable {
    private final PreparedStatement prep;
    private final Jdbcw.Mapper<T> mapper;

    public PrepQuery(PreparedStatement prep, Jdbcw.Mapper<T> mapper) {
        this.prep = prep;
        this.mapper = mapper;
    }

    /** This method is synchronized thus thread safe. For maximum performance in multithreaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link PrepQuery}. */
    public T queryOne(Object... args) throws SQLException {
        synchronized (this) { return Jdbcw.queryOne(prep, mapper, args); }
    }

    @Override
    public void close() throws SQLException { prep.close(); }
}
