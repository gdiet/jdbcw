package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
        synchronized (this) {
            Jdbcw.setArgs(prep, args);
            try (ResultSet rs = prep.executeQuery()) {
                if (!rs.next()) throw new IllegalStateException("Query returned no results, one required.");
                return mapper.apply(rs);
            }
        }
    }

    @Override
    public void close() throws SQLException { prep.close(); }
}
