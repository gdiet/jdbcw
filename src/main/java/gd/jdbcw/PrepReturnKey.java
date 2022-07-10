package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PrepReturnKey<T> implements AutoCloseable {
    private final PreparedStatement prep;
    private final Jdbcw.Mapper<T> mapper;

    PrepReturnKey(PreparedStatement prep, Jdbcw.Mapper<T> mapper) {
        this.prep = prep;
        this.mapper = mapper;
    }

    static PrepReturnKey<Long> longType(PreparedStatement prep) {
        return new PrepReturnKey<>(prep, (rs) -> rs.getLong(1));
    }

    /** This method is synchronized thus thread safe. For maximum performance in multithreaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link PrepReturnKey}. */
    public T exec(Object... args) throws SQLException {
        synchronized (this) {
            Jdbcw.setArgs(prep, args);
            int rows = prep.executeUpdate();
            if (rows != 1) throw new IllegalStateException("Prepared statement should affect one row, not " + rows);
            try (ResultSet res = prep.getGeneratedKeys()) {
                if (!res.next()) throw new IllegalStateException("Missing result set for generated keys.");
                return mapper.apply(res);
            }
        }
    }

    @Override
    public void close() throws SQLException { prep.close(); }
}
