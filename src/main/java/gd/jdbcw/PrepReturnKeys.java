package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PrepReturnKeys<T> implements AutoCloseable {
    public record Result<T>(int rows, T keys) {}

    private final PreparedStatement prep;
    private final Jdbcw.Mapper<T> mapper;

    PrepReturnKeys(PreparedStatement prep, Jdbcw.Mapper<T> mapper) {
        this.prep = prep;
        this.mapper = mapper;
    }

    /** This method is synchronized thus thread safe. For maximum performance in multithreaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link PrepReturnKeys}. */
    public Result<T> exec(Object... args) throws SQLException {
        // TODO The return type is too unwieldy
        synchronized (this) {
            Jdbcw.setArgs(prep, args);
            int rows = prep.executeUpdate();
            try (ResultSet res = prep.getGeneratedKeys()) {
                if (!res.next()) throw new IllegalStateException("Missing result set for generated keys.");
                return new Result<>(rows, mapper.apply(res));
            }
        }
    }

    @Override
    public void close() throws SQLException { prep.close(); }
}
