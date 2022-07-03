package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Prep implements AutoCloseable {
    private final PreparedStatement prep;

    public Prep(PreparedStatement prep) { this.prep = prep; }

    /** This method is synchronized thus thread safe. For maximum performance in multi-threaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link Prep}. */
    public int run(Object... args) throws SQLException {
        synchronized (this) {
            Jdbcw.setArgs(prep, args);
            return prep.executeUpdate();
        }
    }

    @Override
    public void close() throws SQLException { prep.close(); }
}
