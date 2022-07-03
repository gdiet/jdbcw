package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PrepExec implements AutoCloseable {
    private final PreparedStatement prep;

    PrepExec(PreparedStatement prep) { this.prep = prep; }

    /** This method is synchronized thus thread safe. For maximum performance in multithreaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link PrepExec}. */
    public int exec(Object... args) throws SQLException {
        synchronized (this) {
            Jdbcw.setArgs(prep, args);
            return prep.executeUpdate();
        }
    }

    @Override
    public void close() throws SQLException { prep.close(); }
}
