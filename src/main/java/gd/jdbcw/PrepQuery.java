package gd.jdbcw;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class PrepQuery<T> implements AutoCloseable {
    private final PreparedStatement prep;
    private final Jdbcw.Mapper<T> mapper;
    private final ReentrantLock lock = new ReentrantLock();
    private final int timeoutSeconds;

    public PrepQuery(PreparedStatement prep, Jdbcw.Mapper<T> mapper, int timeoutSeconds) {
        this.prep = prep;
        this.mapper = mapper;
        this.timeoutSeconds = timeoutSeconds;
    }

    /** Use a timeout of 5 seconds when waiting for previous queries to finish. */
    public PrepQuery(PreparedStatement prep, Jdbcw.Mapper<T> mapper) {
        this(prep, mapper, 5);
    }

    /** This method is uses locks for thread safety. For maximum performance in multithreaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link PrepQuery}. */
    public T queryOne(Object... args) throws SQLException, InterruptedException {
        if (!lock.tryLock(timeoutSeconds, TimeUnit.SECONDS)) throw new SQLTimeoutException("Previous query still busy.");
        try { return Jdbcw.queryOne(prep, mapper, args); } finally { lock.unlock(); }
    }

    /** This method is uses locks for thread safety. For maximum performance in multithreaded environments, consider
      * using e.g. {@link ThreadLocal} instances of {@link PrepQuery}. The stream must be consumed quickly and closed
      * after use e.g. by wrapping it into a try-resource block to avoid timeouts and deadlocks. */
    public Stream<T> query(Object... args) throws SQLException, InterruptedException {
        if (!lock.tryLock(timeoutSeconds, TimeUnit.SECONDS)) throw new SQLTimeoutException("Previous query still busy.");
        try { return Jdbcw.query(prep, mapper, args).onClose(lock::unlock); }
        catch (Throwable e) { lock.unlock(); throw e; }
    }

    @Override public void close() throws SQLException { prep.close(); }
}
