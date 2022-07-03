package gd.jdbcw;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class UseCase01Test {
    private Connection con;

    @BeforeClass
    public void init_connection() throws SQLException {
        con = DriverManager.getConnection("jdbc:h2:mem:test");
    }

    @Test
    public void e01_run_data_definition() throws SQLException {
        Jdbcw.runDDL(con, "CREATE TABLE users (id BIGINT, name VARCHAR)");
    }
}
