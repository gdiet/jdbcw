package gd.jdbcw;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;

public class UseCase01Test {
    private Connection con;

    @BeforeClass
    public void init_connection() throws SQLException {
        con = DriverManager.getConnection("jdbc:h2:mem:test");
    }

    @Test
    public void e01_exec_data_definition() throws SQLException {
        Jdbcw.ddl(con, "CREATE TABLE users (id BIGINT, name VARCHAR)");
    }

    @Test
    public void e02_exec_update() throws SQLException {
        int result1 = Jdbcw.exec(con, "INSERT INTO users (id, name) VALUES (?, ?)", 1, "Adam");
        assertEquals(result1, 1, "Update count for single row");
        int result2 = Jdbcw.exec(con, "INSERT INTO users (id, name) VALUES (?, ?)", 2, "Eve");
        assertEquals(result2, 1, "Update count for single row");
    }

    @Test
    public void e03_query_one() throws SQLException {
        String name = Jdbcw.queryOne(con, rs -> rs.getString(1), "SELECT name FROM users WHERE id = ?", 2);
        assertEquals(name, "Eve", "name of user 2");
    }

    @Test
    public void e04_query() throws SQLException {
        try (Stream<String> stream =
             Jdbcw.query(con, rs -> rs.getString(1), "SELECT name FROM users ORDER BY id ASC")
        ) {
            List<String> users = stream.toList();
            assertEquals(users, List.of("Adam", "Eve"), "Users in database");
        }
    }

    @Test
    public void e05_prepare_update() throws SQLException {
        try (PrepExec prep = Jdbcw.prepExec(con, "INSERT INTO users (id, name) VALUES (?, ?)")) {
            assertEquals(prep.exec(4, "Able"), 1, "Update count for single row");
            assertEquals(prep.exec(3, "Kain"), 1, "Update count for single row");
        }
        try (Stream<String> stream =
             Jdbcw.query(con, rs -> rs.getString(1), "SELECT name FROM users ORDER BY id ASC")
        ) {
            List<String> users = stream.toList();
            assertEquals(users, List.of("Adam", "Eve", "Kain", "Able"), "Users in database");
        }
    }

    @Test
    public void e06_prepare_query_one() throws SQLException {
        try (PrepQuery<String> prep =
             Jdbcw.prepQuery(con, rs -> rs.getString(1), "SELECT name FROM users WHERE id = ?")
        ) {
            assertEquals(prep.queryOne(4), "Able", "name of user 4");
            assertEquals(prep.queryOne(1), "Adam", "name of user 1");
        }
    }
}
