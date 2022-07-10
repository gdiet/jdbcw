package gd.jdbcw;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;

public class UseCase01Test {
    private Jdbcw jdbc;

    @BeforeClass
    public void init_connection() throws SQLException {
        jdbc = new Jdbcw(DriverManager.getConnection("jdbc:h2:mem:test"));
    }

    @Test
    public void e01_exec_data_definition() throws SQLException {
        jdbc.ddl("CREATE TABLE users (id BIGINT, name VARCHAR)");
    }

    @Test
    public void e02_exec_update() throws SQLException {
        int result1 = jdbc.exec("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Adam");
        assertEquals(result1, 1, "Update count for single row");
        int result2 = jdbc.exec("INSERT INTO users (id, name) VALUES (?, ?)", 2, "Eve");
        assertEquals(result2, 1, "Update count for single row");
    }

    @Test
    public void e03_query_one() throws SQLException {
        String name = jdbc.queryOne(rs -> rs.getString(1), "SELECT name FROM users WHERE id = ?", 2);
        assertEquals(name, "Eve", "name of user 2");
    }

    @Test
    public void e04_query() throws SQLException {
        try (Stream<String> stream =
             jdbc.query(rs -> rs.getString(1), "SELECT name FROM users ORDER BY id ASC")
        ) {
            List<String> users = stream.toList();
            assertEquals(users, List.of("Adam", "Eve"), "Users in database");
        }
    }

    @Test
    public void e05_prepare_update() throws SQLException {
        try (PrepExec prep = jdbc.prepExec("INSERT INTO users (id, name) VALUES (?, ?)")) {
            assertEquals(prep.exec(4, "Able"), 1, "Update count for single row");
            assertEquals(prep.exec(3, "Kain"), 1, "Update count for single row");
        }
        try (Stream<String> stream =
             jdbc.query(rs -> rs.getString(1), "SELECT name FROM users ORDER BY id ASC")
        ) {
            assertEquals(stream.toList(), List.of("Adam", "Eve", "Kain", "Able"), "Users in database");
        }
    }

    @Test
    public void e06_prepare_query_one() throws SQLException, InterruptedException {
        try (PrepQuery<String> prep =
             jdbc.prepQuery(rs -> rs.getString(1), "SELECT name FROM users WHERE id = ?")
        ) {
            assertEquals(prep.queryOne(4), "Able", "name of user 4");
            assertEquals(prep.queryOne(1), "Adam", "name of user 1");
        }
    }

    @Test
    public void e07_prepare_query() throws SQLException, InterruptedException {
        String query = "SELECT name FROM users ORDER BY id ASC";
        try (
            PrepQuery<String> prep = jdbc.prepQuery(rs -> rs.getString(1), query);
            Stream<String> stream = prep.query()
        ) {
            assertEquals(stream.toList(), List.of("Adam", "Eve", "Kain", "Able"), "Users in database");
        }
    }

    @Test
    public void e08_return_keys() throws SQLException {
        jdbc.ddl ("CREATE TABLE cars(id BIGINT AUTO_INCREMENT, name VARCHAR)");
        try (
            PrepReturnKeys<Long> prep = jdbc.prepReturnKeys(rs -> rs.getLong(1), "INSERT INTO cars(name) VALUES (?)")
        ) {
            assertEquals(prep.exec("Alfa Romeo"), new PrepReturnKeys.Result<>(1, 1L), "Update count and generated id for single row");
            assertEquals(prep.exec("Beetle"    ), new PrepReturnKeys.Result<>(1, 2L), "Update count and generated id for single row");
        }
    }

    @Test
    public void e09_transaction() throws SQLException {
        jdbc.ddl ("CREATE TABLE pets(id IDENTITY, name VARCHAR UNIQUE)");
        try (
            PrepReturnKeys<Long> prep = jdbc.prepReturnKeys(rs -> rs.getLong(1), "INSERT INTO pets(name) VALUES (?)")
        ) {
            assertEquals(prep.exec("Dog"), new PrepReturnKeys.Result<>(1, 1L), "Update count and generated id for single row");

            try {
                jdbc.transaction(() -> {
                    assertEquals(prep.exec("Cat"), new PrepReturnKeys.Result<>(1, 2L), "Update count and generated id for single row");
                    // This will throw an integrity constraint violation exception which will cause a transaction rollback.
                    prep.exec("Cat");
                });
            } catch (Exception e) { /**/ }

            prep.exec("Fish");

            try (Stream<String> stream =
                 jdbc.query(rs -> rs.getString(1), "SELECT name FROM pets ORDER BY id ASC")
            ) {
                List<String> users = stream.toList();
                assertEquals(users, List.of("Dog", "Fish"), "Pets in database");
            }
        }
    }
}
