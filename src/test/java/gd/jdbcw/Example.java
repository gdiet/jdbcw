package gd.jdbcw;

import java.sql.DriverManager;
import java.util.stream.Stream;

public class Example {
    public static void main(String[] args) throws Exception {

        // Initialize the JDBC wrapper with a connection.
        Jdbcw jdbc = new Jdbcw(DriverManager.getConnection("jdbc:h2:mem:test"));

        // Create a table.
        jdbc.ddl("CREATE TABLE users (id BIGINT AUTO_INCREMENT, name VARCHAR)");

        // Prepare to insert rows, returning the generated IDs.
        try (PrepReturnKey<Long> prep = jdbc.prepReturnLong("INSERT INTO users(name) VALUES (?)")) {

            // Insert two rows
            long idAdam = prep.exec("Adam"); System.out.printf("Inserted Adam as user %d%n", idAdam);
            long idEve  = prep.exec("Eve");  System.out.printf("Inserted Eve as user %d%n",  idEve );
        }

        try {

            // Do something within a transaction.
            jdbc.transaction(() -> {
                // Where performance is not the issue, you can run updates and queries directly.
                jdbc.exec("INSERT INTO users(name) VALUES (?)", "Kain");
                System.out.println("Inserted user Kain");
            });

            // You can return results from a transaction.
            Integer updateCount = jdbc.transaction(() -> {
                System.out.println("Inserting user Able");
                return jdbc.exec("INSERT INTO users(name) VALUES (?)", "Able");
            });
            System.out.printf("Update count is %d%n", updateCount);

            // This transaction will be rolled back.
            jdbc.transaction(() -> {
                jdbc.exec("INSERT INTO users(name) VALUES (?)", "Elvis");
                System.out.println("Inserted Elvis, but this will be rolled back ...");
                throw new RuntimeException(); // Triggers a transaction rollback
            });

        } catch (RuntimeException e) { /* Expected, nothing to do. */ }

        // Query results are provided as Java streams.
        Stream<String> users = jdbc.query(rs -> rs.getString(1), "SELECT name FROM users ORDER BY id ASC");
        System.out.println("Users in database:");
        users.forEach(System.out::println);

    }
}
