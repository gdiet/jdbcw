package gd.jdbcw;

import java.sql.*;

public class ExamplePlainJDBC {
    public static void main(String[] args) throws Exception {

        // Initialize the JDBC connection.
        Connection con = DriverManager.getConnection("jdbc:h2:mem:test");
        con.setAutoCommit(true); // Not strictly necessary, but documents nicely what is going on.

        // Create a table.
        try(Statement stat = con.createStatement()) {
            stat.execute("CREATE TABLE users (id BIGINT AUTO_INCREMENT, name VARCHAR)");
        }

        // Prepare to insert rows, returning the generated IDs.
        try (PreparedStatement prep = con.prepareStatement("INSERT INTO users(name) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            // Insert a row
            prep.setString(1, "Adam");
            prep.executeUpdate();
            try (ResultSet rs = prep.getGeneratedKeys()) {
                if(!rs.next()) throw new IllegalArgumentException("No generated key.");
                long idAdam = rs.getLong(1);
                System.out.printf("Inserted Adam as user %d%n", idAdam);
            }
        }

        try {
            // Do something within a transaction.
            con.setAutoCommit(false);
            try(Statement stat = con.createStatement()) {
                int updateCount = stat.executeUpdate("INSERT INTO users(name) VALUES ('Eve')");
                System.out.println("Inserted Eve, but this will be rolled back ...");
                if (updateCount == 1) throw new RuntimeException(); // Triggers a transaction rollback
                con.commit();
            } catch (Exception e) { con.rollback(); throw e; }
        }
        catch (RuntimeException e) { /* Expected, nothing to do. */ }
        finally { con.setAutoCommit(true);}

        // Read all users.
        try(Statement stat = con.createStatement()) {
            ResultSet rs = stat.executeQuery("SELECT name FROM users ORDER BY id ASC");
            System.out.println("Users in database:");
            while (rs.next()) {
                String user = rs.getString(1);
                System.out.println(user);
            }
        }

    }
}
