package com.disydev.pacs_engine;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {
    // --- Technical Explanation ---
    // JDBC (Java Database Connectivity) is the standard Java API for connecting to SQL databases.
    // The JDBC URL tells the driver how to connect. For H2, this format means:
    // `jdbc:h2:` - Use the H2 driver.
    // `./pacs_storage/pacs_db` - Create a database file named 'pacs_db' inside our 'pacs_storage' folder.
    // --- Simplified Explanation ---
    // This is the address to our database filing cabinet. It's a file on the hard drive
    // right next to where we store the DICOM images.
    private static final String DB_URL = "jdbc:h2:./pacs_storage/pacs_db";
    private Connection connection;

    public DatabaseManager() {
        try {
            // Load the H2 Driver
            Class.forName("org.h2.Driver");
            // Establish the connection
            this.connection = DriverManager.getConnection(DB_URL, "sa", ""); // user "sa", empty password
            System.out.println("Database connection established.");

            // Create tables if they don't already exist
            createTables();
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Failed to initialize database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void createTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // SQL to create our tables.
            // We use "CREATE TABLE IF NOT EXISTS" so it doesn't fail if we restart the app.
            // `StudyInstanceUID` is the PRIMARY KEY, meaning it must be unique.
            String createStudiesTable = "CREATE TABLE IF NOT EXISTS Studies ("
                    + "StudyInstanceUID VARCHAR(255) PRIMARY KEY, "
                    + "PatientID VARCHAR(255), "
                    + "PatientName VARCHAR(255), "
                    + "StudyDate VARCHAR(8), "
                    + "StudyTime VARCHAR(14), "
                    + "AccessionNumber VARCHAR(255), "
                    + "StudyDescription VARCHAR(255)"
                    + ")";

            // More tables for Series and Instances can be added here later.
            // For now, we will just index studies.
            stmt.execute(createStudiesTable);
            System.out.println("Database tables created or already exist.");
        }
    }

    public void indexDicomObject(Attributes dataset) {
        // --- Technical Explanation ---
        // This is a `PreparedStatement`. It's a way to run SQL queries with parameters (the `?`).
        // It is much safer and more efficient than building strings. It prevents "SQL Injection"
        // The SQL `MERGE` command is a convenient way to do an "upsert":
        // If a record with this StudyInstanceUID already exists, `UPDATE` it.
        // If it doesn't exist, `INSERT` a new one.
        // --- Simplified Explanation ---
        // This is the librarian's standard "Update a Study Record" form. It has blank spots (`?`)
        // for the patient's name, ID, etc. When we have a new DICOM file, we fill in the blanks
        // on this form and give it to the database to file.
        String sql = "MERGE INTO Studies (StudyInstanceUID, PatientID, PatientName, StudyDate, StudyTime, AccessionNumber, StudyDescription) "
                + "KEY(StudyInstanceUID) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // Set values for the '?' parameters, using the DICOM dataset
            pstmt.setString(1, dataset.getString(Tag.StudyInstanceUID));
            pstmt.setString(2, dataset.getString(Tag.PatientID));
            pstmt.setString(3, dataset.getString(Tag.PatientName));
            pstmt.setString(4, dataset.getString(Tag.StudyDate));
            pstmt.setString(5, dataset.getString(Tag.StudyTime));
            pstmt.setString(6, dataset.getString(Tag.AccessionNumber));
            pstmt.setString(7, dataset.getString(Tag.StudyDescription));

            // Execute the query
            pstmt.executeUpdate();
            System.out.println("Indexed study: " + dataset.getString(Tag.StudyInstanceUID));

        } catch (SQLException e) {
            System.err.println("Failed to index DICOM object: " + e.getMessage());
        }
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("Database connection closed.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing database connection: " + e.getMessage());
        }
    }
}