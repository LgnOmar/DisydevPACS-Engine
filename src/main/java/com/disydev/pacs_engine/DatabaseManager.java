package com.disydev.pacs_engine;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
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

    public List<Attributes> queryStudies(Attributes keys) {
        // --- Technical Explanation ---
        // This method takes a DICOM Attributes object (the query keys from the C-FIND request)
        // and dynamically builds a SQL query. It uses a PreparedStatement to prevent SQL injection.
        // It iterates through the SQL ResultSet and builds a list of DICOM Attributes objects to return.
        // The '*' in DICOM is a wildcard, so we translate it to the SQL wildcard '%'.
        // --- Simplified Explanation ---
        // A doctor gives the librarian a search request form (`keys`). The librarian uses this
        // form to write a precise search query for the card catalog (the database).
        // For each matching card found, the librarian creates a new summary card (`Attributes` object)
        // and adds it to a pile (`List<Attributes>`) to give back to the doctor.

        List<Attributes> resultList = new ArrayList<>();

        StringBuilder sql = new StringBuilder("SELECT * FROM Studies WHERE 1=1");
        List<Object> params = new ArrayList<>();

        // Dynamically build the WHERE clause based on the provided keys
        if (keys.containsValue(Tag.PatientName)) {
            sql.append(" AND PatientName LIKE ?");
            params.add(keys.getString(Tag.PatientName).replace('*', '%'));
        }
        if (keys.containsValue(Tag.PatientID)) {
            sql.append(" AND PatientID = ?");
            params.add(keys.getString(Tag.PatientID));
        }
        if (keys.containsValue(Tag.StudyDate)) {
            // Handle date ranges if necessary in a real PACS, for now, exact match
            sql.append(" AND StudyDate = ?");
            params.add(keys.getString(Tag.StudyDate));
        }
        if (keys.containsValue(Tag.AccessionNumber)) {
            sql.append(" AND AccessionNumber = ?");
            params.add(keys.getString(Tag.AccessionNumber));
        }

        System.out.println("Executing C-FIND query: " + sql);

        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Attributes resultAttrs = new Attributes();
                    resultAttrs.setString(Tag.PatientID, VR.LO, rs.getString("PatientID"));
                    resultAttrs.setString(Tag.PatientName, VR.PN, rs.getString("PatientName"));
                    resultAttrs.setString(Tag.StudyInstanceUID, VR.UI, rs.getString("StudyInstanceUID"));
                    resultAttrs.setString(Tag.StudyDate, VR.DA, rs.getString("StudyDate"));
                    resultAttrs.setString(Tag.StudyTime, VR.TM, rs.getString("StudyTime"));
                    resultAttrs.setString(Tag.AccessionNumber, VR.SH, rs.getString("AccessionNumber"));
                    resultAttrs.setString(Tag.StudyDescription, VR.LO, rs.getString("StudyDescription"));

                    // Add query retrieve level, which is required in a C-FIND response
                    resultAttrs.setString(Tag.QueryRetrieveLevel, VR.CS, "STUDY");

                    resultList.add(resultAttrs);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error during C-FIND query: " + e.getMessage());
            // In a real system, you might want to return an error status instead of an empty list
        }

        return resultList;
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