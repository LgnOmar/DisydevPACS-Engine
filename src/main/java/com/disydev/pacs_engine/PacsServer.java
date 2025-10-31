package com.disydev.pacs_engine;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.AAssociateRQ;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCEchoSCP;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomServiceRegistry;
import org.dcm4che3.io.DicomOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class PacsServer {

    // --- Configuration ---
    private static final String AE_TITLE = "DISYDEV_PACS";
    private static final int PORT = 11112;
    private static final File STORAGE_DIR = new File("pacs_storage");
    // ---------------------

    private final Device device = new Device("PacsServerDevice");
    private final ApplicationEntity ae = new ApplicationEntity(AE_TITLE);
    private final Connection conn = new Connection();

    public PacsServer() throws GeneralSecurityException, IOException {
        // --- Technical Explanation ---
        // A Device is the top-level object in dcm4che networking. It manages connections
        // and Application Entities. We create one and give it a descriptive name.
        // --- Simplified Explanation ---
        // The Device is the "library building."

        // Configure the network connection
        conn.setPort(PORT);
        conn.setHostname("0.0.0.0"); // Listen on all network interfaces

        // --- Technical Explanation ---
        // The Application Entity (AE) represents our specific DICOM service. It's identified
        // by its AE Title. We attach the network connection to it.
        // --- Simplified Explanation ---
        // The AE is the "Book Receiving Department," known by the name "DISYDEV_PACS".
        ae.addConnection(conn);

        // --- Technical Explanation ---
        // The DicomServiceRegistry is where we "register" the services our AE will provide.
        // We'll add handlers for C-ECHO (ping) and C-STORE (storage).
        // --- Simplified Explanation ---
        // This is the library's employee roster. We're hiring staff for different jobs.
        DicomServiceRegistry serviceRegistry = new DicomServiceRegistry();

        // 1. Add C-ECHO SCP (The 'ping' responder)
        serviceRegistry.addDicomService(new BasicCEchoSCP());

        // 2. Add C-STORE SCP (The storage service)
        // NEW and Corrected block
        serviceRegistry.addDicomService(new BasicCStoreSCP("*") {
            @Override
            protected void store(Association as, PresentationContext pc, Attributes rq,
                                 PDVInputStream data, Attributes rsp) throws IOException {

                // --- Technical Explanation ---
                // 'rq' contains the command set attributes (like SOP Class UID). The patient/study
                // information is inside the dataset itself, which we get from the 'data' stream.
                // We cannot read the stream twice. So, the standard pattern is:
                // 1. Save the entire incoming data stream to a temporary file.
                // 2. Read the metadata from that temporary file.
                // 3. Use the metadata to construct the final path and filename.
                // 4. Move/rename the temporary file to its final destination.
                // --- Simplified Explanation ---
                // The librarian receives a wrapped package (the `data` stream). They can't see the
                // book's title through the wrapping. So they put the package on a temporary
                // cart (the temp file), unwrap it to read the title (read the metadata), figure out
                // the correct shelf, and then move the book from the cart to the permanent shelf.

                String sopClassUID = rq.getString(Tag.AffectedSOPClassUID);
                String sopInstanceUID = rq.getString(Tag.AffectedSOPInstanceUID);

                System.out.println("-------------------------------------------");
                System.out.println("Received DICOM object for storage:");
                System.out.println("  SOP Class UID: " + sopClassUID);
                System.out.println("  SOP Instance UID: " + sopInstanceUID);

                // 1. Save the incoming data to a temporary file
                File tempFile = File.createTempFile("store-", ".tmp", STORAGE_DIR);
                Attributes fmi; // File Meta Information
                Attributes dataset;

                try (DicomInputStream dis = new DicomInputStream(data)) {
                    dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.YES);

                    // Read File Meta Information (Group 0002)
                    fmi = dis.readFileMetaInformation();

                    // Read the main dataset
                    dataset = dis.readDataset();

                    // Write everything to the temp file
                    try (DicomOutputStream dos = new DicomOutputStream(new FileOutputStream(tempFile))) {
                        dos.writeDataset(fmi, dataset);
                    }
                }

                // 2. Read metadata from the saved temp file to determine final path
                // (Even though we have `dataset` in memory, this pattern is useful if you were to
                // just stream directly to disk without holding the whole object in memory).
                String patientID = dataset.getString(Tag.PatientID, "UNKNOWN_PATIENT");
                String studyUID = dataset.getString(Tag.StudyInstanceUID, "UNKNOWN_STUDY");
                String seriesUID = dataset.getString(Tag.SeriesInstanceUID, "UNKNOWN_SERIES");

                // 3. Construct the final path
                // Use File.separator for cross-platform compatibility
                File seriesDir = new File(STORAGE_DIR, patientID + File.separator + studyUID + File.separator + seriesUID);
                if (!seriesDir.exists()) {
                    seriesDir.mkdirs();
                }

                // 4. Move the temporary file to its final destination
                File finalFile = new File(seriesDir, sopInstanceUID + ".dcm");

                // shutil.move equivalent in Java is a bit more manual.
                if (tempFile.renameTo(finalFile)) {
                    System.out.println("Successfully stored file to: " + finalFile.getAbsolutePath());
                } else {
                    // If renameTo fails (e.g., across different filesystems), try copy-then-delete
                    System.err.println("Rename failed, attempting copy...");
                    // You would add copy/delete logic here for a production system. For now, we'll log it.
                    tempFile.delete(); // Clean up temp file
                    throw new IOException("Failed to move temp file to final location: " + finalFile.getAbsolutePath());
                }
                System.out.println("-------------------------------------------");
            }
        });

        // --- Technical Explanation ---
        // We set our service registry as the handler for incoming DIMSE requests for our AE.
        // We also define what SOP Classes we are willing to accept. Here we accept
        // any standard Storage SOP Class. This is part of the Association negotiation.
        // --- Simplified Explanation ---
        // We post the employee roster on the wall. We also put up a sign that says "We accept
        // all kinds of standard books for storage here."
        // NEW and Corrected block
        ae.setDimseRQHandler(serviceRegistry);
        ae.addTransferCapability(new TransferCapability(null, "*", TransferCapability.Role.SCP, "*"));

        // First, tell the device about the connection it will manage.
        device.addConnection(conn);
        // Then, tell the device about the application entity that uses that connection.
        device.addApplicationEntity(ae);

        ExecutorService executorService = Executors.newCachedThreadPool();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        device.setExecutor(executorService);
        device.setScheduledExecutor(scheduledExecutorService);
    }

    public void start() throws IOException, GeneralSecurityException {
        System.out.println("Starting PACS server...");
        device.bindConnections();
        System.out.println("Server started. Listening on port " + PORT + " with AE Title " + AE_TITLE);
    }

    public void stop() {
        System.out.println("Stopping PACS server...");
        device.unbindConnections();
    }

    public static void main(String[] args) {
        try {
            PacsServer server = new PacsServer();
            server.start();

            // Add a shutdown hook to ensure graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown hook triggered.");
                server.stop();
            }));

        } catch (Exception e) {
            System.err.println("Failed to start PACS server: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}