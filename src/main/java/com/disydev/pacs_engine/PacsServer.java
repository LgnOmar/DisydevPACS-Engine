package com.disydev.pacs_engine;

// --- Imports ---
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.Connection;
import org.dcm4che3.net.Device;
import org.dcm4che3.net.PDVInputStream;
import org.dcm4che3.net.TransferCapability;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCEchoSCP;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomService;
import org.dcm4che3.net.service.DicomServiceRegistry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class PacsServer {

    private static final String AE_TITLE = "DISYDEV_PACS";
    private static final int PORT = 11112;
    private static final File STORAGE_DIR = new File("pacs_storage");

    private final Device device = new Device("PacsServerDevice");
    private final ApplicationEntity ae = new ApplicationEntity(AE_TITLE);
    private final Connection conn = new Connection();

    public PacsServer() throws IOException {
        device.setDeviceName("PacsServerDevice");
        conn.setPort(PORT);
        conn.setHostname("0.0.0.0");

        ae.addConnection(conn);
        device.addConnection(conn);
        device.addApplicationEntity(ae);

        DicomServiceRegistry serviceRegistry = new DicomServiceRegistry();

        serviceRegistry.addDicomService(new BasicCEchoSCP());

        // --- THE CORRECT C-STORE IMPLEMENTATION ---
        // We use BasicCStoreSCP and we override its store method.
        // This is the correct low-level way to handle incoming data.
        serviceRegistry.addDicomService(new BasicCStoreSCP("*") {
            @Override
            protected void store(Association as, PresentationContext pc, Attributes rq,
                                 PDVInputStream data, Attributes rsp) throws IOException {

                String sopClassUID = rq.getString(Tag.AffectedSOPClassUID);
                String sopInstanceUID = rq.getString(Tag.AffectedSOPInstanceUID);

                // Create a temporary file to hold the received data.
                if (!STORAGE_DIR.exists()) {
                    STORAGE_DIR.mkdirs();
                }
                File tempFile = File.createTempFile("store-", ".tmp", STORAGE_DIR);

                try {
                    // DicomInputStream lets us read DICOM data from the raw network stream
                    try (DicomInputStream dis = new DicomInputStream(data)) {
                        // Read the file meta information and the dataset
                        dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.YES);
                        Attributes fileMetaInformation = dis.readFileMetaInformation();
                        Attributes dataset = dis.readDataset();

                        // Use the metadata to create the final directory path
                        String patientID = dataset.getString(Tag.PatientID, "UNKNOWN_PATIENT");
                        String studyUID = dataset.getString(Tag.StudyInstanceUID, "UNKNOWN_STUDY");
                        String seriesUID = dataset.getString(Tag.SeriesInstanceUID, "UNKNOWN_SERIES");

                        File seriesDir = new File(STORAGE_DIR, patientID + File.separator + studyUID + File.separator + seriesUID);
                        if (!seriesDir.exists()) {
                            seriesDir.mkdirs();
                        }

                        // Write the complete DICOM file (meta info + dataset) to disk
                        File finalFile = new File(seriesDir, sopInstanceUID + ".dcm");
                        System.out.println("Storing DICOM object to: " + finalFile.getAbsolutePath());

                        try (DicomOutputStream dos = new DicomOutputStream(finalFile)) {
                            dos.writeDataset(fileMetaInformation, dataset);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error storing DICOM file: " + e.getMessage());
                    e.printStackTrace();
                    // Let the calling device know that something went wrong
                    throw new IOException("Failed to store DICOM object.", e);
                } finally {
                    // It is critical to clean up the temporary file if it was used and an error occurred.
                    // In this corrected code, we write directly to the final path, so no temp file.
                }
            }
        });

        ae.setDimseRQHandler(serviceRegistry);
        ae.addTransferCapability(new TransferCapability(null, "*", TransferCapability.Role.SCP, "*"));

        ExecutorService executorService = Executors.newCachedThreadPool();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        device.setExecutor(executorService);
        device.setScheduledExecutor(scheduledExecutorService);
    }

    public void start() throws Exception {
        System.out.println("Starting PACS server...");
        device.bindConnections();
        System.out.println("Server started. Listening on port " + PORT + " with AE Title " + AE_TITLE);
    }

    public void stop() {
        System.out.println("Stopping PACS server...");
        device.unbindConnections();
    }

    public static void main(String[] args) {
        PacsServer server = null;
        try {
            server = new PacsServer();
            server.start();
            final PacsServer finalServer = server;
            Runtime.getRuntime().addShutdownHook(new Thread(finalServer::stop));
        } catch (Exception e) {
            System.err.println("FATAL: Failed to start PACS server: " + e.getMessage());
            e.printStackTrace(System.err);
            if (server != null) {
                server.stop();
            }
            System.exit(1);
        }
    }
}