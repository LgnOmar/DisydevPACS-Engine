package com.disydev.pacs_engine;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.UID;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCFindSCP;
import org.dcm4che3.net.service.BasicQueryTask;
import org.dcm4che3.net.service.DicomServiceException;
import org.dcm4che3.net.service.QueryTask;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MyCFindSCP extends BasicCFindSCP {

    private final DatabaseManager dbManager;

    public MyCFindSCP(DatabaseManager dbManager) {
        super(UID.StudyRootQueryRetrieveInformationModelFind);
        this.dbManager = dbManager;
    }

    @Override
    protected QueryTask calculateMatches(Association as,
                                         PresentationContext pc,
                                         Attributes rq,
                                         Attributes keys)
            throws DicomServiceException {

        System.out.println("Received C-FIND request with keys:\n" + keys);

        final List<Attributes> results = dbManager.queryStudies(keys);

        System.out.println("Found " + results.size() + " matching studies in the database.");

        // Use BasicQueryTask (concrete helper) — override its nextMatch()/hasMoreMatches()
        return new BasicQueryTask(as, pc, rq, keys) {
            private final Iterator<Attributes> it = results.iterator();

            @Override
            protected boolean hasMoreMatches() throws DicomServiceException {
                return it.hasNext();
            }

            @Override
            protected Attributes nextMatch() throws DicomServiceException {
                if (!it.hasNext()) {
                    throw new NoSuchElementException();
                }
                // Optionally call adjust(...) if you need to transform the match
                return it.next();
            }
        };
    }
}
