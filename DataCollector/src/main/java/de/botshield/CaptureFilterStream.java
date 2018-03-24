package de.botshield;

import java.sql.SQLException;
import java.util.logging.Logger;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Klasse zum Schreiben von Tweets in eine DB basierend auf Schlagwoertern.
 */
public final class CaptureFilterStream {

    private PGDBConnection dbConn = null;
    private boolean blnWritetoDB = false;
    private StatusListener listener = null;
    private TwitterStream twitterStream = null;
    /** data collection session id */
    private long lSessionID = -1;

    private final static Logger objLogger = Logger
            .getLogger("de.botshield.dataCollector.CaptureFilterStream");

    /*
     * Constructor
     */
    public CaptureFilterStream() {

    }

    public void initializeStreamListener() {

        listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (isBlnWritetoDB()) {
                    objLogger
                    .info("*********************** Writing tweet into db! *********************** ");
                    dbConn.insertStatus(status, lSessionID);
                }
            }

            @Override
            public void onDeletionNotice(
                    StatusDeletionNotice statusDeletionNotice) {
                objLogger.warning("Got a status deletion notice id:"
                        + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                objLogger.warning("Got track limitation notice:"
                        + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                objLogger.warning("Got scrub_geo event userId:" + userId
                        + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                objLogger.warning("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                // hier kein closeConnection, da jede SQLException sonst alle
                // zukuenftigen Schreibprozess unmoeglich macht
                // dbConn.closeConnection();
                objLogger.severe(ex.getMessage());
            }
        };
    }

    /**
     * @return false if connection could not be established
     * @throws SQLException
     */
    public boolean setupConnection(String strDBUser, String strDBName,
            String strDBPW) throws SQLException {
        dbConn = new PGDBConnection();
        boolean result = dbConn.establishConnection(strDBUser, strDBPW,
                strDBName);
        if (result) {
            dbConn.prepareStatements();
        }
        return result;
    }

    /**
     * Creates a session entry in the database, or continues with an existing
     * session. Es müssen mehrere parallele Läufe des Datacollectors zu
     * unterschiedlichen Themen unterstützt werden. Dazu muss in der Datenbank
     * eine Session-ID hinterlegt werden, über die alle Einträge des
     * entsprechenden Laufs eindeutig selektiert werden können.
     *
     * Achtung: Session-ID muss vorgegeben werden können, so dass ein
     * Wiederaufsetzen möglich ist. Die Daten des neuen Laufs erweitern somit
     * die Daten des alten Laufs.
     *
     */
    public void setupCollectionSession(String strSessionID, long[] followArray,
            String[] trackArray, String strDatasource) {

        if (!strSessionID.isEmpty()) {
            lSessionID = Long.valueOf(strSessionID).longValue(); // reuse
            // session id
        } else {
            lSessionID = dbConn.selectIDfromSequence("datacollector.param_seq");
            if (trackArray == null && followArray == null) {
                objLogger
                .severe("Keine Stichwörter oder User angegeben - steige aus");
                System.exit(-1);
            }
            // prepare collection session in the DB
            int lret = dbConn.registerDataCollectionParameter(lSessionID,
                    followArray, trackArray, strDatasource);
            if (lret == -1) {
                objLogger
                .severe("Konnte keine Datensammlungssitzung anlegen - steige aus");
                System.exit(1);
            }
        }

        objLogger.info(String.format("Session ID: %d%n", lSessionID));
    }

    public void execute(long[] followArray, String[] trackArray,
            String[] strLanguage) {

        // try to set up new stream, when the listener fails due to HTTP timeout
        // for example

        this.initializeStreamListener();

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        try {
            // filter() method internally creates a thread which manipulates
            // TwitterStream and calls these adequate listener methods
            // continuously.
            twitterStream.filter(new FilterQuery(0, followArray, trackArray,
                    null, strLanguage));
        } catch (Exception ex) {
            twitterStream.removeListener(listener);
            twitterStream.cleanUp();
        }
    }

    public boolean isBlnWritetoDB() {
        return blnWritetoDB;
    }

    public void setBlnWritetoDB(boolean blnWritetoDB) {
        this.blnWritetoDB = blnWritetoDB;
    }

    /**
     * close db connection, stop stream listener
     */
    @Override
    protected void finalize() throws Throwable {
        if (twitterStream != null) {
            twitterStream.removeListener(listener);
            twitterStream.cleanUp();
            twitterStream.clearListeners();
        }

        if (dbConn != null)
            dbConn.finalize();
    }

}
