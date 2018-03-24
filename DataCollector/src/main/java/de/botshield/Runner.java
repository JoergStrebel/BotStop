package de.botshield;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;

public class Runner {

    private final static Logger objLogger = Logger
            .getLogger("de.botshield.dataCollector.Runner");
    private final CaptureFilterStream objCapture = new CaptureFilterStream();

    /** Constructor */
    public Runner() {
    }

    public int setup_Runner() {
        try {

            Configuration config = new Configuration();
            long[] followArray = config.getFollowArray();
            String[] trackArray = config.getTrackArray();
            String[] strLanguage = config.getLanguage();

            // Check if database integration is needed
            if (config.isWriteToDb()) {
                String strDBUser = config.getStrDBUser();
                String strDBName = config.getStrDBName();
                String strDBPW = config.getStrDBPW();
                String strSessionID = config.getStrSessionID();

                objCapture.setupConnection(strDBUser, strDBName, strDBPW);
                objCapture.setBlnWritetoDB(true);
                objLogger.info("Write to DB is set to true!");
                // set up data collection session
                objCapture.setupCollectionSession(strSessionID, followArray,
                        trackArray, "streaming");
            } else {
                objCapture.setBlnWritetoDB(false);
                objLogger.info("Write to DB is set to false!");
            }

            // initialize listener and start following
            objCapture.execute(followArray, trackArray, strLanguage);

            // catch SIGINT et al. and shut down the application
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        objCapture.finalize();
                        objLogger.info("Shutdown hook ran!");
                    } catch (Exception ex) {
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (SQLException sqlex) {
            objLogger.severe(sqlex.getSQLState());
            objLogger.severe(sqlex.getMessage());
            System.exit(-1);
        } catch (IOException ioex) {
            objLogger.severe(ioex.getMessage());
            System.exit(-1);
        } catch (Exception ex) {
            objLogger.severe(ex.getMessage());
            System.exit(-1);
        }

        return 0;
    }

    public static void main(String[] args) {
        Runner objRunner = new Runner();
        objRunner.setup_Runner();
    }

}
