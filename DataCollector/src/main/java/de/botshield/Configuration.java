package de.botshield;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class is responsible for providing all configuration data.
 *
 * @author Andreas Volz, Jörg Strebel
 *
 */
public class Configuration {
    /**
     * Name of the property file containing the configuration for this project
     */
    private final static String PROPERTY_FILE = "dataCollector.properties";

    /**
     * Look in the current directory for the property file.
     */
    private final static String PATH_TO_PROPERTY_FILE = ".";
    private final static String COMMA_SEPARATOR = ",";

    private final static String PROPERTY_DATABASEINTEGRATION = "WriteToDatabase";
    private final static String PROPERTY_LANGUAGE = "Language";
    private final static String PROPERTY_KEY_FOLLOW = "toBeFollowed";
    private final static String PROPERTY_KEY_TOPICS = "trackTopics";

    private final static String PROPERTY_DATABASENAME = "DatabaseName";
    private final static String PROPERTY_DATABASEUSER = "DatabaseUser";
    private final static String PROPERTY_DATABASEPW = "DatabasePW";
    private final static String PROPERTY_SESSIONID = "SessionID";

    private List<String> screenNames = null;
    private List<Long> userIds = null;
    private long[] followArray;

    private String[] trackArray;

    private boolean writeToDb = false;
    private String strSessionID = "";
    private String strDBUser = "";
    private String strDBName = "";
    private String strDBPW = "";
    private String[] strLanguage = null;

    private final static Logger objLogger = Logger
            .getLogger("de.botshield.dataCollector.Configuration");

    /**
     * Constructor. Reads the properties file and sets the member variables
     * according to the definitions in this properties file.
     *
     * @throws IOException
     */
    public Configuration() throws IOException {
        Properties props = readProperties();
        writeToDb = extractWriteToDb(props);
        followArray = readFollow(props);
        trackArray = readTopics(props);
        strSessionID = this.extractSessionID(props);
        strDBUser = this.extractDBUser(props);
        strDBName = this.extractDBName(props);
        strDBPW = this.extractDBPW(props);
        strLanguage = this.extractLanguage(props);
    }

    /**
     * Reads the properties either from a certain path or from the properties
     * file located in the jar of this application.
     *
     * @return
     * @throws IOException
     */
    private Properties readProperties() throws IOException {
        Properties props = new Properties();

        // Try to find the property file in the file system
        Path filePath = Paths.get(PATH_TO_PROPERTY_FILE, PROPERTY_FILE);
        if (Files.exists(filePath) && Files.isRegularFile(filePath)
                && Files.isReadable(filePath)) {
            objLogger.info("Using property file: " + PATH_TO_PROPERTY_FILE
                    + "/" + PROPERTY_FILE);
            props.load(Files.newInputStream(filePath));
        } else {
            // Take the property file from the project-jar (=>
            // src/main/resources/dataCollector.properties)
            objLogger.info("Using property file from jar file!");
            InputStream is = this.getClass().getClassLoader()
                    .getResourceAsStream(PROPERTY_FILE);
            props.load(is);
        }

        if (props.isEmpty()) {
            throw new RuntimeException(
                    "Property file dataCollector.properties not found! Aborting initialization...");
        }

        return props;
    }

    /**
     * Determine whether the data fetched by this application is to be written
     * into the database or not.
     *
     * @param props
     * @return
     */
    private boolean extractWriteToDb(Properties props) {
        String dbIntegration = props.getProperty(PROPERTY_DATABASEINTEGRATION);
        if (dbIntegration != null && !dbIntegration.trim().isEmpty()) {
            if (dbIntegration.equalsIgnoreCase("true")) {
                return true;
            }
        }

        return false;
    }

    /**
     *
     * @param props
     * @return SessionID or empty string
     */
    private String extractSessionID(Properties props) {
        String strSID = props.getProperty(PROPERTY_SESSIONID).trim();
        if (strSID != null && !strSID.isEmpty()) {
            return strSID;
        } else
            return "";
    }

    private String extractDBUser(Properties props) {
        String strSID = props.getProperty(PROPERTY_DATABASEUSER).trim();
        if (strSID != null && !strSID.isEmpty()) {
            return strSID;
        } else
            return "";
    }

    private String extractDBName(Properties props) {
        String strSID = props.getProperty(PROPERTY_DATABASENAME).trim();
        if (strSID != null && !strSID.isEmpty()) {
            return strSID;
        } else
            return "";
    }

    private String extractDBPW(Properties props) {
        String strSID = props.getProperty(PROPERTY_DATABASEPW).trim();
        ;
        if (strSID != null && !strSID.isEmpty()) {
            return strSID;
        } else
            return "";
    }

    public List<Long> getUserIds() {
        return userIds;
    }

    public List<String> getScreenNames() {
        return screenNames;
    }

    public boolean isWriteToDb() {
        return writeToDb;
    }

    public long[] getFollowArray() {
        return followArray;
    }

    public String[] getTrackArray() {
        return trackArray;
    }

    /**
     * Extracts the topics that are to be tracked from a property file. The
     * topics are expected to be contained in the passed "props" as a
     * comma-separated list of Strings.
     *
     * @see <a
     *      href="https://dev.twitter.com/streaming/overview/request-parameters#track">
     *      Twitter API doc for tracking parameters</a>
     *
     * @param props
     *            Contains the topics to be tracked.
     * @return The topics to be tracked as an array of String.
     */
    private String[] readTopics(Properties props) {

        String[] trackArray = null;
        String[] topicsArray = null;

        String topicsToBeTracked = props.getProperty(PROPERTY_KEY_TOPICS);
        if (topicsToBeTracked != null && !topicsToBeTracked.trim().isEmpty()) {
            // comma-separated substrings are handled by Twitter API String[]
            topicsArray = topicsToBeTracked.split(COMMA_SEPARATOR);

            // https://dev.twitter.com/streaming/reference/post/statuses/filter
            // The default access level allows up to 400 track keywords, 5,000
            // follow userids and 25 0.1-360 degree location boxes.
            // Each phrase must be between 1 and 60 bytes, inclusive.
            trackArray = new String[Math.min(400, topicsArray.length)];
            for (int n = 0; n < Math.min(400, topicsArray.length); n++) {
                String topicToBeTracked = topicsArray[n].trim();
                trackArray[n] = topicToBeTracked.trim();
            }

        }
        return trackArray;
    }

    /**
     * Extracts the users that are to be followed from a property file. The
     * users are expected to be contained in the passed "props" as
     * comma-separated list of longs.
     *
     * @param props
     *            Contains the users to be followed.
     * @return The users to be followed as an array of long.
     */
    private long[] readFollow(Properties props) {
        long[] followArray = null;
        String usersToBeFollowed = props.getProperty(PROPERTY_KEY_FOLLOW);

        if (usersToBeFollowed != null && !usersToBeFollowed.trim().isEmpty()) {
            String[] usersArray = usersToBeFollowed.split(COMMA_SEPARATOR);
            followArray = new long[Math.min(5000, usersArray.length)];
            for (int n = 0; n < Math.min(5000, usersArray.length); n++) {
                String userToBeFollowed = usersArray[n].trim();
                followArray[n] = Long.parseLong(userToBeFollowed);
            }
        }
        return followArray;
    }

    public String getStrSessionID() {
        return strSessionID;
    }

    public String getStrDBUser() {
        return strDBUser;
    }

    public String getStrDBName() {
        return strDBName;
    }

    public String getStrDBPW() {
        return strDBPW;
    }

    /**
     * Get the target language for the tracking topics
     *
     * @param props
     * @return language identifier or empty string
     */
    private String[] extractLanguage(Properties props) {
        String strLanguage = props.getProperty(PROPERTY_LANGUAGE);
        // TODO: enable more than one language
        if (strLanguage != null && !strLanguage.isEmpty()) {
            String[] strL = new String[1];
            strL[0] = strLanguage.trim();
            return strL;
        } else
            return null;
    }

    public String[] getLanguage() {
        return strLanguage;
    }

}
