package de.botshield;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.logging.Logger;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

/**
 * Klasse zum Schreiben des Twitter Datenmodells in eine relationale Datenbank
 * (Schema scripts/tabellendefinitionen.sql)
 *
 * @author jstrebel
 *
 */

public class PGDBConnection {
    private Connection db;

    private PreparedStatement stInsStatus;
    private PreparedStatement stInsUser;
    private PreparedStatement stInsURL;
    private PreparedStatement stInsPlace;
    private PreparedStatement stInsEntity;
    private PreparedStatement stInsHashtag;
    private PreparedStatement stInsGeoLoc;
    private PreparedStatement stInsUserMention;
    private PreparedStatement stInsMedia;
    private final static Logger objLogger = Logger.getLogger("de.botshield.dataCollector.PGDBConnection");

    /**
     * Establishes a DB connection to a PostgreSQL DB.
     *
     * @param strUser
     *            the username
     * @param strPW
     *            the password
     * @param strDB
     *            the DB name
     * @return true for success, false for failure
     */
    public boolean establishConnection(String strUser, String strPW, String strDB) {
        String url = "jdbc:postgresql:" + strDB;

        try {
            Class.forName("org.postgresql.Driver");
            objLogger.info("Trying to get connection to db with URL " + url);
            db = DriverManager.getConnection(url, strUser, strPW);

            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery("SELECT count(*) FROM datacollector.T_User");

            rs.close();
            st.close();

            db.setAutoCommit(false);

            return true;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return false;
        }
    }

    /**
     * Prepare the statements that are used in the method "insertStatus".
     *
     * @throws SQLException
     */
    public void prepareStatements() throws SQLException {
        String strInsStatus = "insert into datacollector.T_Status(ID,status_Text,created_at, "
                + "favourites_count,username,screen_name,lang,withheld_in_countries,"
                + "InReplyToScreenName,InReplyToStatusId,"
                + "InReplyToUserId,quoted_status_id,RetweetCount,retweeted_status_id,status_source,isFavorited,"
                + "isPossiblySensitive,isRetweet,isRetweeted,isRetweetedByMe,isTruncated,recorded_at,"
                + "status_user_id,latitude,longitude,status_place_id,URLEntities_id,HashtagEntities_id,"
                + "UserMentionEntities_id,DataCollSession_id, MediaEntities_id) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        String strInsUser = "insert into datacollector.T_User(ID,recorded_at,"
                + "username,screen_name,created_at,description,"
                + "geo_enabled,lang,followers_count,favourites_count,friends_count, listed_count,loca,"
                + "statuses_count,TimeZone,user_URL,UtcOffset,WithheldInCountries,isContributorsEnabled,"
                + "isDefaultProfile,isDefaultProfileImage,isFollowRequestSent,isProfileBackgroundTiled,"
                + "isProfileUseBackgroundImage,isProtected,isTranslator,isverified,URLEntity_id,DescURLEntity_id,DataCollSession_id) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        String strInsURL = "insert into datacollector.T_URL(ID,display_url,expanded_url,"
                + "indices_start,indices_end,url,urltext,entity_id) values (?,?,?,?,?,?,?,?)";

        String strInsPlace = "insert into datacollector.T_Place(ID,pname,pfullname,place_url,bb_type,geo_type,country,country_code,"
                + "place_type,street_address,contained_place_id) values (?,?,?,?,?,?,?,?,?,?,?)";

        String strInsEntity = "insert into datacollector.T_Entity(ID) values (?)";

        String strInsHashtag = "insert into datacollector.T_Hashtag(ID,indices_start,indices_end,httext,entity_id) values (?,?,?,?,?)";

        String strInsUserMention = "insert into datacollector.T_User_Mention(ID,user_id,indices_start,indices_end,"
                + "username,screen_name,umtext,entity_id) values (?,?,?,?,?,?,?,?)";

        String strInsMedia = "insert into datacollector.T_Media(ID,media_url,media_url_https,media_type,display_url,expanded_url,"
                + "indices_start,indices_end,url,urltext,sizes,entity_id) values (?,?,?,?,?,?,?,?,?,?,?,?)";

        /*
         * Code aus twitter4j/twitter4j-core/src/internal-json/java/twitter4j/
         * JSONImplFactory .java, um zu verstehen, wie die das zweidimensionale
         * GeoLocation-Array der BoundingBox in Place aufgebaut ist
         *
         * static GeoLocation[][] coordinatesAsGeoLocationArray(JSONArray
         * coordinates) throws TwitterException { try { GeoLocation[][]
         * boundingBox = new GeoLocation[coordinates.length()][]; for (int i =
         * 0; i < coordinates.length(); i++) { JSONArray array =
         * coordinates.getJSONArray(i); boundingBox[i] = new
         * GeoLocation[array.length()]; for (int j = 0; j < array.length(); j++)
         * { JSONArray coordinate = array.getJSONArray(j); boundingBox[i][j] =
         * new GeoLocation(coordinate.getDouble(1), coordinate.getDouble(0)); }
         * } return boundingBox; } catch (JSONException jsone) { throw new
         * TwitterException(jsone); } }
         */
        String strInsGeoLoc = "insert into datacollector.T_Geolocation(ID,latitude, longitude, bboxcoord_place_id, geocoord_place_id)"
                + " values (?,?,?,?,?)";

        stInsPlace = db.prepareStatement(strInsPlace);
        stInsURL = db.prepareStatement(strInsURL);
        stInsUser = db.prepareStatement(strInsUser);
        stInsStatus = db.prepareStatement(strInsStatus);
        stInsEntity = db.prepareStatement(strInsEntity);
        stInsHashtag = db.prepareStatement(strInsHashtag);
        stInsMedia = db.prepareStatement(strInsMedia);
        stInsGeoLoc = db.prepareStatement(strInsGeoLoc);
        stInsUserMention = db.prepareStatement(strInsUserMention);

    }

    /**
     * Retrieves a unique data collection session id
     *
     * @param strSeqName
     *            the name of the sequence
     * @return new id from the sequence or -1 in error case
     */
    public long selectIDfromSequence(String strSeqName) {
        long lID = -1;
        try {
            // hole Sequenznummer
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery("select nextval('" + strSeqName + "')");
            rs.next();
            lID = rs.getLong(1);
            rs.close();
            st.close();
            return lID;

        } catch (SQLException e) {
            System.err.println(e.toString());
            return lID;
        }
    }

    /**
     * Method for writing the data collection parameters to the database. It
     * creates a single entry for each collection session.
     *
     * @param strTopics
     *            the topics used for filtering
     * @param strDatasource
     *            REST or Streaming API
     * @return parameter ID
     */
    public int registerDataCollectionParameter(long lSessionID, long[] followArray, String[] trackArray,
            String strDatasource) {
        PreparedStatement stInsDCPARAM = null;
        String strInsDCPARAM = "insert into datacollector.T_DataCollParameter(ID,track_topics,track_followers,datasource) "
                + "values (?,?,?,?)";
        StringBuilder sb = new StringBuilder("");
        StringBuilder sbfollow = new StringBuilder("");
        String strTopics = "";
        String strFollowers = "";
        try {
            stInsDCPARAM = db.prepareStatement(strInsDCPARAM);

            stInsDCPARAM.setLong(1, lSessionID);

            if (trackArray != null)
                if (trackArray.length > 0) {

                    for (String strelem : trackArray) {
                        sb.append(strelem.trim());
                        sb.append(",");
                    }

                    // remove trailing comma
                    if (sb.length() > 4000) {
                        strTopics = sb.substring(0, sb.length() - 1).substring(0, 3999).toString();
                    } else {
                        strTopics = sb.substring(0, sb.length() <= 1 ? 0 : sb.length() - 1).toString();
                    }
                }

            stInsDCPARAM.setString(2, strTopics);

            if (followArray != null)
                if (followArray.length > 0) {
                    for (long lelem : followArray) {
                        sbfollow.append(Long.toString(lelem).trim());
                        sbfollow.append(",");
                    }
                    // remove trailing comma and check for field length
                    // constraint
                    if (sb.length() > 4000) {
                        strFollowers = sbfollow.substring(0, sbfollow.length() <= 1 ? 0 : sbfollow.length() - 1)
                                .substring(0, 3999).toString();
                    } else {
                        strFollowers = sbfollow.substring(0, sbfollow.length() <= 1 ? 0 : sbfollow.length() - 1)
                                .toString();
                    }
                }

            stInsDCPARAM.setString(3, strFollowers);
            stInsDCPARAM.setString(4, strDatasource);

            stInsDCPARAM.executeUpdate();
            db.commit();
            stInsDCPARAM.close();

            return 0;
        } catch (SQLException e) {

            System.err.println(e.toString());
            if (db != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    db.rollback();
                    if (stInsDCPARAM != null)
                        stInsDCPARAM.close();
                } catch (SQLException excep) {
                    System.err.println(excep.toString());
                }
            } // if

            return -1;
        }
    }

    /**
     * Inserts Status object into database. Extended Media Entities are not
     * supported.
     *
     * @param twStatus
     *            The Twitter4J Status interface
     * @param lSessionID
     *            data collection session
     * @return 0 for success, -1 for failure
     */
    public int insertStatus(Status twStatus, long lSessionID) {

        Timestamp tsrecorded_at = new Timestamp(Calendar.getInstance().getTimeInMillis());

        long lURLEntityid = -1;
        long lDescURLEntityid = -1; // User Description URL Entitites
        long lPlaceid = -1;
        long lStatusURLEntitiesid = -1;
        long lStatusHashtagEntitiesid = -1;
        long lStatusUserMentionEntitiesid = -1;
        long lStatusMediaEntitiesid = -1;

        try {
            // Schreibe Place Objekt
            if (twStatus.getPlace() != null) {
                lPlaceid = insertPlaceIntoDb(twStatus.getPlace());
            }

            // schreibe URL-Objekt
            // User's URL
            // leider kann es das URLEntity-Objekt geben, ohne dass dessen
            // Felder gefuellt sind.
            if (twStatus.getUser() != null) {
                if (twStatus.getUser().getURLEntity() != null
                        && !twStatus.getUser().getURLEntity().getURL().isEmpty()) {
                    lURLEntityid = insertUserUrlEntityIntoDb(twStatus.getUser().getURLEntity());
                }

                // User Description URL Entities
                if (twStatus.getUser().getDescriptionURLEntities() != null) {
                    lDescURLEntityid = insertUrlEntitiesIntoDb(twStatus.getUser().getDescriptionURLEntities());
                }

                // schreibe User-Objekt
                insertUserIntoDb(twStatus.getUser(), tsrecorded_at, lURLEntityid, lDescURLEntityid, lSessionID);

            }
            // schreibe Status URL Entities
            if (twStatus.getURLEntities() != null) {
                lStatusURLEntitiesid = insertUrlEntitiesIntoDb(twStatus.getURLEntities());
            }

            // schreibe Status Hashtag Entities
            if (twStatus.getHashtagEntities() != null) {
                lStatusHashtagEntitiesid = insertHashtagEntitiesIntoDb(twStatus.getHashtagEntities());
            }

            // schreibe UserMention Entities
            if (twStatus.getUserMentionEntities() != null) {
                lStatusUserMentionEntitiesid = insertUserMentionEntitiesIntoDb(twStatus.getUserMentionEntities());
            }

            // schreibe Status Media Entities
            if (twStatus.getMediaEntities() != null) {
                lStatusMediaEntitiesid = insertMediaEntitiesIntoDb(twStatus.getMediaEntities());
            }

            // schreibe Status-objekt
            insertStatusIntoDb(twStatus, tsrecorded_at, lPlaceid, lStatusURLEntitiesid, lStatusHashtagEntitiesid,
                    lStatusUserMentionEntitiesid, lSessionID, lStatusMediaEntitiesid);

            db.commit();
            return 0;

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println(e.getSQLState());
            if (db != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    db.rollback();
                    // hier kein Statement.close, da eine einzelne SQLException
                    // sonst alle zukuenftigen Schreibprozess unmoeglich macht
                } catch (SQLException excep) {
                    System.err.println(excep.toString());
                }
            } // if
            return -1;
        } // catch
    }// insertStatus

    /**
     * Symbol Entities, Extended Entites werden für das MVP erstmal nicht
     * betrachtet.
     *
     * @param twStatus
     * @param tsrecorded_at
     * @param lPlaceid
     * @param lStatusURLEntitiesid
     * @param lStatusHashtagEntitiesid
     * @throws SQLException
     */
    private void insertStatusIntoDb(Status twStatus, Timestamp tsrecorded_at, long lPlaceid, long lStatusURLEntitiesid,
            long lStatusHashtagEntitiesid, long lStatusUserMentionEntitiesid, long lSessionID,
            long lStatusMediaEntitiesid) throws SQLException {
        stInsStatus.setLong(1, twStatus.getId());
        stInsStatus.setString(2, twStatus.getText());
        stInsStatus.setTimestamp(3, new Timestamp(twStatus.getCreatedAt().getTime()));
        stInsStatus.setInt(4, twStatus.getFavoriteCount());
        stInsStatus.setString(5, twStatus.getUser().getName());
        stInsStatus.setString(6, twStatus.getUser().getScreenName());
        stInsStatus.setString(7, twStatus.getLang());

        if (twStatus.getWithheldInCountries() != null) {
            stInsStatus.setString(8, twStatus.getWithheldInCountries().toString());
        } else {
            stInsStatus.setNull(8, Types.VARCHAR);
        }
        stInsStatus.setString(9, twStatus.getInReplyToScreenName());
        stInsStatus.setLong(10, twStatus.getInReplyToStatusId());
        stInsStatus.setLong(11, twStatus.getInReplyToUserId());
        stInsStatus.setLong(12, twStatus.getQuotedStatusId());
        stInsStatus.setInt(13, twStatus.getRetweetCount());
        if (twStatus.getRetweetedStatus() != null) {
            stInsStatus.setLong(14, twStatus.getRetweetedStatus().getId());
        } else {
            stInsStatus.setNull(14, Types.BIGINT);
        }
        stInsStatus.setString(15, twStatus.getSource());
        stInsStatus.setInt(16, (twStatus.isFavorited() ? 1 : 0));
        stInsStatus.setInt(17, (twStatus.isPossiblySensitive() ? 1 : 0));
        stInsStatus.setInt(18, (twStatus.isRetweet() ? 1 : 0));
        stInsStatus.setInt(19, (twStatus.isRetweeted() ? 1 : 0));
        stInsStatus.setInt(20, (twStatus.isRetweetedByMe() ? 1 : 0));
        stInsStatus.setInt(21, (twStatus.isTruncated() ? 1 : 0));
        stInsStatus.setTimestamp(22, tsrecorded_at);

        if (twStatus.getUser() != null) {
            stInsStatus.setLong(23, twStatus.getUser().getId());
        } else {
            stInsStatus.setNull(23, Types.BIGINT);
        }

        // Geolocation des Tweets
        if (twStatus.getGeoLocation() != null) {
            stInsStatus.setDouble(24, twStatus.getGeoLocation().getLatitude());
            stInsStatus.setDouble(25, twStatus.getGeoLocation().getLongitude());
        } else {
            stInsStatus.setNull(24, Types.DOUBLE);
            stInsStatus.setNull(25, Types.DOUBLE);
        }
        // Place
        if (lPlaceid != -1) {
            stInsStatus.setLong(26, lPlaceid);
        } else {
            stInsStatus.setNull(26, Types.BIGINT);
        }

        // URL Entities
        if (lStatusURLEntitiesid != -1) {
            stInsStatus.setLong(27, lStatusURLEntitiesid);
        } else {
            stInsStatus.setNull(27, Types.BIGINT);
        }

        // Hashtag Entities
        if (lStatusHashtagEntitiesid != -1) {
            stInsStatus.setLong(28, lStatusHashtagEntitiesid);
        } else {
            stInsStatus.setNull(28, Types.BIGINT);
        }

        // UserMentionEntities
        if (lStatusUserMentionEntitiesid != -1) {
            stInsStatus.setLong(29, lStatusUserMentionEntitiesid);
        } else {
            stInsStatus.setNull(29, Types.BIGINT);
        }

        // Data Collection Session
        stInsStatus.setLong(30, lSessionID);

        // Media Entities
        if (lStatusMediaEntitiesid != -1) {
            stInsStatus.setLong(31, lStatusMediaEntitiesid);
        } else {
            stInsStatus.setNull(31, Types.BIGINT);
        }

        stInsStatus.executeUpdate();
    }

    /**
     * @param twStatus
     * @return EntityID
     * @throws SQLException
     */
    private long insertUserMentionEntitiesIntoDb(UserMentionEntity[] arrUM) throws SQLException {
        long lStatusUserMentionEntitiesid = -1;
        long lUMid = -1;

        // gibt es ueberhaupt Datensaetze?
        if (arrUM.length > 0) {

            // get Entity ID
            lStatusUserMentionEntitiesid = this.selectIDfromSequence("datacollector.entity_seq");

            // schreibe Entity zuerst
            stInsEntity.setLong(1, lStatusUserMentionEntitiesid);
            stInsEntity.executeUpdate();

            for (UserMentionEntity elem : arrUM) {
                // hole Sequenznummer fuer die UserMention
                lUMid = this.selectIDfromSequence("datacollector.usermention_seq");

                // dann schreibe UserMention
                stInsUserMention.setLong(1, lUMid);
                stInsUserMention.setLong(2, elem.getId());
                stInsUserMention.setInt(3, elem.getStart());
                stInsUserMention.setInt(4, elem.getEnd());
                stInsUserMention.setString(5, elem.getName());
                stInsUserMention.setString(6, elem.getScreenName());
                stInsUserMention.setString(7, elem.getText());
                stInsUserMention.setLong(8, lStatusUserMentionEntitiesid);
                stInsUserMention.executeUpdate();

            }

        } else {
            lStatusUserMentionEntitiesid = -1;
        }
        return lStatusUserMentionEntitiesid;
    }

    /**
     * @param twStatus
     * @return
     * @throws SQLException
     */
    private long insertMediaEntitiesIntoDb(MediaEntity[] arrMedia) throws SQLException {
        long lStatusMediaEntityID = -1;
        long lMediaid = -1;

        // gibt es ueberhaupt Datensaetze?
        if (arrMedia.length > 0) {

            // get Entity ID
            lStatusMediaEntityID = this.selectIDfromSequence("datacollector.entity_seq");

            // schreibe Entity zuerst
            stInsEntity.setLong(1, lStatusMediaEntityID);
            stInsEntity.executeUpdate();

            for (MediaEntity elem : arrMedia) {
                // hole Sequenznummer fuer die Media
                lMediaid = this.selectIDfromSequence("datacollector.media_seq");

                // dann schreibe Media

                stInsMedia.setLong(1, lMediaid);
                stInsMedia.setString(2, elem.getMediaURL());
                stInsMedia.setString(3, elem.getMediaURLHttps());
                stInsMedia.setString(4, elem.getType());
                stInsMedia.setString(5, elem.getDisplayURL());
                stInsMedia.setString(6, elem.getExpandedURL());
                stInsMedia.setInt(7, elem.getStart());
                stInsMedia.setInt(8, elem.getEnd());
                stInsMedia.setString(9, elem.getURL());
                stInsMedia.setString(10, elem.getText());
                if (elem.getSizes() != null) {
                    stInsMedia.setString(11, elem.getSizes().toString());
                } else {
                    stInsMedia.setString(11, "");
                }

                stInsMedia.setLong(12, lStatusMediaEntityID);
                stInsMedia.executeUpdate();

            }

        } else {
            lStatusMediaEntityID = -1;
        }
        return lStatusMediaEntityID;
    }

    /**
     * @param twStatus
     * @return
     * @throws SQLException
     */
    private long insertHashtagEntitiesIntoDb(HashtagEntity[] arrHT) throws SQLException {
        long lStatusHashtagEntitiesid = -1;
        long lHTid = -1;

        // gibt es ueberhaupt Datensaetze?
        if (arrHT.length > 0) {

            // get Entity ID
            lStatusHashtagEntitiesid = this.selectIDfromSequence("datacollector.entity_seq");

            // schreibe Entity zuerst
            stInsEntity.setLong(1, lStatusHashtagEntitiesid);
            stInsEntity.executeUpdate();

            for (HashtagEntity elem : arrHT) {
                // hole Sequenznummer fuer die URL
                lHTid = this.selectIDfromSequence("datacollector.hashtag_seq");

                // dann schreibe URL
                stInsHashtag.setLong(1, lHTid);
                stInsHashtag.setInt(2, elem.getStart());
                stInsHashtag.setInt(3, elem.getEnd());
                stInsHashtag.setString(4, elem.getText());
                stInsHashtag.setLong(5, lStatusHashtagEntitiesid);
                stInsHashtag.executeUpdate();

            }

        } else {
            lStatusHashtagEntitiesid = -1;
        }
        return lStatusHashtagEntitiesid;
    }

    /**
     * @param twStatus
     * @return
     * @throws SQLException
     */
    private long insertUrlEntitiesIntoDb(URLEntity[] arrURL) throws SQLException {
        long lURLEntitiesId = -1;
        long lURLid = -1;

        // gibt es ueberhaupt Datensaetze?
        if (arrURL.length > 0) {

            // get Entity ID
            lURLEntitiesId = this.selectIDfromSequence("datacollector.entity_seq");

            // schreibe Entity zuerst
            stInsEntity.setLong(1, lURLEntitiesId);
            stInsEntity.executeUpdate();

            for (URLEntity elem : arrURL) {
                // hole Sequenznummer fuer die URL
                lURLid = this.selectIDfromSequence("datacollector.url_seq");

                // dann schreibe URL
                stInsURL.setLong(1, lURLid);
                stInsURL.setString(2, elem.getDisplayURL());
                stInsURL.setString(3, elem.getExpandedURL());
                stInsURL.setInt(4, elem.getStart());
                stInsURL.setInt(5, elem.getEnd());
                stInsURL.setString(6, elem.getURL());
                stInsURL.setString(7, elem.getText());
                stInsURL.setLong(8, lURLEntitiesId);

                stInsURL.executeUpdate();
            }

        } else {
            lURLEntitiesId = -1;
        }
        return lURLEntitiesId;
    }

    /**
     * @param twStatus
     * @param tsrecorded_at
     * @param lURLEntityid
     * @param lDescURLEntityid
     * @throws SQLException
     */
    private void insertUserIntoDb(User twUser, Timestamp tsrecorded_at, long lURLEntityid, long lDescURLEntityid,
            long lSessionID) throws SQLException {
        stInsUser.setLong(1, twUser.getId());
        stInsUser.setTimestamp(2, tsrecorded_at);
        stInsUser.setString(3, twUser.getName());
        stInsUser.setString(4, twUser.getScreenName());
        stInsUser.setTimestamp(5, new Timestamp(twUser.getCreatedAt().getTime()));
        stInsUser.setString(6, twUser.getDescription());
        stInsUser.setInt(7, (twUser.isGeoEnabled() ? 1 : 0));
        stInsUser.setString(8, twUser.getLang());
        stInsUser.setInt(9, twUser.getFollowersCount());
        stInsUser.setInt(10, twUser.getFavouritesCount());
        stInsUser.setInt(11, twUser.getFriendsCount());
        stInsUser.setInt(12, twUser.getListedCount());
        stInsUser.setString(13, twUser.getLocation());
        stInsUser.setInt(14, twUser.getStatusesCount());
        stInsUser.setString(15, twUser.getTimeZone());
        stInsUser.setString(16, twUser.getURL());

        stInsUser.setInt(17, twUser.getUtcOffset());

        if (twUser.getWithheldInCountries() != null) {
            stInsUser.setString(18, twUser.getWithheldInCountries().toString());
        } else {
            stInsUser.setNull(18, Types.VARCHAR);
        }
        stInsUser.setInt(19, (twUser.isContributorsEnabled() ? 1 : 0));
        stInsUser.setInt(20, (twUser.isDefaultProfile() ? 1 : 0));
        stInsUser.setInt(21, (twUser.isDefaultProfileImage() ? 1 : 0));
        stInsUser.setInt(22, (twUser.isFollowRequestSent() ? 1 : 0));
        stInsUser.setInt(23, (twUser.isProfileBackgroundTiled() ? 1 : 0));
        stInsUser.setInt(24, (twUser.isProfileUseBackgroundImage() ? 1 : 0));
        stInsUser.setInt(25, (twUser.isProtected() ? 1 : 0));
        stInsUser.setInt(26, (twUser.isTranslator() ? 1 : 0));
        stInsUser.setInt(27, (twUser.isVerified() ? 1 : 0));
        if (lURLEntityid != -1) {
            stInsUser.setLong(28, lURLEntityid);
        } else {
            stInsUser.setNull(28, Types.BIGINT);
        }
        if (lDescURLEntityid != -1) {
            stInsUser.setLong(29, lDescURLEntityid);
        } else {
            stInsUser.setNull(29, Types.BIGINT);
        }
        stInsUser.setLong(30, lSessionID);

        stInsUser.executeUpdate();
    }

    /**
     * @param twStatus
     * @return
     * @throws SQLException
     */
    private long insertUserUrlEntityIntoDb(URLEntity twURL) throws SQLException {
        long lURLEntityid = -1;
        long lURLid = -1;

        // hole Sequenznummer fuer die URL
        Statement st = db.createStatement();
        ResultSet rs = st.executeQuery("select nextval('datacollector.url_seq'),nextval('datacollector.entity_seq')");
        rs.next();
        lURLid = rs.getLong(1);
        lURLEntityid = rs.getLong(2);
        rs.close();
        st.close();

        // schreibe Entity zuerst
        stInsEntity.setLong(1, lURLEntityid);
        stInsEntity.executeUpdate();

        // dann schreibe URL
        stInsURL.setLong(1, lURLid);
        stInsURL.setString(2, twURL.getDisplayURL());
        stInsURL.setString(3, twURL.getExpandedURL());
        stInsURL.setInt(4, twURL.getStart());
        stInsURL.setInt(5, twURL.getEnd());
        stInsURL.setString(6, twURL.getURL());
        stInsURL.setString(7, twURL.getText());
        stInsURL.setLong(8, lURLEntityid);

        stInsURL.executeUpdate();
        return lURLEntityid;
    }

    /**
     * @param twStatus
     * @return
     * @throws SQLException
     */
    private long insertPlaceIntoDb(Place twPlace) throws SQLException {
        long lPlaceid = -1;
        long lGeoLocid;

        // hole Sequenznummer fuer den Place
        lPlaceid = this.selectIDfromSequence("datacollector.place_seq");

        // setze Parameter
        stInsPlace.setLong(1, lPlaceid);
        stInsPlace.setString(2, twPlace.getName());
        stInsPlace.setString(3, twPlace.getFullName());
        stInsPlace.setString(4, twPlace.getURL());
        stInsPlace.setString(5, twPlace.getBoundingBoxType());
        stInsPlace.setString(6, twPlace.getGeometryType());
        stInsPlace.setString(7, twPlace.getCountry());
        stInsPlace.setString(8, twPlace.getCountryCode());
        stInsPlace.setString(9, twPlace.getPlaceType());
        stInsPlace.setString(10, twPlace.getStreetAddress());
        // TODO: rekursive Aufloesung verschachtelter Places
        // nicht wichtig, da Place sowieso nicht oft vorkommt
        stInsPlace.setNull(11, Types.BIGINT);

        stInsPlace.executeUpdate();

        // schreibe BoundingBox aus Place
        // Durchlaufe das doppelte Array GeoLocation[0][j] und schreibe
        // die Einzelkoordinaten in die DB
        // Annahme: es gibt nur ein einziges Polygon als Bounding Box
        GeoLocation[][] arrGeo = twPlace.getBoundingBoxCoordinates();
        if (arrGeo != null) {
            insertCoordinatesIntoDb(arrGeo, lPlaceid, true);
        }

        // schreibe Geometry aus Place
        arrGeo = twPlace.getGeometryCoordinates();
        if (arrGeo != null) {
            insertCoordinatesIntoDb(arrGeo, lPlaceid, false);
        }

        return lPlaceid;
    }

    /**
     * Writes Geo coordinates into database
     *
     * @param lPlaceid
     * @param arrGeo
     * @throws SQLException
     */
    private void insertCoordinatesIntoDb(GeoLocation[][] arrGeo, long lPlaceid, boolean isBoundingBox)
            throws SQLException {
        long lGeoLocid = -1;
        GeoLocation[] arrbboxloc = arrGeo[0];
        Statement st1 = db.createStatement();

        // Durchlaufe alle Ecken des Polygons
        for (GeoLocation element : arrbboxloc) {
            // hole id aus Sequence geoloc_seq
            ResultSet rs1 = st1.executeQuery("select nextval('datacollector.geoloc_seq')");
            rs1.next();
            lGeoLocid = rs1.getLong(1);
            rs1.close();

            if (lGeoLocid != -1) {
                // schreibe Attribute
                // ID,latitude, longitude, bboxcoord_place_id,
                // geocoord_place_id
                stInsGeoLoc.setLong(1, lGeoLocid);
                stInsGeoLoc.setDouble(2, element.getLatitude());
                stInsGeoLoc.setDouble(3, element.getLongitude());
                if (isBoundingBox) {
                    stInsGeoLoc.setLong(4, lPlaceid);
                    stInsGeoLoc.setNull(5, Types.BIGINT);
                } else {
                    stInsGeoLoc.setNull(4, Types.BIGINT);
                    stInsGeoLoc.setLong(5, lPlaceid);
                }

                stInsGeoLoc.executeUpdate();
            }
        }
        st1.close();
    }

    public int closeConnection() {
        try {
            if (db != null)
                db.close();
            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return -1;
        }
    }// closeConnection

    /**
     * Clean up on exit
     */
    @Override
    protected void finalize() throws Throwable {
        if (stInsStatus != null) {
            stInsStatus.close();
        }
        if (stInsUser != null) {
            stInsUser.close();
        }
        if (stInsURL != null) {
            stInsURL.close();
        }
        if (stInsPlace != null) {
            stInsPlace.close();
        }
        if (stInsEntity != null) {
            stInsEntity.close();
        }
        if (stInsGeoLoc != null) {
            stInsGeoLoc.close();
        }
        if (stInsHashtag != null) {
            stInsHashtag.close();
        }
        if (stInsMedia != null) {
            stInsMedia.close();
        }
        if (stInsUserMention != null) {
            stInsUserMention.close();
        }
        this.closeConnection();
    }

}// class
