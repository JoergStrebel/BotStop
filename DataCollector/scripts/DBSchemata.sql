/* SQL Statements, um die DB-Struktur der Datenbank twitter zu definieren */

/* als user postgres */
CREATE ROLE dbuser LOGIN;
CREATE SCHEMA IF NOT EXISTS DataCollector AUTHORIZATION dbUser;
CREATE SCHEMA IF NOT EXISTS TimeLine AUTHORIZATION dbUser;

/* verschiebe die Objekte aus Schema public in Schema DataCollector und stelle sicher, dass sie den richtigen owner haben */

ALTER TABLE IF EXISTS T_User owner to dbuser;
ALTER TABLE IF EXISTS T_Attribut owner to dbuser;
ALTER TABLE IF EXISTS T_Hashtag owner to dbuser;
ALTER TABLE IF EXISTS T_Symbol owner to dbuser;
ALTER TABLE IF EXISTS T_URL owner to dbuser;
ALTER TABLE IF EXISTS T_User_Mention owner to dbuser;
ALTER TABLE IF EXISTS T_MediaEntitySize owner to dbuser;
ALTER TABLE IF EXISTS T_Media owner to dbuser;
ALTER TABLE IF EXISTS T_Entity owner to dbuser;
ALTER TABLE IF EXISTS T_Place owner to dbuser;
ALTER TABLE IF EXISTS T_Status owner to dbuser;
ALTER TABLE IF EXISTS T_DataCollParameter owner to dbuser;
ALTER TABLE IF EXISTS T_Geolocation owner to dbuser;


ALTER TABLE IF EXISTS T_User SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Attribut SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Hashtag SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Symbol SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_URL SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_User_Mention SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_MediaEntitySize SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Media SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Entity SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Place SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Status SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_DataCollParameter SET SCHEMA DataCollector;
ALTER TABLE IF EXISTS T_Geolocation SET SCHEMA DataCollector;

ALTER VIEW IF EXISTS V_USER_ALLKPI owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_ALLPOSTS owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_PROFILEAGE owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_NAMDESCLEN owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_ANZFOLLOW owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_TWEETMENTION owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_STATUSFREQUENCY  owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_REACTIONTIME owner to dbuser;
ALTER VIEW IF EXISTS V_USER_KPI_DECILPOSTS owner to dbuser;
ALTER VIEW IF EXISTS V_USER_TOTALBOTSCORE owner to dbuser;

ALTER VIEW IF EXISTS V_USER_ALLKPI SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_ALLPOSTS SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_PROFILEAGE SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_NAMDESCLEN SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_ANZFOLLOW SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_TWEETMENTION SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_STATUSFREQUENCY  SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_REACTIONTIME SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_KPI_DECILPOSTS SET SCHEMA DataCollector;
ALTER VIEW IF EXISTS V_USER_TOTALBOTSCORE SET SCHEMA DataCollector;

ALTER SEQUENCE IF EXISTS url_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS param_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS place_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS geoloc_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS entity_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS hashtag_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS usermention_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS media_seq owner to dbuser;

ALTER SEQUENCE IF EXISTS url_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS param_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS place_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS geoloc_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS entity_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS hashtag_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS usermention_seq SET SCHEMA DataCollector;
ALTER SEQUENCE IF EXISTS media_seq SET SCHEMA DataCollector;


/* lege die Objekte im DB-Schema TimeLine an und setze den richtigen Owner 


*/

set schema 'TimeLine';

DROP SEQUENCE IF EXISTS 
timeline.url_seq,
timeline.param_seq,
timeline.place_seq,
timeline.geoloc_seq,
timeline.entity_seq,
timeline.hashtag_seq,
timeline.usermention_seq,
timeline.media_seq CASCADE;

CREATE SEQUENCE timeline.url_seq; -- eine Sequence, um die URLs eines Tweets mit einer Nummer auszustatten.
CREATE SEQUENCE  timeline.param_seq; -- eine Sequence für die IDs einer Datensammel-Sitzung.
CREATE SEQUENCE  timeline.place_seq; -- eine Sequence für die IDs eines Place.
CREATE SEQUENCE  timeline.geoloc_seq; -- eine Sequence für die IDs einer Geolocation.
CREATE SEQUENCE  timeline.entity_seq; -- eine Sequence für die IDs einer Entity.
CREATE SEQUENCE  timeline.hashtag_seq; -- eine Sequence für die IDs eines Hashtags.
CREATE SEQUENCE  timeline.usermention_seq; -- eine Sequence für die IDs einer UserMention.
CREATE SEQUENCE  timeline.media_seq; -- eine Sequence für die IDs eines Hashtags.

DROP TABLE IF EXISTS 
timeline.T_Attribut, 
timeline.T_Hashtag, 
timeline.T_Symbol, 
timeline.T_URL, 
timeline.T_User_Mention, 
timeline.T_MediaEntitySize, 
timeline.T_Media, 
timeline.T_Entity, 
timeline.T_Place, 
timeline.T_Status, 
timeline.T_User,
timeline.T_DataCollParameter, 
timeline.T_Geolocation,
timeline.t_source_qualification,
timeline.T_CHALLENGE_USER cascade;


CREATE TABLE  timeline.T_DataCollParameter --Data Collector Parameters
(
 ID bigint PRIMARY KEY,
 track_topics  VARCHAR (4000),
 track_followers  VARCHAR (4000),
 datasource VARCHAR (4000) -- streaming or rest 
);

/*wird für den MVP nicht befüllt*/
CREATE  TABLE  timeline.T_Attribut
(
 ID bigint PRIMARY KEY,
 KEY   VARCHAR (4000) ,
 Value VARCHAR (4000) 
);

-- Ein Place kann Teil eines anderen Place sein
-- RateLimitStatus habe ich als Meta-Datum der TwitterResponse ausgeschlossen
CREATE  TABLE   timeline.T_Place
(
    ID   bigint PRIMARY KEY,
    pname VARCHAR (100) ,
    pfullname VARCHAR (4000) ,
    place_url  VARCHAR (4000) ,
    bb_type VARCHAR (4000),
    geo_type VARCHAR (4000),
    country VARCHAR (4000),
    country_code VARCHAR (4000),  
    place_type VARCHAR (4000),
    street_address VARCHAR (4000),    
    contained_place_id  bigint REFERENCES timeline.T_Place(ID)
);

-- Annahme: es gibt nur ein Polygon pro BoundingBox und Geometry in Place
CREATE  TABLE  timeline.T_Geolocation
(
 ID bigint PRIMARY KEY,
 latitude double precision,
 longitude double precision,
 bboxcoord_place_id  bigint REFERENCES timeline.T_Place(ID),
 geocoord_place_id  bigint REFERENCES timeline.T_Place(ID)
);


/* Noch ohne ExtendedMedia
* Tabelle für die Klasse Entity, deren abgeleitete Klassen dann 
* ExtendedMediaEntity, HashtagEntity, MediaEntity, SymbolEntity, URLEntity, UserMentionEntity sind
*/
CREATE  TABLE  timeline.T_Entity
(      
       ID bigint PRIMARY KEY
);


CREATE  TABLE  timeline.T_Hashtag
(
 ID bigint PRIMARY KEY,
    indices_start integer ,
    indices_end   integer,
    httext        VARCHAR (4000),
    entity_id	  bigint REFERENCES timeline.T_Entity(ID)
);

/* T_Symbol wird für den MVP nicht gefüllt, da es keine Indikatoren dafür gibt */
CREATE  TABLE  timeline.T_Symbol
(
 ID bigint PRIMARY KEY,
    indices_start integer ,
    indices_end   integer,
    symtext        VARCHAR (4000),
    entity_id	  bigint REFERENCES timeline.T_Entity(ID)
);

/* T_Status
Feld contributors fehlt, weil deprecated laut API-Doku
Keine Ref. Integrität für quoted und retweeted tweets, da sonst eine rekursive Auflösung dieser Tweets erfolgen muss.
Feld scopes fehlt, weil nur für Twitter-Werbung
Feld WithheldInCountries enthält die String-Verkettung aus der Twitter4J API.
T_Geolocation wird entfernt und deren zwei Felder werden dem Status zugeschlagen
TODO: Entitys einbauen
*/
CREATE  TABLE  timeline.T_Status
(
    ID   bigint,
    recorded_at  TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE ,
    favourites_count integer ,
    username        VARCHAR (4000) ,
    screen_name VARCHAR (4000) ,
    lang            VARCHAR (4000) ,
    status_place_id  bigint REFERENCES timeline.T_Place(ID), 
    withheld_in_countries VARCHAR (4000),
    InReplyToScreenName varchar(4000),
    InReplyToStatusId bigint,
    InReplyToUserId bigint,
    quoted_status_id  bigint,
    RetweetCount integer,
    retweeted_status_id bigint,
    status_source varchar(4000),
    status_Text varchar(4000),
    status_user_id bigint,
    isFavorited integer,
    isPossiblySensitive integer,
    isRetweet integer,
    isRetweeted integer,
    isRetweetedByMe integer,
    isTruncated integer,
    dcparam_id bigint REFERENCES timeline.T_DataCollParameter(ID),
    latitude double precision,
    longitude double precision,
    URLEntities_id bigint REFERENCES timeline.T_Entity(ID),
    HashtagEntities_id bigint REFERENCES timeline.T_Entity(ID),
    UserMentionEntities_id bigint REFERENCES timeline.T_Entity(ID),
    DataCollSession_id bigint REFERENCES timeline.T_DataCollParameter(ID),
    MediaEntities_id bigint REFERENCES timeline.T_Entity(ID),
    PRIMARY KEY (ID,recorded_at,DataCollSession_id)
);

/*
T_User:
Folgende Felder sind nicht implementiert:
String	getOriginalProfileImageURL() 
String	getOriginalProfileImageURLHttps() 
String	getProfileBackgroundColor() 
String	getProfileBackgroundImageURL() 
String	getProfileBackgroundImageUrlHttps() 
String	getProfileBannerIPadRetinaURL() 
String	getProfileBannerIPadURL() 
String	getProfileBannerMobileRetinaURL() 
String	getProfileBannerMobileURL() 
String	getProfileBannerRetinaURL() 
String	getProfileBannerURL()
String	getProfileImageURL()
String	getProfileLinkColor() 
String	getProfileSidebarBorderColor() 
String	getProfileSidebarFillColor() 
String	getProfileTextColor() 
boolean	isShowAllInlineMedia()
Da sich das User-Profil im Zeitverlauf ändern kann, brauchen wir einen Zeitstempel im Primary Key. Es macht keinen Sinn, die User-Daten in der DB zu aktualisieren.
User und Status sind 1:1 verknüpft, nicht 1:N, wie ich früher dachte.
*/
CREATE  TABLE  timeline.T_User
(
    ID   bigint, --user ID, not status ID!!
    recorded_at  TIMESTAMP WITH TIME ZONE,
    username        VARCHAR (4000) ,
    screen_name VARCHAR (4000) ,
    created_at  TIMESTAMP WITH TIME ZONE ,
    description     VARCHAR (4000) ,
    geo_enabled     integer,
    lang            VARCHAR (4000) ,
    followers_count integer ,
    favourites_count integer ,
    friends_count   integer ,
    listed_count   integer ,
    loca            VARCHAR (4000),
    statuses_count        integer ,
    TimeZone varchar(4000),
    user_URL varchar(4000),
    URLEntity_id bigint,
    DescURLEntity_id bigint,
    UtcOffset integer, 
    WithheldInCountries varchar(4000),
    isContributorsEnabled integer,
    isDefaultProfile integer,
    isDefaultProfileImage integer,
    isFollowRequestSent integer,
    isProfileBackgroundTiled integer,
    isProfileUseBackgroundImage integer,
    isProtected integer,
    isTranslator integer,
    isverified  integer,
    DataCollSession_id bigint REFERENCES timeline.T_DataCollParameter(ID),
    PRIMARY KEY (ID,recorded_at,DataCollSession_id)
);
ALTER TABLE  timeline.T_Status ADD CONSTRAINT fk_uid FOREIGN KEY (status_user_id,recorded_at,DataCollSession_id) REFERENCES timeline.T_User(ID,recorded_at,DataCollSession_id);

CREATE  TABLE  timeline.T_URL
(
    ID bigint,
    display_url   VARCHAR (4000) ,
    expanded_url  VARCHAR (4000) ,
    indices_start integer ,
    indices_end   integer ,
    url           VARCHAR (4000),
    urltext	  VARCHAR (4000),
    entity_id	  bigint REFERENCES timeline.T_Entity(ID),
    PRIMARY KEY (ID)
);

ALTER TABLE timeline.T_User ADD FOREIGN KEY (URLEntity_id) REFERENCES timeline.T_Entity(ID);
ALTER TABLE timeline.T_User ADD FOREIGN KEY (DescURLEntity_id) REFERENCES timeline.T_Entity(ID);

CREATE  TABLE timeline.T_User_Mention
(
    ID bigint PRIMARY KEY,
    user_id       bigint,
    indices_start integer ,
    indices_end   integer ,
    username      VARCHAR (4000),
    screen_name   VARCHAR (4000),
    umtext	  VARCHAR (4000),
    entity_id	  bigint REFERENCES timeline.T_Entity(ID)  
);


-- Ich bilde die Vererbung in Twitter4J über eine einfache Wiederholung der Felder aus der Basisklasse URL ab.
CREATE  TABLE timeline.T_Media
(
    ID               bigint PRIMARY KEY,
    media_url        VARCHAR (4000) ,
    media_url_https  VARCHAR (4000) ,
    media_type             VARCHAR (4000) , 
    display_url   VARCHAR (4000) ,
    expanded_url  VARCHAR (4000) ,
    indices_start integer ,
    indices_end   integer ,
    url           VARCHAR (4000),
    urltext	  VARCHAR (4000),
    sizes 	  VARCHAR (4000),
    entity_id	  bigint REFERENCES timeline.T_Entity(ID)
);

/* Wird für den MVP nicht befüllt*/
CREATE  TABLE timeline.T_MediaEntitySize
(
    ID bigint,
    size          integer, -- large, medium, small, thumb als Twitter4j-Konstante 
    height	  integer,
    resize	  integer,
    width	  integer,
    media_id	  bigint REFERENCES timeline.T_Media(ID),
    PRIMARY KEY (ID, size)
);

CREATE TABLE timeline.t_source_qualification
(
    ID SERIAL PRIMARY KEY,
    source VARCHAR(4000),
    comment varchar(4000),
    botscore double precision,
    kobotscore double precision
);

CREATE TABLE timeline.T_CHALLENGE_USER
(
    ID SERIAL PRIMARY KEY,
    user_id BIGINT,
    parent_screen_name VARCHAR(4000),
    parent_id BIGINT,
    botscore double precision
);


ALTER TABLE IF EXISTS timeline.T_User owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Attribut owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Hashtag owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Symbol owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_URL owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_User_Mention owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_MediaEntitySize owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Media owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Entity owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Place owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Status owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_DataCollParameter owner to dbuser;
ALTER TABLE IF EXISTS timeline.T_Geolocation owner to dbuser;
ALTER TABLE IF EXISTS timeline.t_source_qualification owner to dbuser;

ALTER SEQUENCE IF EXISTS timeline.url_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.param_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.place_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.geoloc_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.entity_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.hashtag_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.usermention_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.media_seq owner to dbuser;

/* Import data */

\COPY timeline.t_source_qualification(source,comment,botscore,kobotscore) FROM 'all_source_values_validation_import.csv' WITH (FORMAT 'csv', DELIMITER ';', HEADER);
\copy timeline.t_challenge_user(user_id,parent_screen_name,parent_id,botscore) from 'IDS.csv' with (FORMAT 'csv', delimiter ',', HEADER);
