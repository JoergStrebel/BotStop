/* SQL Statements, um die DB-Struktur der Datenbank twitter zu definieren 
   DB-Schema für die Munich Bot Challenge
   Das Schema datacollector wird nicht mehr benötigt, daher werden die Tabellen gelöscht.
*/


/* als user postgres in der Datenbank twitter*/
CREATE ROLE dbuser LOGIN;
DROP SCHEMA IF EXISTS DataCollector CASCADE;
DROP SCHEMA IF EXISTS TimeLine CASCADE;

CREATE SCHEMA IF NOT EXISTS DataCollector AUTHORIZATION dbUser;
CREATE SCHEMA IF NOT EXISTS TimeLine AUTHORIZATION dbUser;


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
timeline.T_CHALLENGE_USER cascade,
timeline.T_AA_FOLLOWER cascade;

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

CREATE TABLE timeline.T_AA_FOLLOWER
(
    userid BIGINT primary key,
    maxtweetid BIGINT,
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
ALTER TABLE IF EXISTS timeline.t_challenge_user owner to dbuser;
ALTER TABLE IF EXISTS timeline.t_aa_follower owner to dbuser;

ALTER SEQUENCE IF EXISTS timeline.url_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.param_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.place_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.geoloc_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.entity_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.hashtag_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.usermention_seq owner to dbuser;
ALTER SEQUENCE IF EXISTS timeline.media_seq owner to dbuser;



/* Importiere CSVs aus dem aktuellen Verzeichnis */

\COPY timeline.t_source_qualification(source,comment,botscore,kobotscore) FROM 'all_source_values_validation_import.csv' WITH (FORMAT 'csv', DELIMITER ';', HEADER);
\copy timeline.t_challenge_user(user_id,parent_screen_name,parent_id,botscore) from 'IDS.csv' with (FORMAT 'csv', delimiter ',', HEADER);


/* Weitere Strukturen zur Performance-Optimierung */
--Constraints (NOT NULL, UNIQUE KEY) einbauen
alter table timeline.t_status alter column status_user_id set not null;
alter table timeline.t_challenge_user alter column user_id set not null;

--Index auf foreign key columns
-- v.a. wichtig für  t_status
-- Index wird immer im Schema der Quelltabelle angelegt
drop index if exists timeline.idx_statusjoin; 
create index idx_statusjoin on timeline.t_status (status_user_id, recorded_at, datacollsession_id);

--Index auf status_user_id
drop index if exists timeline.idx_statusuid; 
create index idx_statusuid on timeline.t_status (status_user_id);

--Index auf entity_id - foreign key index!
drop index if exists timeline.idx_um_entity_id; 
create index idx_um_entity_id on timeline.T_User_Mention (entity_id);

-- Index auf user_id
drop index if exists timeline.idx_chal_user_uid; 
create index idx_chal_user_uid  on timeline.t_challenge_user (user_id);

alter table timeline.t_challenge_user add column if not exists MAXTWEETID bigint;
update timeline.t_challenge_user set MAXTWEETID = 1;
alter table timeline.t_challenge_user alter column MAXTWEETID set not null;

-- MAXTWEETID mit Wert >1 initialisieren
WITH list_userids as 
(
	select tcu.user_id uid, coalesce(max(ts.id),1) maxtweetid
 	from timeline.T_Status ts right outer join timeline.t_challenge_user tcu
	on (ts.status_user_id=tcu.user_id)
	group by tcu.user_id
)
update timeline.t_challenge_user set MAXTWEETID = list_userids.maxtweetid from list_userids where user_id=list_userids.uid;

--Statistiken aktualisieren lassen
Vacuum (analyze);
