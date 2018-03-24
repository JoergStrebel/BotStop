/*
Viewdefinitionen für BotShield zur Implementierung der Indikatoren
Autor: jstrebel
Jede KPI hat einen eindeutigen Namen KPINAME, ein zugeordnetes Gewicht WEIGHT und einen Botscore BOTSCORE pro User USERID
Angepasste Version für die Munich Bot Challenge. Änderungen:
- Das  datacollector-Schema wird nicht genutzt. 
- Alle KPIs werden über das timeline-Schema berechnet.
- Es gibt keine Stufe 1/ Stufe 2-Unterscheidung mehr, und dementsprechend auch keine Botkandidaten. Wir machen nur noch Scoring in einer Stufe.
*/

/* Tabelle mit Gewichtungsfaktoren für die KPI-Berechnung. Jede KPI hat einen eindeutigen Namen*/
DROP TABLE IF EXISTS timeline.T_KPI_WEIGHTS CASCADE;
CREATE table timeline.T_KPI_WEIGHTS (
KPINAME varchar(100) PRIMARY KEY,
WEIGHT real
);

INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('PROFILEAGE',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('NAMDESCLEN',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('ANZFOLLOW',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETMENTION',1.0);
--INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('DECILPOSTS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('LANGUSERTWEETS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETFREQ',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('STATUSFREQUENCY',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('REACTIONTIME',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETLANGS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETCOUNT',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('SOURCEQUALIFICATION',1.0);

DROP TABLE IF EXISTS timeline.T_USER_BOTSCORE CASCADE;
CREATE unlogged table timeline.T_USER_BOTSCORE (
KPINAME varchar(100),
userid bigint,
botscore double precision not null,
PRIMARY KEY (userid, kpiname)
);


/**************************************************************************

 View: Indikator "Alter des Profils" in Tagen

 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_PROFILEAGE CASCADE;


/**************************************************************************

 Funktion: Indikator "Alter des Profils" in Tagen

 ***************************************************************************/

drop function if exists timeline.F_KPI_PROFILEAGE(pUser bigint);
CREATE FUNCTION timeline.F_KPI_PROFILEAGE(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('PROFILEAGE' as varchar(100)) KPINAME, 
pUser, 
case when PROFILEAGE>365 THEN 0.0 
else 1-(PROFILEAGE/365.0) end
BOTSCORE 
from
	(select 	
	extract(epoch from age(MCA))/(3600*24) PROFILEAGE 
	from
		(select min(created_at) MCA from timeline.t_user where ID=pUser) as R_Mindates) as R_Profage;
$$ LANGUAGE SQL;


/**************************************************************************
 View:
 Indikator "Länge des Profilnamens und Länge des Beschreibungstextes"
 Funktioniert auch, wenn der User sein Profil über die Zeit ändert

 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_NAMDESCLEN CASCADE;

/**************************************************************************
 Function:
 Indikator "Länge des Profilnamens und Länge des Beschreibungstextes"
 Funktioniert auch, wenn der User sein Profil über die Zeit ändert

 ***************************************************************************/
DROP function if exists timeline.F_KPI_NAMDESCLEN(pUser bigint);
CREATE FUNCTION timeline.F_KPI_NAMDESCLEN(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('NAMDESCLEN' as varchar(100)) KPINAME, 
pUser userid,
cast(case 
     when PROFILENAMELEN<8 AND DESCLEN=0 THEN 1.0
     when NUMBERINNAME>0 THEN 1.0
     else 0.0 
end as double precision) 
BOTSCORE 
from
	(select 
	COALESCE(avg(char_length(screen_name)),0) PROFILENAMELEN, 
	COALESCE(avg(char_length(description)),0) DESCLEN,
	COALESCE(avg(char_length(substring(screen_name,'[0-9]+'))),0) NUMBERINNAME  
	from timeline.T_USER where ID=pUser) R_PROFDESC;
$$ LANGUAGE SQL;

/**************************************************************************
 View:
 Indikator "Anzahl Follower"

 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_ANZFOLLOW CASCADE;

/**************************************************************************
 Function:
 Indikator "Anzahl Follower"

 ***************************************************************************/
DROP function if exists timeline.F_KPI_ANZFOLLOW(pUser bigint);
CREATE FUNCTION timeline.F_KPI_ANZFOLLOW(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('ANZFOLLOW' as varchar(100)) KPINAME, 
pUser userid, 
cast(case when RATIOFOLFRIEND<1 THEN 1-RATIOFOLFRIEND
else 0.0 end as double precision)
BOTSCORE 
from
	(select 
	avg(followers_count)/NULLIF(avg(friends_count),0) RATIOFOLFRIEND 
	from timeline.T_USER where  ID=pUser) r_foltofriend;
$$ LANGUAGE SQL;


/**************************************************************************
 * Indikator "Anzahl der direkten Referenz auf einen Nutzer pro Tweet"
 * Retweets ein Problem, da in Retweets der Originaluser erwähnt wird, d.h. da ist immer eine direkte UserMention drin
 * Replys haben das gleiche Problem.
 * TODO
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETMENTION CASCADE;

/**************************************************************************
* Funktion: 
* Indikator "Anzahl der direkten Referenz auf einen Nutzer pro Tweet"
 * Retweets ein Problem, da in Retweets der Originaluser erwähnt wird, d.h. da ist immer eine direkte UserMention drin
 * Replys haben das gleiche Problem.
 * TODO
 ***************************************************************************/
DROP function if exists timeline.F_KPI_TWEETMENTION(pUser bigint);
CREATE FUNCTION timeline.F_KPI_TWEETMENTION(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('TWEETMENTION' as varchar(100)) KPINAME, 
pUser USERID, 
cast(case when UMPROTWEET<0.5 THEN 1-UMPROTWEET
else 0.0 end as double precision)
BOTSCORE 
from
	 (select 
	 coalesce(taum.ANZAHLUM/cast(nullif(tat.ANZAHLTWEETS,0) as double precision),0) UMPROTWEET 
	 from 
	     (select  count(*) ANZAHLTWEETS from timeline.T_Status ts
	      where ts.status_user_id=pUser) tat,
	     (select count(*) ANZAHLUM from 
	     	     timeline.T_Status ts, 
		     timeline.T_Entity te, 
		     timeline.T_User_Mention tum
	     where ts.UserMentionEntities_id=te.ID and te.ID=tum.entity_id and ts.status_user_id=pUser) taum 
) r_tweetmention;
$$ LANGUAGE SQL;

/************************************************************************** 
 Indikator "Anzahl der Posts im Vergleich zum Durchschnitt aller Posts im Thread/Thema/Gruppe"
 Die Top 5% der Nutzer werden ermittelt
 Indikator verwendet die Session ID zur themenbezogenen Unterscheidung
 ACHTUNG: Funktion nicht mehr berechenbar für Tweets aus der Timeline
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_DECILPOSTS CASCADE;

/*DROP function if exists timeline.F_KPI_DECILPOSTS(pUser bigint);
CREATE FUNCTION timeline.F_KPI_DECILPOSTS(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('DECILPOSTS' as varchar(100)) KPINAME, 
USERID, 
cast(case when DECIL>10 THEN DECIL/20.0
else 0.0 end as double precision) BOTSCORE 
from
	(select status_user_id USERID, ntile(20) over (order by ANZ ASC) DECIL from  
       	       		(select status_user_id,	DataCollSession_id, count(*) ANZ 
			from timeline.t_status group by status_user_id,DataCollSession_id) r_ugroup) 
	r_anz;
$$ LANGUAGE SQL;
*/

/**************************************************************************  
 Indikator Profilnamen: Passt die Sprache der gesendeten Nachrichten zur Sprache im Benutzerprofil? (Nr. 8)
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_LANGUSERTWEETS CASCADE;

/**************************************************************************  
 Funkion:
 Indikator Profilnamen: Passt die Sprache der gesendeten Nachrichten zur Sprache im Benutzerprofil? (Nr. 8)
 ***************************************************************************/
DROP function if exists timeline.F_KPI_LANGUSERTWEETS(pUser bigint);
CREATE FUNCTION timeline.F_KPI_LANGUSERTWEETS(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('LANGUSERTWEETS' as varchar(100)) KPINAME, 
pUser USERID, 
SUMMATCH/cast(TWEETCOUNT as double precision)  BOTSCORE 
from
	(select sum(LANGMATCH) SUMMATCH, count(*) TWEETCOUNT from
		(select case 
			when t2.lang='und' THEN 0.2
			when t1.lang=t2.lang THEN 0.0
			else 1 end LANGMATCH 
		from timeline.T_USER T1 inner join timeline.t_status T2 
		on t1.ID=t2.status_user_id and t1.recorded_at=t2.recorded_at and t1.DataCollSession_id=t2.DataCollSession_id
		where t1.ID=pUser) v1 
	) v2;
$$ LANGUAGE SQL;

/**************************************************************************  
 Indikator "Minimaler Zeitabstand zwischen zwei Posts"
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETFREQ CASCADE;

/**************************************************************************  
 Funktion:
 Indikator "Minimaler Zeitabstand zwischen zwei Posts"
 Hier werden nur die Tweets aus dem Streaming angeschaut
 Die View liefert einen Botscore zurück, wenn der User mind. zwei Tweets geschrieben haben.
 TODO: Reine, leere Retweets mit wenig Text anders behandeln, d.h. evtl. in 2 Sekunden möglich.
 Bei Retweets zählt nicht der Abstand zum Vorgänger-Tweet, sondern der Abstand zum Original-Tweet, im Sinne einer Reaktionszeit.
 Es gibt mehrere Fälle von Tweet-Sequenzen deren Zeitabstand messbar ist: 
 1.   eigener Tweet -> eigener Tweet. Zeitlimit: 5s
 2.   eigener Tweet -> Retweet.  Zeitlimit: 2s
 3.   Retweet -> Retweet.  Zeitlimit: 2s
 4.   Retweet -> eigener Tweet.  Zeitlimit: 5s
 5.   (Erzeugung fremder Tweet -> Retweet.  Zeitlimit: 2s) eigentlich eine eigene Kennzahl
 D.h. ich habe zwei Fälle: sobald ich einen Retweet sehe, gilt das Zeitlimit 2s.
 ***************************************************************************/
DROP function if exists timeline.F_KPI_TWEETFREQ(pUser bigint);
CREATE FUNCTION timeline.F_KPI_TWEETFREQ(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
cast('TWEETFREQ' as varchar(100)) KPINAME,
pUser USERID,
cast(case when min(antwzeitsec)<5 THEN 1.0
else 0.0 end as double precision) BOTSCORE 
from 
     (select 
     status_user_id USERID, 
     extract(epoch from secondval-firstval) antwzeitsec
     from 
     	  (select 
     	  status_user_id, 
	  created_at secondval, 
	  lag(created_at,1) over (PARTITION by status_user_id ORDER BY created_at asc) firstval
	  from (
	       select distinct status_user_id,created_at from timeline.T_Status where status_user_id=pUser
     	       ) all_status order by created_at asc
     	  ) timestamps where firstval is not null
     ) timedist;
$$ LANGUAGE SQL;

/**************************************************************************   
 View
   Indikator: Zeitliche Entwicklung der Follower, Friends. 
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_STATUSFREQUENCY CASCADE;

       
/**************************************************************************   
 Funktion:
 Indikator: Zeitliche Entwicklung der Follower, Friends. 
   Es werden 24 Attribute definiert, die jeweils zeigen, wieviele Tweets der Nutzer in der Stunde abgesetzt hat. 
   created_at enthält die zur Zeitzone UTC passende Zeit.
   Lösung: created_at+make_interval(secs => utcoffset), wo utcoffset aus t_user kommt.
   Annahme: jeder User hat genau eine zugeordnete Zeitzone. Stand 11.06.2017 stimmt das empirisch auch.
 ***************************************************************************/
DROP function if exists timeline.F_KPI_STATUSFREQUENCY(pUser bigint);
CREATE FUNCTION timeline.F_KPI_STATUSFREQUENCY(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
Select 
 cast('STATUSFREQUENCY' as varchar(100)) KPINAME,
 pUser USERID, 
  cast(case when Uhr1>0 OR Uhr2>0 OR Uhr3>0 OR Uhr4>0 THEN 1.0
  else 0.0 end as double precision) BOTSCORE 
  from
    (select  
    count(*) FILTER (WHERE extract(hour from created_at_tz)=0) Uhr0,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=1) Uhr1,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=2) Uhr2,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=3) Uhr3,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=4) Uhr4,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=5) Uhr5,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=6) Uhr6,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=7) Uhr7,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=8) Uhr8,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=9) Uhr9,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=10) Uhr10,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=11) Uhr11,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=12) Uhr12,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=13) Uhr13,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=14) Uhr14,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=15) Uhr15,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=16) Uhr16,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=17) Uhr17,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=18) Uhr18,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=19) Uhr19,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=20) Uhr20,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=21) Uhr21,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=22) Uhr22,
    count(*) FILTER (WHERE extract(hour from created_at_tz)=23) Uhr23
    from 
    	 (
	 select distinct ID, status_user_id,created_at, utcoffset, created_at+make_interval(secs => utcoffset) created_at_tz from 
       	 	(
		select ts.ID, ts.status_user_id,ts.created_at,tu.utcoffset 
		from timeline.T_Status ts inner join timeline.t_user tu 
		on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
		where tu.id=pUser) all_status
	 ) all_dist_status
   ) uuids;
$$ LANGUAGE SQL;

/****************************************************************************************************************************
* View
* Indikator "Reaktionszeit bei Antworttweets in Sekunden"
*
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_REACTIONTIME CASCADE;

/****************************************************************************************************************************
* Funktion
* Indikator "Reaktionszeit bei Antworttweets in Sekunden"
*
******************************************************************************************************************************/
DROP function if exists timeline.F_KPI_REACTIONTIME(pUser bigint);
CREATE FUNCTION timeline.F_KPI_REACTIONTIME(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
SELECT
cast('REACTIONTIME' as varchar(100)) KPINAME,
pUser userid,
cast(case when MINAWZ<5 and MINAWZ IS NOT null THEN 1.0
else 0.0 end as double precision) BOTSCORE 
from (select        	
       	min(antwzeitsec) MINAWZ 
 	from  ( 
	      With all_dist_status as 
	      (
	      	   select distinct ID, status_user_id, InReplyToStatusId, created_at+make_interval(secs => utcoffset) created_at_tz from 
       		   	(
		   	select ts.ID, ts.status_user_id, ts.created_at, ts.InReplyToStatusId, tu.utcoffset 
			from timeline.T_Status ts inner join timeline.t_user tu 
			on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
			where tu.id=pUser) all_status
	      )
	      select 		  
       	      t1.status_user_id USERID,
	      extract(epoch from t1.created_at_tz-t2.created_at_tz) antwzeitsec 
	      from all_dist_status t1, all_dist_status t2 
	      where t1.InReplyToStatusId=t2.ID
	      ) r_timeperuser 
	) all_data;
$$ LANGUAGE SQL;


/****************************************************************************************************************************
 View
 Indikator: Anzahl verschiedener Sprachen (Nr. 18)
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETLANGS CASCADE;

/****************************************************************************************************************************
 Funktion
   Indikator: Anzahl verschiedener Sprachen (Nr. 18)
   Leider erkennt Twitter die Tweet-Sprache nicht besonders zuverlässig.
   Ich kann also nicht einfach die verschiedenen Sprachen eines Nutzers zählen, sondern muss einen relativen Grenzwert festlegen.
   Oder ich schaue mir die Verteilung der versch. Sprachen an
   Statistik: Wieviele Sprachen pro Tweet hat der User?
   Die Twitter Spracherkennung kann bei ca. 6% der Tweets keine Sprache erkennen, d.h. wenn ein user  mehr als das hat, 
   ist er ein Botkandidat. 
   Alle Tweets ohne Text (z.B. Retweets) werden aus der Berechnung des Indikators entfernt --> Feld "t_status.isretweet"
   TODO: Entferne alle Sprachen deren Tweetanzahl <5% alle Tweets des Nutzers ist. Sobald die verbleibenden Sprachanzahl > 3 ist, dann setze Botscore auf 1.
******************************************************************************************************************************/
DROP function if exists timeline.F_KPI_TWEETLANGS(pUser bigint);
CREATE FUNCTION timeline.F_KPI_TWEETLANGS(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
with 
v_dist_status as  
	      (select distinct ID, status_user_id,lang from  timeline.T_Status where status_user_id=pUser and isretweet=0),
v_anzlang as 
	     (select count(distinct lang) anzlang from v_dist_status where lang!='und'),
v_anztweets as  (select count(*) ANZTWEETS from v_dist_status where lang!='und')
select 
       cast('TWEETLANGS' as varchar(100)) KPINAME, 
       pUser USERID,   
       cast(case when v_anzlang.ANZLANG/cast(nullif(v_anztweets.anztweets,0) as double precision)>0.06 THEN 1.0  else 0.0 end as double precision) BOTSCORE 
from v_anzlang,v_anztweets;
$$ LANGUAGE SQL;

/****************************************************************************************************************************
 View
 Indikator "Anzahl aller  Posts pro Tag"
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETCOUNT CASCADE;

/****************************************************************************************************************************
 Funktion
 Indikator "Anzahl aller  Posts pro Tag"
 Falls der User mehr als 50 Tweets pro Tag abgesetzt hat, dann Botscore=1
******************************************************************************************************************************/
DROP function if exists timeline.F_KPI_TWEETCOUNT(pUser bigint);
CREATE FUNCTION timeline.F_KPI_TWEETCOUNT(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
       cast('TWEETCOUNT' as varchar(100)) KPINAME, 
       pUser, 
       cast(case when max(ANZTWEETS)>50 THEN 1.0
       else 0.0 end as double precision) BOTSCORE 
from
	(
	select to_char(created_at,'DD.MM.YYYY') DTAG, count(*) ANZTWEETS
	from
		(select distinct ID, created_at, status_user_id from timeline.T_Status where status_user_id=pUser) v_alldiststatus
	group by to_char(created_at,'DD.MM.YYYY')
	) v_tweets;
$$ LANGUAGE SQL;

/****************************************************************************************************************************
 View
 Indikator "Source-Feld" - normaler, gewichteter Bot-Score
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_SOURCE_QUALIFICATION CASCADE;

/****************************************************************************************************************************
 Funktion
 Indikator "Source-Feld" - normaler, gewichteter Bot-Score
 Berechnung: Durchschnitt der Scores über alle Tweets.
 Die Subquery v_tweets dient v.a. dazu, doppelte Status-IDs zu behandeln.
 Die KPI wird nur für User berechnet, die überhaupt Tweets abgesetzt haben
******************************************************************************************************************************/
DROP function if exists timeline.F_KPI_SOURCE_QUALIFICATION(pUser bigint);
CREATE FUNCTION timeline.F_KPI_SOURCE_QUALIFICATION(pUser bigint) RETURNS timeline.T_USER_BOTSCORE AS $$
select 
       cast('SOURCEQUALIFICATION' as varchar(100)) KPINAME, 
       pUser, 
       cast(avg(v_tweets.BOTSCORE) as double precision) BOTSCORE
from
(
	select  avg(coalesce(tsq.botscore,0)) botscore
	from timeline.t_status ts left outer join timeline.t_source_qualification tsq
	on (ts.status_source=tsq.source) where ts.status_user_id=pUser
	group by ts.id
) v_tweets;
$$ LANGUAGE SQL;


/**************************************************************************************************************************** 
 reine Zusammenfassung aller KPIs  in einer Tabelle
 Enthält alle UserID / KPI-Kombinationen für die User aus dem MBC-Datensatz
 ACHTUNG: evtl. sind viele KPIs NULL, weil es für den User eben noch keine Daten gibt!
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_ALLKPIDATA CASCADE;

DROP function if exists timeline.F_KPI_ALLKPIDATA(pUser bigint);
CREATE FUNCTION timeline.F_KPI_ALLKPIDATA(pUser bigint) RETURNS double precision AS $$
Select 
       cast(sum(botscore*weight)/cast(nullif(sum(weight),0) as double precision) as double precision) TOTALBOTSCORE
       from 
(
       select  
       case when vkpi.botscore is null THEN 0 else vkpi.botscore end botscore,
       case when vkpi.botscore is null THEN 0 else vw.weight end weight 
       from 
       (
	select t1.kpiname, t1.userid, t1.botscore from timeline.F_KPI_PROFILEAGE(pUser) as  T1
	union
	select t2.kpiname, t2.userid, t2.botscore from timeline.F_KPI_NAMDESCLEN(pUser) as T2
	union
	select t3.kpiname, t3.userid, t3.botscore from timeline.F_KPI_ANZFOLLOW(pUser) as T3
	union
	select t4.kpiname, t4.userid,  t4.botscore from timeline.F_KPI_TWEETMENTION(pUser) as T4
	union
	select t6.kpiname, t6.userid,  t6.botscore from timeline.F_KPI_LANGUSERTWEETS(pUser) as T6
	union
	select t7.kpiname, t7.userid,  t7.botscore from timeline.F_KPI_TWEETFREQ(pUser) as T7
	union
	select t8.kpiname, t8.userid, t8.botscore from timeline.F_KPI_STATUSFREQUENCY(pUser) as T8
	union
	select t9.kpiname, t9.userid, t9.botscore from timeline.F_KPI_REACTIONTIME(pUser) as T9
	union
	select t10.kpiname, t10.userid, t10.botscore from timeline.F_KPI_TWEETLANGS(pUser) as T10
	union
	select t11.kpiname, t11.userid, t11.botscore from timeline.F_KPI_TWEETCOUNT(pUser) as T11
	union
	select t12.kpiname, t12.userid, t12.botscore from timeline.F_KPI_SOURCE_QUALIFICATION(pUser) as T12
	) vkpi
	left outer join 
	(
		select t2.kpiname, t2.weight from timeline.t_kpi_weights t2
	) vw
	on (vkpi.kpiname=vw.kpiname)
) v_allkpi;
$$ LANGUAGE SQL;

/**************************************************************************************************************************** 
 Funktion: Zusammenfassung aller gewichteten KPIs durch Berechnung des gewichteten Mittelwerts 
 Die View liefert den Input für die manuelle Begutachtung.
 Nur die User aus der MBU werden bewertet.
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
******************************************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_BOTLIST CASCADE;
DROP function if exists timeline.F_USER_CALCBOTSCORE(pUser bigint);

/********************************************************************************************************
 Ab hier Berechnung der KO-KPIs
 Aufgrund des modularen Aufbaus können neue KPIs relativ einfach integriert werden:
 1. Wert in T_KPI_KOFLAGS eintragen
 2. View mit KPI bauen
 3. View in V_USER_KOKPIDATA eintragen
 Die Zusammenrechnung mit den restlichen KPIs erfolgt dann automatisch.
*********************************************************************************************************/

/* Tabelle mit KO-KPIs Jede KPI hat einen eindeutigen Namen*/
DROP TABLE IF EXISTS timeline.T_KPI_KOFLAGS CASCADE;
/*
CREATE table timeline.T_KPI_KOFLAGS (
KPINAME varchar(100) PRIMARY KEY
);

INSERT INTO timeline.T_KPI_KOFLAGS(KPINAME) VALUES ('KOSOURCEQUALIFICATION');
*/

/********************************************************************************************************
 Funktion
 Indikator "Source-Feld" - KO- Bot-Score
 Berechnung: Mehrheitsentscheidung über alle Tweets, d.h. wenn mehr als 50% das KO-Flag haben.
 Die Subquery v_tweets dient v.a. dazu, doppelte Status-IDs zu behandeln.
 TODO: Optimierunspotential, da fast identisch mit timeline.F_KPI_SOURCE_QUALIFICATION
********************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KO_SOURCE_QUALIFICATION CASCADE;

DROP function if exists timeline.F_KPI_KO_SOURCE_QUALIFICATION(pUser bigint);
CREATE FUNCTION timeline.F_KPI_KO_SOURCE_QUALIFICATION(pUser bigint) RETURNS double precision AS $$
Select 
       cast(case when avg(v_tweets.KOBOTSCORE)>0.5 THEN 1.0
       else 0.0 end as double precision) KOBOTSCORE  
from
(
	select  max(coalesce(tsq.kobotscore,0)) kobotscore
	from timeline.t_status ts left outer join timeline.t_source_qualification tsq
	on (ts.status_source=tsq.source) where ts.status_user_id=pUser
	group by ts.id
) v_tweets;
$$ LANGUAGE SQL;

/********************************************************************************************************* 
 Funktion: Zusammenfassung aller KO-KPI-Kennzahlen über alle User in eine Liste.
 Nur die User aus der MBU werden bewertet.
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
**********************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KOKPIDATA CASCADE;


/******************************************************************************************************** 
 Funktion: Berechnung des KO-Flags pro User über alle KO-Kennzahlen hinweg 
 Rechenregel: max(Botscore_KO), was einer Ver-ODER-ung der Flags entspricht.
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
*********************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_KOBOTLIST CASCADE;


/********************************************************************************************************* 
 Funktion: Zusammenfassung aller Ergebnisse (gewichteter Mittelwert und KO-Flag) pro User.
 Rechenregel: greatest(Botscore_gewichtet, Botscore_KO)
 Nur die User aus der MBU werden bewertet.
**********************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_INTEGRATE_SCORES CASCADE;

DROP function if exists timeline.F_USER_INTEGRATE_SCORES(pUser bigint);
CREATE FUNCTION timeline.F_USER_INTEGRATE_SCORES(pUser bigint) RETURNS double precision AS $$
Select greatest(timeline.F_KPI_KO_SOURCE_QUALIFICATION(pUser),timeline.F_KPI_ALLKPIDATA(pUser)) TOTALSCORE;
$$ LANGUAGE SQL;

/**************************************************************************************************************************** 
 Exportview für die Abgabe des Botscore 
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
 TODO: anzupassen an neue Views
******************************************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_EXPORT CASCADE;


/**************************************************************************  
 VERALTET!! IST NUR EIN STUB!
 Bleibt bestehen, weil der StatusFetcher die View nutzt. Liefert keinen sinnvollen Inhalt mehr.
 Input für Statusfetcher.
 Falls noch kein Eintrag in timeline zu einer UserID besteht, wird 1 als max-id angenommen.
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_MAXTWEETID CASCADE;
/*
CREATE VIEW timeline.V_USER_MAXTWEETID as 
select  userid, tweetid from (values(1,1),(2,1)) foo (userid, tweetid);  
*/

/**************************************************************************  
   View zur Identifikation der letzten Tweet ID pro User, der im Rahmen der Munich Bot Challenge untersucht werden soll. 
   Input für Statusfetcher.
   Falls noch kein Eintrag in timeline zu einer UserID besteht, wird 1 als max-id angenommen.
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_CHALLENGE_USER_MAXTWEETID CASCADE;
/**************************************************************************  
CREATE VIEW timeline.V_CHALLENGE_USER_MAXTWEETID as 
select coalesce(ul.user_id,tl.userid) userid, coalesce(tl.maxid,1) tweetid from 
       (select distinct user_id from timeline.T_CHALLENGE_USER) ul full outer join 
       (select status_user_id userid, max(id) maxid from timeline.t_status group by status_user_id) tl 
       on ul.user_id=tl.userid;
 ***************************************************************************/

/**************************************************************************************************************************** 
 Funktion zur Verarbeitung der Tweets einer UserID
 Lösche alle Daten zur UserID
 Die Fehlerbehandlung muss in der aufrufenden Prozedur gemacht werden (d.h. Rollback und Logging)
******************************************************************************************************************************/
DROP function if exists timeline.F_Process_UID(pUser bigint);
CREATE FUNCTION timeline.F_Process_UID(pUser bigint) RETURNS VOID AS $$
	update timeline.T_CHALLENGE_USER set botscore=timeline.F_USER_INTEGRATE_SCORES(pUser) where user_id=pUser;
$$ LANGUAGE SQL;

