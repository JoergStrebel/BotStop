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
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('DECILPOSTS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('LANGUSERTWEETS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETFREQ',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('STATUSFREQUENCY',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('REACTIONTIME',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETLANGS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETCOUNT',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('SOURCEQUALIFICATION',1.0);

/**************************************************************************

 Indikator "Alter des Profils" in Tagen

 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_PROFILEAGE CASCADE;
CREATE VIEW timeline.V_USER_KPI_PROFILEAGE as 
select 
cast('PROFILEAGE' as varchar(100)) KPINAME, USERID, PROFILEAGE, 
case when PROFILEAGE>365 THEN 0.0 
else 1-(PROFILEAGE/365) end
BOTSCORE 
from
	(select 
	ID USERID,
	extract(epoch from age(MCA))/(3600*24) PROFILEAGE 
	from
		(select ID,min(created_at) MCA from timeline.t_user group by ID) as R_Mindates) as R_Profage;

/**************************************************************************

 Indikator "Länge des Profilnamens und Länge des Beschreibungstextes"
 Funktioniert auch, wenn der User sein Profil über die Zeit ändert

 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_NAMDESCLEN CASCADE;
CREATE VIEW timeline.V_USER_KPI_NAMDESCLEN as 
select 
cast('NAMDESCLEN' as varchar(100)) KPINAME, USERID,  PROFILENAMELEN, DESCLEN,
case 
     when PROFILENAMELEN<8 AND DESCLEN=0 THEN 1.0
     when NUMBERINNAME>0 THEN 1.0
     else 0.0 
end
BOTSCORE 
from
	(select 
	ID USERID,
	COALESCE(avg(char_length(screen_name)),0) PROFILENAMELEN, 
	COALESCE(avg(char_length(description)),0) DESCLEN,
	COALESCE(avg(char_length(substring(screen_name,'[0-9]+'))),0) NUMBERINNAME  
	from timeline.T_USER group by ID) R_PROFDESC;

/**************************************************************************

 Indikator "Anzahl Follower"

 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_ANZFOLLOW CASCADE;
CREATE VIEW timeline.V_USER_KPI_ANZFOLLOW as 
select 
cast('ANZFOLLOW' as varchar(100)) KPINAME, USERID, ANZFOLLOWER, ANZFRIENDS, RATIOFOLFRIEND,
case when RATIOFOLFRIEND<1 THEN 1-RATIOFOLFRIEND
else 0 end
BOTSCORE 
from
	(select 
	ID USERID, 
	avg(followers_count) ANZFOLLOWER, 
	avg(friends_count) ANZFRIENDS, 
	avg(followers_count)/NULLIF(avg(friends_count),0) RATIOFOLFRIEND 
	from timeline.T_USER group by ID) r_foltofriend;

/**************************************************************************
 * Indikator "Anzahl der direkten Referenz auf einen Nutzer pro Tweet"
 * Retweets ein Problem, da in Retweets der Originaluser erwähnt wird, d.h. da ist immer eine direkte UserMention drin
 * Replys haben das gleiche Problem.
 * TODO
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETMENTION CASCADE;
CREATE VIEW timeline.V_USER_KPI_TWEETMENTION as
select 
cast('TWEETMENTION' as varchar(100)) KPINAME, USERID, ANZAHLTWEETS, UMPROTWEET,
case when UMPROTWEET<0.5 THEN 1-UMPROTWEET
else 0.0 end
BOTSCORE 
from
	 (select 
	 tat.USERID USERID,
	 tat.ANZAHLTWEETS ANZAHLTWEETS, 
	 coalesce(taum.ANZAHLUM/cast(tat.ANZAHLTWEETS as double precision),0) UMPROTWEET 
	 from 
	     (select ts.status_user_id USERID, count(*) ANZAHLTWEETS from timeline.T_Status ts
	      group by ts.status_user_id) tat 
	 left outer join
	     (select ts.status_user_id USERID, count(*) ANZAHLUM from 
	     	     timeline.T_Status ts, 
		     timeline.T_Entity te, 
		     timeline.T_User_Mention tum
	     where ts.UserMentionEntities_id=te.ID and te.ID=tum.entity_id
     	     group by ts.status_user_id) taum 
	 on tat.USERID=taum.USERID) r_tweetmention;

/************************************************************************** 
 Indikator "Anzahl der Posts im Vergleich zum Durchschnitt aller Posts im Thread/Thema/Gruppe"
 Die Top 5% der Nutzer werden ermittelt
 Indikator verwendet die Session ID zur themenbezogenen Unterscheidung
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_DECILPOSTS CASCADE;
CREATE VIEW timeline.V_USER_KPI_DECILPOSTS as
select 
cast('DECILPOSTS' as varchar(100)) KPINAME, USERID, SESSIONID, ANZ, DECIL,
case when DECIL>10 THEN DECIL/20.0
else 0.0 end
BOTSCORE 
from
	(select status_user_id USERID,DataCollSession_id SESSIONID, ANZ, ntile(20) over (order by ANZ ASC) DECIL from  
       	       		(select status_user_id,	DataCollSession_id, count(*) ANZ 
			from timeline.t_status group by status_user_id,DataCollSession_id) r_ugroup) 
	r_anz;

/**************************************************************************  
 Indikator Profilnamen: Passt die Sprache der gesendeten Nachrichten zur Sprache im Benutzerprofil? (Nr. 8)
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_LANGUSERTWEETS CASCADE;
CREATE VIEW timeline.V_USER_KPI_LANGUSERTWEETS as
select 
cast('LANGUSERTWEETS' as varchar(100)) KPINAME, USERID, SUMMATCH/cast(TWEETCOUNT as double precision)  BOTSCORE 
from
	(select ID USERID, sum(LANGMATCH) SUMMATCH, count(*) TWEETCOUNT from
		(select t1.ID, 
			case 
			when t2.lang='und' THEN 0.2
			when t1.lang=t2.lang THEN 0.0
			else 1 end LANGMATCH 
		from timeline.T_USER T1 inner join timeline.t_status T2 
		on t1.ID=t2.status_user_id and t1.recorded_at=t2.recorded_at and t1.DataCollSession_id=t2.DataCollSession_id)
 	v1 group by ID) v2;

/**************************************************************************  
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
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETFREQ CASCADE;
CREATE VIEW timeline.V_USER_KPI_TWEETFREQ as
select 
USERID,
cast('TWEETFREQ' as varchar(100)) KPINAME,
case when min(antwzeitsec)<5 THEN 1.0
else 0.0 end BOTSCORE 
from 
(select 
status_user_id USERID, 
extract(epoch from secondval-firstval) antwzeitsec
from 
(
     select 
	    status_user_id, 
	    created_at secondval, 
	    lag(created_at,1) over (PARTITION by status_user_id ORDER BY created_at asc) firstval
     from (
     	  select distinct status_user_id,created_at from timeline.T_Status
	  ) all_status order by status_user_id asc,created_at asc
) timestamps where firstval is not null) timedist group by USERID;



/**************************************************************************  
 VERALTET!! IST NUR EIN STUB!
 Bleibt bestehen, weil der StatusFetcher die View nutzt. Liefert keinen sinnvollen Inhalt mehr.
 Input für Statusfetcher.
 Falls noch kein Eintrag in timeline zu einer UserID besteht, wird 1 als max-id angenommen.
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_MAXTWEETID CASCADE;
CREATE VIEW timeline.V_USER_MAXTWEETID as 
select  userid, tweetid from (values(1,1),(2,1)) foo (userid, tweetid);  


/**************************************************************************  
   View zur Identifikation der letzten Tweet ID pro User, der im Rahmen der Munich Bot Challenge untersucht werden soll. 
   Input für Statusfetcher.
   Falls noch kein Eintrag in timeline zu einer UserID besteht, wird 1 als max-id angenommen.
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_CHALLENGE_USER_MAXTWEETID CASCADE;
CREATE VIEW timeline.V_CHALLENGE_USER_MAXTWEETID as 
select coalesce(ul.user_id,tl.userid) userid, coalesce(tl.maxid,1) tweetid from 
       (select distinct user_id from timeline.T_CHALLENGE_USER) ul full outer join 
       (select status_user_id userid, max(id) maxid from timeline.t_status group by status_user_id) tl 
       on ul.user_id=tl.userid;

       
/**************************************************************************   
 Indikator: Zeitliche Entwicklung der Follower, Friends. 
   Es werden 24 Attribute definiert, die jeweils zeigen, wieviele Tweets der Nutzer in der Stunde abgesetzt hat. 
   created_at enthält die zur Zeitzone UTC passende Zeit.
   Lösung: created_at+make_interval(secs => utcoffset), wo utcoffset aus t_user kommt.
   Annahme: jeder User hat genau eine zugeordnete Zeitzone. Stand 11.06.2017 stimmt das empirisch auch.
 ***************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_STATUSFREQUENCY CASCADE;
CREATE VIEW timeline.V_USER_KPI_STATUSFREQUENCY as 
  select 
  USERID,
  cast('STATUSFREQUENCY' as varchar(100)) KPINAME,
  case when Uhr1>0 OR Uhr2>0 OR Uhr3>0 OR Uhr4>0 THEN 1.0
  else 0.0 end
  BOTSCORE 
  from
    (select status_user_id USERID, 
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
		) all_status
	 ) all_dist_status
   group by status_user_id
   ) uuids;


/****************************************************************************************************************************
*
* Indikator "Reaktionszeit bei Antworttweets in Sekunden"
*
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_REACTIONTIME CASCADE;
CREATE VIEW timeline.V_USER_KPI_REACTIONTIME as
select 
USERID,
cast('REACTIONTIME' as varchar(100)) KPINAME,
case when MINAWZ<5 and MINAWZ IS NOT null THEN 1.0
else 0.0 end
BOTSCORE 
from 
	(select 
       	USERID, 
       	min(antwzeitsec) MINAWZ, 
       	avg(antwzeitsec) MITTELAWZ,
       	max(antwzeitsec) MAXAWZEIT,
       	count(antwzeitsec) ANZAWZEIT from 
              (
	      With all_dist_status as 
	      (
	      	   select distinct ID, status_user_id, InReplyToStatusId, created_at+make_interval(secs => utcoffset) created_at_tz from 
       		   	(
		   	select ts.ID, ts.status_user_id, ts.created_at, ts.InReplyToStatusId, tu.utcoffset 
			from timeline.T_Status ts inner join timeline.t_user tu 
			on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
			) all_status
	      )
	      select 		  
       	      t1.status_user_id USERID,
	      extract(epoch from t1.created_at_tz-t2.created_at_tz) antwzeitsec 
	      from all_dist_status t1, all_dist_status t2 
	      where t1.InReplyToStatusId=t2.ID
	      ) r_timeperuser 
	group by USERID) all_data;


/****************************************************************************************************************************
   Indikator: Anzahl verschiedener Sprachen (Nr. 18)
   Leider scheint die Erkennung der Tweet-Sprache nicht besonders zuverlässig zu sein.
   Ich kann also nicht einfach die verschiedenen Sprachen eines Nutzers zählen, sondern muss einen relativen Grenzwert festlegen.
   Oder ich schaue mir die Verteilung der versch. Sprachen an
   Statistik: Wieviele Sprachen pro Tweet hat der User?
   Die Twitter Spracherkennung kann bei ca. 6% der Tweets keine Sprache erkennen, d.h. wenn ein user  mehr als das hat, 
   ist er ein Botkandidat. 
   TODO: Alle Tweets ohne Text (z.B. Retweets) sollten aus der Berechnung des Indikators entfernt werden.
   TODO: Entferne alle Sprachen deren Tweetanzahl <5% alle Tweets des Nutzers ist. Sobald die verbleibenden Sprachanzahl > 3 ist, dann setze Botscore auf 1.
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETLANGS CASCADE;
CREATE VIEW timeline.V_USER_KPI_TWEETLANGS as
with 
v_dist_status as  
	      (select distinct ID, status_user_id,lang from  timeline.T_Status),
v_anzlang as 
	     (select status_user_id, count(*) ANZLANG from 
       	     	     (select distinct status_user_id,lang from v_dist_status where lang!='und') v1
		     group by status_user_id),
v_anztweets as  (select status_user_id,count(*) ANZTWEETS from v_dist_status where lang!='und' group by status_user_id),
v_langtweetratio as
		 (select coalesce(v_anztweets.status_user_id,v_anzlang.status_user_id) status_user_id,
		 v_anzlang.anzlang, v_anztweets.anztweets  
		 from v_anzlang full outer join v_anztweets on v_anzlang.status_user_id=v_anztweets.status_user_id)
select 
       cast('TWEETLANGS' as varchar(100)) KPINAME, 
       status_user_id USERID,   
       case when ANZLANG/cast(anztweets as real)>0.06 THEN 1.0  else 0.0 end BOTSCORE 
from v_langtweetratio;


/****************************************************************************************************************************
 Indikator "Anzahl aller  Posts pro Tag"
 Falls der User mehr als 50 Tweets pro Tag abgesetzt hat, dann Botscore=1
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KPI_TWEETCOUNT CASCADE;
CREATE VIEW timeline.V_USER_KPI_TWEETCOUNT as
select 
       cast('TWEETCOUNT' as varchar(100)) KPINAME, 
       USERID, 
       case when max(ANZTWEETS)>50 THEN 1.0
       else 0.0 end BOTSCORE 
from
	(
	select status_user_id USERID, to_char(created_at,'DD.MM.YYYY') DTAG, count(*) ANZTWEETS
	from
		(select distinct ID, created_at, status_user_id from  timeline.T_Status) v_alldiststatus
	group by status_user_id, to_char(created_at,'DD.MM.YYYY')
	) v_tweets
group by USERID;

/****************************************************************************************************************************
 Indikator "Source-Feld" - normaler, gewichteter Bot-Score
 Berechnung: Durchschnitt der Scores über alle Tweets.
 Die Subquery v_tweets dient v.a. dazu, doppelte Status-IDs zu behandeln.
 Die KPI wird nur für User berechnet, die überhaupt Tweets abgesetzt haben
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_SOURCE_QUALIFICATION CASCADE;
CREATE VIEW timeline.V_USER_SOURCE_QUALIFICATION as
select 
       cast('SOURCEQUALIFICATION' as varchar(100)) KPINAME, 
       USERID, 
       avg(v_tweets.BOTSCORE) BOTSCORE
from
(
	select ts.id, 
	       min(ts.status_user_id) userid, 
	       avg(coalesce(tsq.botscore,0)) botscore
	from timeline.t_status ts left outer join timeline.t_source_qualification tsq
	on (ts.status_source=tsq.source)
	group by ts.id
) v_tweets
group by USERID;


/**************************************************************************************************************************** 
 reine Zusammenfassung aller KPIs  in einer Tabelle
 Enthält alle UserID / KPI-Kombinationen für die User aus dem MBC-Datensatz
 ACHTUNG: evtl. sind viele KPIs NULL, weil es für den User eben noch keine Daten gibt!
******************************************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_ALLKPIDATA CASCADE;
CREATE VIEW timeline.V_USER_ALLKPIDATA as 
select vw.kpiname, vw.userid, vkpi.botscore botscore, vw.weight from 
(
	select t1.kpiname, t1.userid, t1.botscore from timeline.V_USER_KPI_PROFILEAGE T1
	union
	select t2.kpiname, t2.userid, t2.botscore from timeline.V_USER_KPI_NAMDESCLEN T2
	union
	select t3.kpiname, t3.userid, t3.botscore from timeline.V_USER_KPI_ANZFOLLOW T3
	union
	select t4.kpiname, t4.userid,  t4.botscore from timeline.V_USER_KPI_TWEETMENTION T4
	union
	select t5.kpiname, t5.userid, max(t5.botscore) from timeline.V_USER_KPI_DECILPOSTS T5 group by t5.kpiname,t5.userid,t5.sessionid
	union
	select t6.kpiname, t6.userid,  t6.botscore from timeline.V_USER_KPI_LANGUSERTWEETS T6
	union
	select t7.kpiname, t7.userid,  t7.botscore from timeline.V_USER_KPI_TWEETFREQ T7
	union
	select t8.kpiname, t8.userid, t8.botscore from timeline.V_USER_KPI_STATUSFREQUENCY T8
	union
	select t9.kpiname, t9.userid, t9.botscore from timeline.V_USER_KPI_REACTIONTIME T9
	union
	select t10.kpiname, t10.userid, t10.botscore from timeline.V_USER_KPI_TWEETLANGS T10
	union
	select t11.kpiname, t11.userid, t11.botscore from timeline.V_USER_KPI_TWEETCOUNT T11
	union
	select t12.kpiname, t12.userid, t12.botscore from timeline.V_USER_SOURCE_QUALIFICATION T12
) vkpi
right outer join 
(
	select t2.kpiname, t1.userid, t2.weight from
	       (select distinct user_id userid from timeline.T_CHALLENGE_USER) t1, timeline.t_kpi_weights t2
) vw
on (vkpi.kpiname=vw.kpiname and vw.userid=vkpi.userid);


/**************************************************************************************************************************** 
 Funktion: Zusammenfassung aller gewichteten KPIs durch Berechnung des gewichteten Mittelwerts 
 Die View liefert den Input für die manuelle Begutachtung.
 Nur die User aus der MBU werden bewertet.
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
******************************************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_BOTLIST CASCADE;
CREATE VIEW timeline.V_USER_BOTLIST as 
select USERID, TOTALBOTSCORE from
       (
       select 
       USERID,
       sum(botscore*weight)/cast(sum(weight) as real) TOTALBOTSCORE
       from timeline.V_USER_ALLKPIDATA
       group by userid
       ) v_allerg
order by TOTALBOTSCORE desc;

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
CREATE table timeline.T_KPI_KOFLAGS (
KPINAME varchar(100) PRIMARY KEY
);

INSERT INTO timeline.T_KPI_KOFLAGS(KPINAME) VALUES ('KOSOURCEQUALIFICATION');


/********************************************************************************************************
 Indikator "Source-Feld" - KO- Bot-Score
 Berechnung: Mehrheitsentscheidung über alle Tweets, d.h. wenn mehr als 50% das KO-Flag haben.
 Die Subquery v_tweets dient v.a. dazu, doppelte Status-IDs zu behandeln.
 Die KPI wird nur für User berechnet, die überhaupt Tweets abgesetzt haben
********************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KO_SOURCE_QUALIFICATION CASCADE;
CREATE VIEW timeline.V_USER_KO_SOURCE_QUALIFICATION as
select 
       cast('KOSOURCEQUALIFICATION' as varchar(100)) KPINAME, 
       USERID,
       case when avg(v_tweets.KOBOTSCORE)>0.5 THEN 1.0
       else 0.0 end KOBOTSCORE  
from
(
	select ts.id, 
	       min(ts.status_user_id) userid, 
	       max(coalesce(tsq.kobotscore,0)) kobotscore
	from timeline.t_status ts left outer join timeline.t_source_qualification tsq
	on (ts.status_source=tsq.source)
	group by ts.id
) v_tweets
group by USERID;


/********************************************************************************************************* 
 Funktion: Zusammenfassung aller KO-KPI-Kennzahlen über alle User in eine Liste.
 Nur die User aus der MBU werden bewertet.
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
**********************************************************************************************************/
DROP VIEW IF EXISTS timeline.V_USER_KOKPIDATA CASCADE;
CREATE VIEW timeline.V_USER_KOKPIDATA as 
select vw.kpiname, vw.userid, vkpi.kobotscore kobotscore from 
(
	select t1.kpiname, t1.userid, t1.kobotscore from timeline.V_USER_KO_SOURCE_QUALIFICATION T1
) vkpi
right outer join 
(
	select t2.kpiname, t1.userid from
	       (select distinct user_id userid from timeline.T_CHALLENGE_USER) t1, timeline.t_kpi_koflags t2
) vw
on (vkpi.kpiname=vw.kpiname and vw.userid=vkpi.userid);

/******************************************************************************************************** 
 Funktion: Berechnung des KO-Flags pro User über alle KO-Kennzahlen hinweg 
 Rechenregel: max(Botscore_KO), was einer Ver-ODER-ung der Flags entspricht.
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
*********************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_KOBOTLIST CASCADE;
CREATE VIEW timeline.V_USER_KOBOTLIST as 
select USERID, TOTALKOBOTSCORE from
       (
       select 
       USERID,
       max(kobotscore) TOTALKOBOTSCORE
       from timeline.V_USER_KOKPIDATA
       group by userid
       ) v_allerg
order by TOTALKOBOTSCORE desc;


/********************************************************************************************************* 
 Funktion: Zusammenfassung aller Ergebnisse (gewichteter Mittelwert und KO-Flag) pro User.
 Rechenregel: greatest(Botscore_gewichtet, Botscore_KO)
 Nur die User aus der MBU werden bewertet.
 Der Full Outer Join ist nur sicherheitshalber, falls bei der KPI-Berechnung doch irgendwo ein User vergessen wurde.
**********************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_INTEGRATE_SCORES CASCADE;
CREATE VIEW timeline.V_USER_INTEGRATE_SCORES as 
select 
       coalesce(tkb.userid, tb.userid) USERID,
       greatest(tkb.totalkobotscore,tb.totalbotscore) TOTALSCORE
from timeline.V_USER_KOBOTLIST tkb full outer join timeline.V_USER_BOTLIST tb
on (tkb.userid=tb.userid)
order by TOTALSCORE desc;


/**************************************************************************************************************************** 
 Exportview für die Abgabe des Botscore 
 ACHTUNG: sobald eine KPI NULL ist, wird auch der Botscore NULL.
 TODO: anzupassen an neue Views
******************************************************************************************************************************/
DROP VIEW IF EXISTS  timeline.V_USER_EXPORT CASCADE;
CREATE VIEW timeline.V_USER_EXPORT as 
select v_mbc_ids.user_id, v_mbc_ids.parent_screen_name, v_mbc_ids.parent_id, v_allerg.TOTALSCORE botscore from
       (
       select 
       USERID, TOTALSCORE
       from timeline.V_USER_INTEGRATE_SCORES
       ) v_allerg 
       right outer join 
       (select user_id, parent_screen_name,parent_id from timeline.t_challenge_user) v_mbc_ids 
       on (v_allerg.userid=v_mbc_ids.user_id);


