/* 
   Viewdefinitionen für BotShield zur Implementierung der Indikatoren
   Autor: jstrebel
   KPIs der Stufe 1
   Jede KPI hat einen eindeutigen Namen KPINAME, ein zugeordnetes Gewicht WEIGHT und einen Botscore BOTSCORE pro User USERID
*/

DROP VIEW IF EXISTS 
datacollector.V_USER_KPI_ALLPOSTS,
datacollector.V_USER_KPI_PROFILEAGE,
datacollector.V_USER_KPI_NAMDESCLEN,
datacollector.V_USER_KPI_ANZFOLLOW,
datacollector.V_USER_KPI_TWEETMENTION,
datacollector.V_USER_KPI_STATUSFREQUENCY,
datacollector.V_USER_KPI_REACTIONTIME,
datacollector.V_USER_KPI_DECILPOSTS,
datacollector.V_USER_TOTALBOTSCORE, 
datacollector.V_USER_BOTLIST,
datacollector.V_USER_KPI_LANGUSERTWEETS,
datacollector.V_USER_ALLKPIDATA_STUFE1,
datacollector.V_USER_KPI_TWEETFREQ
CASCADE;

DROP TABLE IF EXISTS
datacollector.T_KPI_WEIGHTS CASCADE;

/* Tabelle mit Gewichtungsfaktoren für die KPI-Berechnung. Jede KPI hat einen eindeutigen Namen*/
create table datacollector.T_KPI_WEIGHTS (
KPINAME varchar(100) PRIMARY KEY,
WEIGHT real
);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('PROFILEAGE',1.0);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('NAMDESCLEN',1.0);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('ANZFOLLOW',1.0);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETMENTION',1.0);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('DECILPOSTS',1.0);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('LANGUSERTWEETS',1.0);
INSERT INTO datacollector.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETFREQ',1.0);

-- Indikator "Alter des Profils" in Tagen
CREATE VIEW datacollector.V_USER_KPI_PROFILEAGE as 
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
		(select ID,min(created_at) MCA from datacollector.t_user group by ID) as R_Mindates) as R_Profage;

-- Indikator "Länge des Profilnamens und Länge des Beschreibungstextes"
CREATE VIEW datacollector.V_USER_KPI_NAMDESCLEN as 
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
	from datacollector.T_USER group by ID) R_PROFDESC;

-- Indikator "Anzahl Follower"
CREATE VIEW datacollector.V_USER_KPI_ANZFOLLOW as 
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
	from datacollector.T_USER group by ID) r_foltofriend;

/**************************************************************************
 * Indikator "Anzahl der direkten Referenz auf einen Nutzer pro Tweet"
 * Retweets ein Problem, da in Retweets der Originaluser erwähnt wird, d.h. da ist immer eine direkte UserMention drin
 * Replys haben das gleiche Problem.
 ***************************************************************************/
CREATE VIEW datacollector.V_USER_KPI_TWEETMENTION as
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
	     (select ts.status_user_id USERID, count(*) ANZAHLTWEETS from datacollector.T_Status ts
	      group by ts.status_user_id) tat 
	 left outer join
	     (select ts.status_user_id USERID, count(*) ANZAHLUM from 
	     	     datacollector.T_Status ts, 
		     datacollector.T_Entity te, 
		     datacollector.T_User_Mention tum
	     where ts.UserMentionEntities_id=te.ID and te.ID=tum.entity_id
     	     group by ts.status_user_id) taum 
	 on tat.USERID=taum.USERID) r_tweetmention;

/* Indikator "Anzahl der Posts im Vergleich zum Durchschnitt aller Posts im Thread/Thema/Gruppe"
   Die Top 5% der Nutzer werden ermittelt
   Indikator verwendet die Session ID zur themenbezogenen Unterscheidung
*/
CREATE VIEW datacollector.V_USER_KPI_DECILPOSTS as
select 
cast('DECILPOSTS' as varchar(100)) KPINAME, USERID, SESSIONID, ANZ, DECIL,
case when DECIL>10 THEN DECIL/20.0
else 0.0 end
BOTSCORE 
from
	(select status_user_id USERID,DataCollSession_id SESSIONID, ANZ, ntile(20) over (order by ANZ ASC) DECIL from  
       	       		(select status_user_id,	DataCollSession_id, count(*) ANZ 
			from datacollector.t_status group by status_user_id,DataCollSession_id) r_ugroup) 
	r_anz;

/* 
   Indikator Profilnamen: Passt die Sprache der gesendeten Nachrichten zur Sprache im Benutzerprofil? (Nr. 8)
*/

CREATE VIEW datacollector.V_USER_KPI_LANGUSERTWEETS as
select 
cast('LANGUSERTWEETS' as varchar(100)) KPINAME, USERID, SUMMATCH/cast(TWEETCOUNT as double precision)  BOTSCORE 
from
(select ID USERID, sum(LANGMATCH) SUMMATCH, count(*) TWEETCOUNT from
(select t1.ID, 
case 
when t2.lang='und' THEN 0.2
when t1.lang=t2.lang THEN 0.0
else 1 end
LANGMATCH 
from datacollector.T_USER T1 inner join datacollector.t_status T2 
on t1.ID=t2.status_user_id and t1.recorded_at=t2.recorded_at and t1.DataCollSession_id=t2.DataCollSession_id) v1 group by ID) v2;

/*
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
 */
CREATE VIEW datacollector.V_USER_KPI_TWEETFREQ as
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
     	  select distinct status_user_id,created_at from datacollector.T_Status
	  ) all_status order by status_user_id asc,created_at asc
) timestamps where firstval is not null) timedist group by USERID;



/* reine Zusammenfassung aller KPIs der Stufe 1 in einer Tabelle
   Enthält alle UserID / KPI-Kombinationen aus Stufe 1
   Dient als Input für Stufe 2
*/
CREATE VIEW datacollector.V_USER_ALLKPIDATA_STUFE1 as 
select vw.kpiname, vw.userid, coalesce(vkpi.botscore,0) botscore, vw.weight from 
(
	select t1.kpiname, t1.userid, t1.botscore from datacollector.V_USER_KPI_PROFILEAGE T1
	union
	select t2.kpiname, t2.userid, t2.botscore from datacollector.V_USER_KPI_NAMDESCLEN T2
	union
	select t3.kpiname, t3.userid, t3.botscore from datacollector.V_USER_KPI_ANZFOLLOW T3
	union
	select t4.kpiname, t4.userid,  t4.botscore from datacollector.V_USER_KPI_TWEETMENTION T4
	union
	select t5.kpiname, t5.userid, max(t5.botscore) from datacollector.V_USER_KPI_DECILPOSTS T5 group by t5.kpiname,t5.userid,t5.sessionid
	union
	select t6.kpiname, t6.userid,  t6.botscore from datacollector.V_USER_KPI_LANGUSERTWEETS T6
	union
	select t7.kpiname, t7.userid,  t7.botscore from datacollector.V_USER_KPI_TWEETFREQ T7
) vkpi
right outer join 
(
	select t2.kpiname, t1.userid, t2.weight from
	       (select distinct status_user_id userid from datacollector.t_status) t1, datacollector.t_kpi_weights t2
) vw
on (vkpi.kpiname=vw.kpiname and vw.userid=vkpi.userid);


/* 
   Funktion: Berechnung des gewichteten Mittelwerts der Stufe 1 und Auswahl der Botkandidaten
   Input View für statusFetcher mit den erkannten Bots aus Stufe 1.
   Bot-Auswahlkriterium siehe Suchstrategie
   Mich interessiert für den BotScore eine Nutzers nur der maximale Rang über alle Sessions.
*/
CREATE VIEW datacollector.V_USER_BOTLIST as 
select USERID, TOTALBOTSCORE from
(select 
v_alldata.USERID,
sum(v_alldata.botscore*weight)/sum(weight) TOTALBOTSCORE -- Berechnung des gewichteten Durchschnitts über alle KPI
from 
datacollector.V_USER_ALLKPIDATA_STUFE1 v_alldata group by userid) v_allerg 
where TOTALBOTSCORE>0.5;








/**********************************************************************
* Stufe 2
*
* Die folgenden Views benötigen die Daten des statusFetchers.
*
*
*
***********************************************************************/

DROP VIEW IF EXISTS 
timeline.V_USER_KPI_ALLPOSTS,
timeline.V_USER_KPI_STATUSFREQUENCY,
timeline.V_USER_KPI_REACTIONTIME, 
timeline.V_USER_BOTLIST,
timeline.V_USER_ALLKPIDATA_STUFE2,
timeline.V_USER_COMMONKPIDATA_STUFE1,
timeline.V_USER_KPI_TWEETLANGS,
timeline.V_USER_KPI_TWEETFREQ,
timeline.V_USER_KPI_TWEETCOUNT,
timeline.V_USER_MAXTWEETID,
timeline.V_CHALLENGE_USER_MAXTWEETID
CASCADE;

DROP TABLE IF EXISTS
timeline.T_KPI_WEIGHTS CASCADE;

/* Tabelle mit Gewichtungsfaktoren für die KPI-Berechnung. Jede KPI hat einen eindeutigen Namen*/
create table timeline.T_KPI_WEIGHTS (
KPINAME varchar(100) PRIMARY KEY,
WEIGHT real
);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('STATUSFREQUENCY',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('REACTIONTIME',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETLANGS',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETFREQ',1.0);
INSERT INTO timeline.T_KPI_WEIGHTS(KPINAME,WEIGHT) VALUES ('TWEETCOUNT',1.0);


/* 
   View zur Identifikation der letzten Tweet ID pro Stufe 1 Botkandidat. 
   Input für Statusfetcher.
   Falls noch kein Eintrag in timeline zu einer UserID besteht, wird 1 als max-id angenommen.
*/
CREATE VIEW timeline.V_USER_MAXTWEETID as 
select coalesce(bl.userid,tl.userid) userid, coalesce(tl.maxid,1) tweetid from 
       (select distinct USERID from datacollector.V_USER_BOTLIST) bl full outer join 
       (select status_user_id userid, max(id) maxid from timeline.t_status group by status_user_id) tl 
       on bl.userid=tl.userid;

/*
   View zur Identifikation der letzten Tweet ID pro User, der im Rahmen der Munich Bot Challenge untersucht werden soll. 
   Input für Statusfetcher.
   Falls noch kein Eintrag in timeline zu einer UserID besteht, wird 1 als max-id angenommen.
 */
CREATE VIEW timeline.V_CHALLENGE_USER_MAXTWEETID as 
select coalesce(ul.user_id,tl.userid) userid, coalesce(tl.maxid,1) tweetid from 
       (select distinct user_id from timeline.T_CHALLENGE_USER) ul full outer join 
       (select status_user_id userid, max(id) maxid from timeline.t_status group by status_user_id) tl 
       on ul.user_id=tl.userid;

       
/* Indikator: Zeitliche Entwicklung der Follower, Friends. 
   Es werden 24 Attribute definiert, die jeweils zeigen, wieviele Tweets der Nutzer in der Stunde abgesetzt hat. 
   created_at enthält die zur Zeitzone UTC passende Zeit.
   Lösung: created_at+make_interval(secs => utcoffset), wo utcoffset aus t_user kommt.
   Annahme: jeder User hat genau eine zugeordnete Zeitzone. Stand 11.06.2017 stimmt das empirisch auch.
*/
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
		select ts.ID, ts.status_user_id,ts.created_at,tu.utcoffset from timeline.T_Status ts inner join timeline.t_user tu on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
		union
		select ts.ID, ts.status_user_id,ts.created_at,tu.utcoffset from datacollector.T_Status ts inner join datacollector.t_user tu on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
		) all_status
	 ) all_dist_status
   group by status_user_id
   ) uuids;


/****************************************************************************************************************************
*
* Indikator "Reaktionszeit bei Antworttweets in Sekunden"
*
******************************************************************************************************************************/
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
		   	select ts.ID, ts.status_user_id, ts.created_at, ts.InReplyToStatusId, tu.utcoffset from timeline.T_Status ts inner join timeline.t_user tu on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
			union
			select ts.ID, ts.status_user_id, ts.created_at, ts.InReplyToStatusId, tu.utcoffset from datacollector.T_Status ts inner join datacollector.t_user tu on (ts.status_user_id=tu.id and ts.recorded_at=tu.recorded_at and ts.DataCollSession_id=tu.DataCollSession_id)
			) all_status
	      )
	      select 		  
       	      t1.status_user_id USERID,
	      extract(epoch from t1.created_at_tz-t2.created_at_tz) antwzeitsec 
	      from all_dist_status t1, all_dist_status t2 
	      where t1.InReplyToStatusId=t2.ID
	      ) r_timeperuser 
	group by USERID) all_data;


/*
 Indikator "Minimaler Zeitabstand zwischen zwei Posts"
 Man braucht immer alle Tweets sowohl aus REST als auch aus Streaming
 Die View liefert einen Botscore für alle User zurück, egal woher, wenn sie mind. zwei Tweets geschrieben haben.
*/
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
     	  select distinct status_user_id,created_at from         
          (                                                
	     select status_user_id,created_at from timeline.T_Status
	     union                                                           
	     select status_user_id,created_at from datacollector.T_Status
	  ) all_status order by status_user_id asc,created_at asc
     ) alldiststatus
) timestamps where firstval is not null) timedist group by USERID;


/* 
   Indikator: Anzahl verschiedener Sprachen (Nr. 18)
   Leider scheint die Erkennung der Tweet-Sprache nicht besonders zuverlässig zu sein.
   Ich kann also nicht einfach die verschiedenen Sprachen eines Nutzers zählen, sondern muss einen relativen Grenzwert festlegen.
   Oder ich schaue mir die Verteilung der versch. Sprachen an
   Statistik: Wieviele Sprachen pro Tweet hat der User?
   Die Twitter Spracherkennung kann bei ca. 6% der Tweets keine Sprache erkennen, d.h. wenn ein user  mehr als das hat, 
   ist er ein Botkandidat. 
   TODO: Alle Tweets ohne Text (z.B. Retweets) sollten aus der Berechnung des Indikators entfernt werden.
   TODO: Entferne alle Sprachen deren Tweetanzahl <5% alle Tweets des Nutzers ist. Sobald die verbleibenden Sprachanzahl > 3 ist, dann setze Botscore auf 1.
*/

CREATE VIEW timeline.V_USER_KPI_TWEETLANGS as
with 
v_dist_status as  
	      (select distinct ID, status_user_id,lang from 
       		   	(
		   	select ID, status_user_id,lang from timeline.T_Status
			union
			select ID, status_user_id,lang from datacollector.T_Status
			) all_status),
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


/*
 Indikator "Anzahl aller  Posts pro Tag"
 Falls der User mehr als 50 Tweets pro Tag abgesetzt hat, dann Botscore=1
*/
CREATE VIEW timeline.V_USER_KPI_TWEETCOUNT as
select 
cast('TWEETCOUNT' as varchar(100)) KPINAME, 
USERID, 
case when max(ANZTWEETS)>50 THEN 1.0
else 0.0 end
BOTSCORE 
from
(select status_user_id USERID, to_char(created_at,'DD.MM.YYYY') DTAG, count(*) ANZTWEETS
from
(select distinct ID, created_at, status_user_id from 
       		   	(
		   	select ID, created_at, status_user_id from timeline.T_Status
			union
			select ID, created_at, status_user_id from datacollector.T_Status
			) v_all_status) v_alldiststatus
group by status_user_id, to_char(created_at,'DD.MM.YYYY')) v_tweets
group by USERID;

/* 
   reine Zusammenfassung aller KPIs der Stufe 2 in einer Tabelle 
   Enthält alle UserID / KPI Kombinationen.
*/
CREATE VIEW timeline.V_USER_ALLKPIDATA_STUFE2 as 
select vw.kpiname, vw.userid, coalesce(vkpi.botscore,0) botscore, vw.weight from 
(
	select t1.kpiname, t1.userid, t1.botscore from timeline.V_USER_KPI_STATUSFREQUENCY T1
	union
	select t2.kpiname, t2.userid, t2.botscore from timeline.V_USER_KPI_REACTIONTIME T2
	union
	select t3.kpiname, t3.userid, t3.botscore from timeline.V_USER_KPI_TWEETLANGS T3
	union
	select t4.kpiname, t4.userid, t4.botscore from timeline.V_USER_KPI_TWEETFREQ T4
	union
	select t5.kpiname, t5.userid, t5.botscore from timeline.V_USER_KPI_TWEETCOUNT T5
) vkpi 
right outer join 
(
	select t2.kpiname, t1.userid, t2.weight from
	       (select distinct status_user_id userid from timeline.t_status) t1, timeline.t_kpi_weights t2 --füge alle User hinzu um eine vollständige Liste zu bekommen.
) vw 
on (vkpi.kpiname=vw.kpiname and vw.userid=vkpi.userid);


/* Einschränkung der Stufe 1 KPIs auf UserIDs, die auch in Stufe 2 vorkommen */

CREATE VIEW timeline.V_USER_COMMONKPIDATA_STUFE1 as 
select t1.status_user_id userid, t2.botscore, t2.weight from
(select distinct status_user_id from timeline.T_STATUS) T1 left outer join 
(select userid, botscore, weight from  datacollector.V_USER_ALLKPIDATA_STUFE1) T2  
on (t1.status_user_id=t2.userid);


/* 
   Funktion: Berechnung des gewichteten Mittelwerts der Stufe 2 und Auswahl der Botkandidaten
   Die View liefert den Input für die manuelle Begutachtung.
   Nur die in Stufe 1 als Bots erkannten UserIDs werden in Stufe 2 weiter bewertet.
*/
CREATE VIEW timeline.V_USER_BOTLIST as 
select USERID, TOTALBOTSCORE from
       (select 
       USERID,
       sum(botscore*weight)/sum(weight) TOTALBOTSCORE
       from 
       	    (select userid, botscore, weight from  timeline.V_USER_ALLKPIDATA_STUFE2
	    union
	    select userid, botscore, weight from  timeline.V_USER_COMMONKPIDATA_STUFE1) vall
       group by userid) v_allerg
where TOTALBOTSCORE>0.5;

