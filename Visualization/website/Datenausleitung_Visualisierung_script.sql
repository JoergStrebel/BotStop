/*
	Datenausleitung bzw. Datenquellen für Visualisierung
	Die Datenaufbereitung findet in SQL statt, R macht nur die Visualisierung
*/

/*******************************************************************
 BotScores Stufe 1 für alle Nutzer
 Hilfsview ohne Beschränkungen auf Botscores >0,5
*******************************************************************/

drop view if exists datacollector.V_ANALYTICS_ALLBOTSCORES_STUFE1;

CREATE VIEW datacollector.V_ANALYTICS_ALLBOTSCORES_STUFE1 as
select USERID, TOTALBOTSCORE from
       (select 
       	       v_alldata.USERID,
	       sum(v_alldata.botscore*weight)/sum(weight) TOTALBOTSCORE
	from 
	       datacollector.V_USER_ALLKPIDATA_STUFE1 v_alldata group by userid) v_allerg;


--\copy (SELECT * FROM datacollector.V_ANALYTICS_ALLBOTSCORES_STUFE1) TO './V_ANALYTICS_ALLBOTSCORES_STUFE1.csv' with (FORMAT CSV, HEADER true, QUOTE '"')

/*******************************************************************
 BotScores Stufe 2 für alle Nutzer
*******************************************************************/

drop view if exists timeline.V_ANALYTICS_ALLBOTSCORES_STUFE2;

CREATE VIEW timeline.V_ANALYTICS_ALLBOTSCORES_STUFE2 as
select USERID,
       sum(botscore*weight)/sum(weight) TOTALBOTSCORE
from 
     (select userid, botscore, weight from  timeline.V_USER_ALLKPIDATA_STUFE2
	    union
	    select userid, botscore, weight from  timeline.V_USER_COMMONKPIDATA_STUFE1) vall
       group by userid;

drop table if exists timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2;
CREATE unlogged table timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2 as
select userid, totalbotscore from timeline.V_ANALYTICS_ALLBOTSCORES_STUFE2;


/*******************************************************************
* Zurückrechnung: 
* Welche Bot-UserId gehört zu welchem Thema? 
* Wieviele Nutzer, wieviele Bots pro Thema?
* 
*******************************************************************/

drop view if exists timeline.V_ANALYTICS_BOT_PER_SESSION;

CREATE VIEW timeline.V_ANALYTICS_BOT_PER_SESSION as
select tl.track_topics, count(distinct ul.userid) Useranzahl, count(distinct bl.userid) Botanzahl from
(select distinct status_user_id userid, datacollsession_id from datacollector.t_status) ul
left outer join 
(select distinct userid from timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2 where TOTALBOTSCORE>0.5) bl
on (bl.userid=ul.userid)
left outer join
(select distinct ID,track_topics from datacollector.t_datacollparameter) tl
on (tl.ID=ul.datacollsession_id)
group by tl.track_topics;


drop table if exists timeline.MV_ANALYTICS_BOT_PER_SESSION;
CREATE unlogged table timeline.MV_ANALYTICS_BOT_PER_SESSION as
select track_topics, Useranzahl, Botanzahl from timeline.V_ANALYTICS_BOT_PER_SESSION;


/*******************************************************************
* Zurückrechnung:
* Zeitverlauf pro Thema, also ein Chart mit den Daten pro Tag:
* Anzahl Tweets, 
* Anzahl unterschiedl. User, 
* Anzahl Tweets von Bots (gemäß Stufe 2 Erkennung)
*******************************************************************/

drop view if exists timeline.V_ANALYTICS_COUNT_PER_DAY;

CREATE VIEW timeline.V_ANALYTICS_COUNT_PER_DAY as
select 
       tl.track_topics, 
       tweetl.tweetdatum, 
       count(distinct tweetl.userid) Useranzahl, 
       count(distinct tweetl.tweetid) Tweetanzahl, 
       count(distinct tweetl.tweetid) FILTER (WHERE bl.userid is not null) BotTweetAnzahl 
from
	(select distinct id tweetid, date_trunc('day', created_at) tweetdatum, status_user_id userid, datacollsession_id dcsid from datacollector.t_status) tweetl
	left outer join 
	(select distinct userid from timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2 where TOTALBOTSCORE>0.5) bl
	on (bl.userid=tweetl.userid)
	left outer join
	(select distinct ID,track_topics from datacollector.t_datacollparameter) tl
	on (tl.ID=tweetl.dcsid)
group by tl.track_topics,tweetl.tweetdatum;

drop table if exists timeline.MV_ANALYTICS_COUNT_PER_DAY;
CREATE unlogged table timeline.MV_ANALYTICS_COUNT_PER_DAY as
select track_topics, tweetdatum, Useranzahl, Tweetanzahl, BotTweetAnzahl from timeline.V_ANALYTICS_COUNT_PER_DAY;



/*******************************************************************
 Zurückrechnung: Welche URLs werden von Bots verwendet?
 Dazu hole ich mir sämtliche Tweets eines Bots und deren dazugehörigen URLs
 Eine Aufspaltung nach URLs pro Thema ist nicht machbar, da in der Timeline keine Themen mehr habe.
 Wenn ein User eine ULR 5x verwendet, dann zählt sie nur als 1.
*******************************************************************/
drop view if exists timeline.V_ANALYTICS_BOTURLS;
CREATE VIEW timeline.V_ANALYTICS_BOTURLS as
select all_status.url, count(*) urlanz from 
(
	select t1.status_user_id,t3.expanded_url url 
	from timeline.T_Status t1 
	     inner join timeline.t_entity t2 on t1.urlentities_id=t2.id
	     inner join timeline.t_url t3 on t2.id=t3.entity_id
	union
	select  t1.status_user_id,t3.expanded_url url 
	from datacollector.T_Status t1 
	     inner join datacollector.t_entity t2 on t1.urlentities_id=t2.id
	     inner join datacollector.t_url t3 on t2.id=t3.entity_id
) all_status 
where 
      all_status.status_user_id in (select distinct userid from timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2 where TOTALBOTSCORE>0.5) 
      and all_status.url is not null
group by all_status.url
order by urlanz desc
limit 30;

--\copy (SELECT * FROM timeline.V_ANALYTICS_BOTURLS) TO './V_ANALYTICS_BOTURLS.csv' with (FORMAT CSV, HEADER true, QUOTE '"')

/*******************************************************************
 Zurückrechnung: Welche Hashtags werden (von Bots) verwendet?
 Dazu hole ich mir sämtliche Tweets eines Bots und deren dazugehörigen URLs
 Wenn ein User einen hashtag 5x verwendet, dann zählt sie nur als 1.
*******************************************************************/
drop view if exists timeline.V_ANALYTICS_BOTHASHTAGS;
CREATE VIEW timeline.V_ANALYTICS_BOTHASHTAGS as
select all_status.httext, count(*) htanz from 
(
	select  t1.status_user_id,t3.httext 
	from timeline.T_Status t1 
	     inner join timeline.t_entity t2 on t1.hashtagentities_id=t2.id
	     inner join timeline.t_hashtag t3 on t2.id=t3.entity_id
	union
	select  t1.status_user_id,t3.httext 
	from datacollector.T_Status t1 
	     inner join datacollector.t_entity t2 on t1.hashtagentities_id=t2.id
	     inner join datacollector.t_hashtag t3 on t2.id=t3.entity_id
) all_status 
where 
      all_status.status_user_id in (select distinct userid from timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2 where TOTALBOTSCORE>0.5) 
      and all_status.httext is not null
group by all_status.httext
order by htanz desc
limit 30;


/*******************************************************************
 Themenspezifische Wörter aus Tweets von Bots für Tag Cloud.
 Holt maximal 2 Mio. Zeichen pro Thema, da die R Wordcloud sonst zu langsam berechnet wird.
*******************************************************************/
drop view if exists timeline.V_ANALYTICS_WORDS;
drop view if exists timeline.V_ANALYTICS_WORDS;
CREATE VIEW timeline.V_ANALYTICS_WORDS as
SELECT 
       t1.track_topics, 
       left(lower(string_agg(t2.status_text,' ') FILTER (WHERE t3.userid is not null)),2000000) bottweets,
       left(lower(string_agg(t2.status_text,' ') FILTER (WHERE t3.userid is null)),2000000) notbottweets  
from 
     datacollector.t_datacollparameter t1 
inner join 
      datacollector.t_status t2 
on t1.id=t2.datacollsession_id 
left outer join
      datacollector.v_user_botlist t3
on t2.status_user_id=t3.userid 
group by t1.track_topics order by 1;


/*******************************************************************
 Holt Inhalt des Source-Feldes für eine manuelle Qualifikation
 Die Ergebnisse werden als CSV ausgegeben.
*******************************************************************/
--\copy (select v_allsource.sourceatt, count(*) anz from (select status_source sourceatt from datacollector.t_status union all select status_source sourceatt from timeline.t_status) v_allsource group by v_allsource.sourceatt order by anz desc) TO './all_source_values.csv' with (FORMAT CSV, HEADER true, QUOTE '"')

