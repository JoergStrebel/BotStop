/*
	Datenausleitung bzw. Datenquellen für Visualisierung
	Die Datenaufbereitung findet in SQL statt, R macht nur die Visualisierung
*/



/*******************************************************************
 Themenspezifische Wörter aus Tweets von Bots für Tag Cloud.
 Holt maximal 2 Mio. Zeichen pro Thema, da die R Wordcloud sonst zu langsam berechnet wird.

#gameinsight kommt nur bei 1415 von 36418 bot tweets vor. klingt nicht entscheidend...
*******************************************************************/

drop view if exists timeline.V_ANALYTICS_WORDS;
CREATE VIEW timeline.V_ANALYTICS_WORDS as
SELECT 
       left(lower(string_agg(t2.status_text,' ') FILTER (WHERE t3.botflag='Bot')),2000000) bottweets,
       left(lower(string_agg(t2.status_text,' ') FILTER (WHERE t3.botflag='Kein Bot')),2000000) notbottweets  
from 
      timeline.t_status t2 inner join
      (select user_id, botscore,case when botscore>0.5 then 'Bot' else 'Kein Bot' end botflag from timeline.t_challenge_user) t3
on t2.status_user_id=t3.user_id
where t2.status_text not like '%#gameinsight%';

/*
SELECT count(t2.status_text)
from 
      timeline.t_status t2 inner join
      (select user_id, botscore,case when botscore>0.5 then 'Bot' else 'Kein Bot' end botflag from timeline.t_challenge_user) t3
on t2.status_user_id=t3.user_id
where  t3.botflag='Bot' and t2.status_text like '%gameinsight%';
*/

