/*
	Datenausleitung bzw. Datenquellen für Validierung
*/

/*******************************************************************
 Holt Inhalt des Botscore-Feldes für eine manuelle Qualifikation
 Die Ergebnisse werden als CSV ausgegeben.
*******************************************************************/
\copy (select distinct 'https://twitter.com/' || tu.screen_name UserURL, to_char(ts.botscore, 'FM0D00') from timeline.t_challenge_user ts left outer join timeline.t_user tu on (ts.user_id=tu.id) where ts.botscore is not null OFFSET floor(random()*1000) limit 100) TO './user_botscore_eval.csv' with (FORMAT CSV, HEADER true, QUOTE '"')

/*******************************************************************
 Holt neue Inhalte des Source-Feldes für eine manuelle Qualifikation
 Die Ergebnisse werden als CSV ausgegeben.
*******************************************************************/

\copy (select ts.status_source, count(ts.status_source) from timeline.t_status ts left outer join timeline.t_source_qualification tsq on (ts.status_source = tsq.source) where tsq.source is null and ts.status_source is not null group by ts.status_source) TO './20170902_source_extension.csv' with (FORMAT CSV, HEADER true, QUOTE '"')

/*******************************************************************
 Backup der t_challenge_user-Tabelle
*******************************************************************/
\copy (select ID, user_id, parent_screen_name,parent_id, botscore,maxtweetid from timeline.t_challenge_user)  TO './20170908_t_challenge_user.csv' with (FORMAT CSV, HEADER true, QUOTE '"')

/*******************************************************************
 Exportiert die Inhalte der Source-Qualifizierungstabelle
 Die Ergebnisse werden als CSV ausgegeben.
*******************************************************************/

\copy (select source,comment,botscore,kobotscore from timeline.t_source_qualification) TO './20170909_t_source_qualification.csv' with (FORMAT CSV, HEADER true, QUOTE '"', DELIMITER ';')
