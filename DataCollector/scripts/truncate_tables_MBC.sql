/**************************************************************************
 Löschskript für Db-Tabellen der MBC
 Nutzt truncate, braucht daher exklusiven Tabellenzugriff, kann daher nicht parallel zum statusFetcher laufen
 **************************************************************************/


begin;
truncate timeline.t_status cascade;
truncate timeline.t_user cascade;
truncate timeline.T_Place cascade;
truncate timeline.T_Hashtag cascade;
truncate timeline.T_URL cascade;
truncate timeline.T_User_Mention cascade;
truncate timeline.T_Media cascade;
truncate timeline.T_Entity cascade;

truncate timeline.T_DataCollParameter cascade;
truncate timeline.T_Geolocation cascade;

--update timeline.T_CHALLENGE_USER set MAXTWEETID=1 where botscore is not NULL;	

\echo Anzahl der Zeilen in timeline.t_challenge_user
select count(*) from timeline.t_challenge_user;

\echo Anzahl der Zeilen in timeline.t_source_qualification
select count(*) from timeline.t_source_qualification;

commit;

Vacuum (analyze);

