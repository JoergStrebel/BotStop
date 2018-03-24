/***************************
 Script zum Update von timeline.t_source_qualification, falls dort neue Einträge dazu kommen, oder geändert werden.

****************************/

--truncate timeline.t_source_qualification;
\COPY timeline.t_source_qualification(source,comment,botscore,kobotscore) FROM '20170902_source_values_extension_import.csv' WITH (FORMAT 'csv', DELIMITER ';', HEADER);


