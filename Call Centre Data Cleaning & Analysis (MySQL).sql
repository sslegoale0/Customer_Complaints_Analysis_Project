SET sql_safe_update = 0;

SET sql_mode = "Traditional";

/* ---------------------------------------------------------------------------------------------------------------------------- */

/* Call Centre Data Cleaning */

CREATE DATABASE call_centre;



-- Imported Call Centre data into MySQL using SQLAlchemy.



USE call_centre;



SELECT *
FROM calls;



DESCRIBE calls;



/* 1. Removal of duplicate rows. */

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Id`
ORDER BY `Id`) AS "Row Number"
FROM calls
ORDER BY `Row Number` DESC;

WITH duplicate_calls AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Id`
ORDER BY `Id`) AS "Row Number"
FROM calls)
SELECT *
FROM duplicate_calls
WHERE `Row Number` > 1;



SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Customer Name`
ORDER BY `Customer Name`) AS "Row Number"
FROM calls
ORDER BY `Row Number` DESC;

WITH duplicate_calls AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Customer Name`
ORDER BY `Customer Name`) AS "Row Number"
FROM calls)
SELECT *
FROM duplicate_calls
WHERE `Row Number` > 1;



SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Call Timestamp`,
`Call-Centres City`,
`Channel`,
`City`,
`Reason`,
`Response Time`,
`Sentiment`,
`State`,
`Call Duration In Minutes`,
`Csat Score`
ORDER BY `Id`) AS "Row Number"
FROM calls
ORDER BY `Row Number` DESC;

WITH duplicate_calls AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Call Timestamp`,
`Call-Centres City`,
`Channel`,
`City`,
`Reason`,
`Response Time`,
`Sentiment`,
`State`,
`Call Duration In Minutes`,
`Csat Score`
ORDER BY `Id`) AS "Row Number"
FROM calls)
SELECT *
FROM duplicate_calls
WHERE `Row Number` > 1;



/* 2. Data formatting & standardisation */

SELECT DISTINCT `Id`
FROM calls
ORDER BY `Id` ASC;

SELECT DISTINCT `Id`,
UPPER(`Id`) AS "Call Id"
FROM calls
ORDER BY `Id` ASC;

UPDATE calls
SET `Id` = UPPER(`Id`);

ALTER TABLE calls
RENAME COLUMN `Id` TO `Call Id`;

SELECT DISTINCT `Call Id`
FROM calls
ORDER BY `Call Id` ASC;



SELECT DISTINCT `Call Timestamp`
FROM calls
ORDER BY `Call Timestamp` ASC;

SELECT DISTINCT `Call Timestamp`,
STR_TO_DATE(`Call Timestamp`, "%d-%m-%Y", "%Y-%m-%d")
FROM calls
ORDER BY `Call Timestamp` ASC;

UPDATE calls
SET `Call Timestamp` = STR_TO_DATE(`Call Timestamp`, "%d-%m-%Y", "%Y-%m-%d");

ALTER TABLE calls
CHANGE COLUMN `Call Timestamp` `Call Date` DATE NULL;

SELECT DISTINCT `Call Date`
FROM calls
ORDER BY `Call Date` ASC;



SELECT DISTINCT `Call-Centres City`
FROM calls
ORDER BY `Call-Centres City` ASC;

ALTER TABLE calls
RENAME COLUMN `Call-Centres City` TO `Call Centre City`;

SELECT DISTINCT `Call Centre City`
FROM calls
ORDER BY `Call Centre City` ASC;



SELECT DISTINCT `Channel`
FROM calls
ORDER BY `Channel` ASC;

UPDATE calls
SET `Channel` = 'Call Centre'
WHERE `Channel` LIKE 'Call-Center';



SELECT DISTINCT `City`
FROM calls
ORDER BY `City` ASC;



SELECT DISTINCT `Reason`
FROM calls
ORDER BY `Reason` ASC;



SELECT DISTINCT `Response Time`
FROM calls
ORDER BY `Response Time` ASC;



SELECT DISTINCT `Sentiment`
FROM calls
ORDER BY `Sentiment` ASC;



SELECT DISTINCT `State`
FROM calls
ORDER BY `State` ASC;



SELECT DISTINCT `Call Duration In Minutes`
FROM calls
ORDER BY `Call Duration In Minutes` ASC;

ALTER TABLE calls
RENAME COLUMN `Call Duration In Minutes` TO `Call Duration (Mins)`;

SELECT DISTINCT `Call Duration (Mins)`
FROM calls
ORDER BY `Call Duration (Mins)` ASC;



SELECT DISTINCT `Csat Score`
FROM calls
ORDER BY `Csat Score`;

ALTER TABLE calls
RENAME COLUMN `Csat Score` TO `CSAT Score`;

SELECT DISTINCT `CSAT Score`
FROM calls
ORDER BY `CSAT Score` ASC;



/* 3. Imputation of null/blank values. */

SELECT DISTINCT `Sentiment`,
`CSAT Score`
FROM calls
ORDER BY `Sentiment`, `CSAT Score` ASC;



/* 4. Removal of redundant/irrelevant columns. */

----------------------------------------------------------------------------------------------------------------------

/* Call Centre Data Analysis. */

/* 1. Total number of calls. */

SELECT COUNT(DISTINCT `Call Id`) AS "Total Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii');



/* 2. Total call duration in munutes. */

SELECT SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii');



/* 3. Average call duration in minutes */

SELECT ROUND(AVG(`Call Duration (Mins)`), 2) AS "Average Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii');



/* 4. Average CSAT score */

SELECT ROUND(AVG(`CSAT Score`), 2) AS "Average CSAT Score"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii');



/* 5. Total calls by call date. */

SELECT `Call Date`,
COUNT(DISTINCT `Call Id`) AS "Total Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Call Date`
ORDER BY `Call Date`;



/* 6. Total call duration (mins) by call date. */

SELECT `Call Date`,
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Call Date`
ORDER BY `Call Date`;



/* 7. Total calls by weekday. */

SELECT DAYNAME(`Call Date`) AS "Dayname",
COUNT( DISTINCT `Call Id`) AS "Total Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY DAYNAME(`Call Date`), WEEKDAY(`Call Date`)
ORDER BY WEEKDAY(`Call Date`);



/* 8. Total call duration (mins) by weekday. */

SELECT DAYNAME(`Call Date`) AS "Weekday",
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY DAYNAME(`Call Date`), WEEKDAY(`Call Date`)
ORDER BY WEEKDAY(`Call Date`);



/* 9. Total calls by state. */

SELECT `State`,
COUNT(DISTINCT `Call Id`) AS "Total Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `State`;



/* 10. Total call duration (mins) by state. */

SELECT `State`,
COUNT(DISTINCT `Call Id`) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `State`;



/* 11. Percentage of calls by the call centre cities.   */

SELECT `Call Centre City`,
COUNT(DISTINCT `Call Id`) AS "Total Calls",
COUNT(DISTINCT `Call Id`) * 100
/
(SELECT COUNT(DISTINCT `Call Id`)
FROM calls)  AS "% of Total Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Call Centre City`;



/* 12. Percentage of call durations (mins) by the call centre cities.   */

SELECT `Call Centre City`,
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)",
ROUND(SUM(`Call Duration (Mins`) * 100
/
(SELECT SUM(`Call Duration (Mins)`)
FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Call Centre City`;



/* 13. Percentage of calls by the channels.   */

SELECT `Channel`,
COUNT(DISTINCT `Call Id`) AS "Total Calls",
ROUND(COUNT(DISTINCT `Call Id`) * 100
/
(SELECT COUNT(DISTINCT `Call Id`)
FROM calls), 2)  AS "% of Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Channel`;



/* 14. Percentage of call durations (mins) by the channels.   */

SELECT `Channel`,
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)",
ROUND(SUM(`Call Duration (Mins)`) * 100
/
(SELECT SUM(`Call Duration (Mins)`)
FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Channel`;



/* 15. Percentage of calls by the reasons.   */

SELECT `Reason`,
COUNT(DISTINCT `Call Id`) AS "Total Calls",
ROUND(COUNT(DISTINCT `Call Id`) * 100
/
(SELECT COUNT(DISTINCT `Call Id`)
FROM calls), 2) AS "% of Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Reason`;



/* 16. Percentage of call durations (mins) by the reasons.   */

SELECT `Reason`,
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)",
ROUND(SUM(`Call Duration (Mins)`) * 100
/
(SELECT SUM(`Call Duration (Mins)`)
FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Reason`;



/* 17. Percentage of calls by the response time.   */

SELECT `Response Time`,
COUNT(DISTINCT `Call Id`) AS "Total Calls",
ROUND(COUNT(DISTINCT `Call Id`) * 100
/
(SELECT COUNT(DISTINCT `Call Id`)
FROM calls), 2)  AS "% of Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Response Time`;



/* 18. Percentage of call durations (mins) by the response times.   */

SELECT `Response Time`,
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)",
ROUND(SUM(`Call Duration (Mins)`) * 100
/
(SELECT SUM(`Call Duration (Mins)`)
FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Response Time`;



/* 19. Percentage of calls by the sentiments.   */

SELECT `Sentiment`,
COUNT(DISTINCT `Call Id`) AS "Total Calls",
ROUND(COUNT(DISTINCT `Call Id`) * 100
/
(SELECT COUNT(DISTINCT `Call Id`)
FROM calls), 2)  AS "% of Calls"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Sentiment`;



/* 20. Percentage of call durations (mins) by the sentiments.   */

SELECT `Sentiment`,
SUM(`Call Duration (Mins)`) AS "Total Call Duration (Mins)",
ROUND(SUM(`Call Duration (Mins)`) * 100
/
(SELECT SUM(`Call Duration (Mins)`)
FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY(`Call Date`) <> 31 AND `State` NOT IN ('Alaska', 'Hawaii')
GROUP BY `Sentiment`;