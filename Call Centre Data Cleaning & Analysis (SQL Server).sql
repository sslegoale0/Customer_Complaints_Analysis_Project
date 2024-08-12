/* Call Centre Data Cleaning */

CREATE DATABASE call_centre;



USE call_centre;



SELECT *
FROM calls;



EXEC sp_columns calls;



/* 1. Removal of duplicate rows. */

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Id]
ORDER BY [Id]) AS "Row Number"
FROM calls
ORDER BY [Row Number] DESC;

WITH duplicate_calls AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Id]
ORDER BY [Id]) AS "Row Number"
FROM calls)
SELECT *
FROM duplicate_calls
WHERE [Row Number] > 1;



SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Customer Name]
ORDER BY [Customer Name]) AS "Row Number"
FROM calls
ORDER BY [Row Number] DESC;

WITH duplicate_calls AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Customer Name]
ORDER BY [Customer Name]) AS "Row Number"
FROM calls)
SELECT *
FROM duplicate_calls
WHERE [Row Number] > 1;



SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Call Timestamp],
[Call-Centres City],
[Channel],
[City],
[Reason],
[Response Time],
[Sentiment],
[State],
[Call Duration In Minutes],
[Csat Score]
ORDER BY [Id]) AS "Row Number"
FROM calls
ORDER BY [Row Number] DESC;

WITH duplicate_calls AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Call Timestamp],
[Call-Centres City],
[Channel],
[City],
[Reason],
[Response Time],
[Sentiment],
[State],
[Call Duration In Minutes],
[Csat Score]
ORDER BY [Id]) AS "Row Number"
FROM calls)
SELECT *
FROM duplicate_calls
WHERE [Row Number] > 1;



/* 2. Data formatting & standardisation */

SELECT DISTINCT [Id]
FROM calls
ORDER BY [Id];

SELECT DISTINCT [Id],
UPPER([Id]) AS "Call Id"
FROM calls
ORDER BY [Id];

UPDATE calls
SET [Id] = UPPER([ID]);

EXEC sp_rename 'calls.[Id]', 'Call Id', 'COLUMN';

SELECT DISTINCT [Call Id]
FROM calls
ORDER BY [Call Id] ASC;



SELECT DISTINCT [Call Timestamp]
FROM calls
ORDER BY [Call Timestamp];

SELECT DISTINCT [Call Timestamp],
CONVERT(NVARCHAR(255), CONVERT(DATE, [Call Timestamp], 105)) AS "Call Date"
FROM calls
ORDER BY [Call Timestamp];

UPDATE calls
SET [Call Timestamp] = CONVERT(NVARCHAR(255), CONVERT(DATE, [Call Timestamp], 105))
ALTER TABLE calls
ALTER COLUMN [Call Timestamp] DATE;

EXEC sp_rename 'calls.[Call Timestamp]', 'Call Date', 'COLUMN';

SELECT DISTINCT [Call Date]
FROM calls
ORDER BY [Call Date] ASC;



SELECT DISTINCT [Call-Centres City]
FROM calls
ORDER BY [Call-Centres City];

EXEC sp_rename 'calls.[Call-Centres City]', 'Call Centre City', 'COLUMN';

SELECT DISTINCT [Call Centre City]
FROM calls
ORDER BY [Call Centre City];



SELECT DISTINCT [Channel]
FROM calls
ORDER BY [Channel];

UPDATE calls
SET [Channel] = 'Call Centre'
WHERE [Channel] LIKE 'Call-Center';



SELECT DISTINCT [City]
FROM calls
ORDER BY [City];



SELECT DISTINCT [Reason]
FROM calls
ORDER BY [Reason];



SELECT DISTINCT [Response Time]
FROM calls
ORDER BY [Response Time];



SELECT DISTINCT [Sentiment]
FROM calls
ORDER BY [Sentiment];



SELECT DISTINCT [State]
FROM calls
ORDER BY [State];



SELECT DISTINCT [Call Duration In Minutes]
FROM calls
ORDER BY [Call Duration In Minutes];

EXEC sp_rename 'calls.[Call Duration In Minutes]', 'Call Duration (Mins)', 'COLUMN';

SELECT DISTINCT [Call Duration (Mins)]
FROM calls
ORDER BY [Call Duration (Mins)];



SELECT DISTINCT [Csat Score]
FROM calls
ORDER BY [Csat Score];

EXEC sp_rename 'calls.[Csat Score]', 'CSAT Score', 'COLUMN';

SELECT DISTINCT [CSAT Score]
FROM calls
ORDER BY [CSAT Score];



/* 3. Imputation of null/blank values. */

SELECT DISTINCT [Sentiment],
[CSAT Score]
FROM calls
ORDER BY [Sentiment], [CSAT Score];



/* 4. Removal of redundant/irrelevant columns. */

----------------------------------------------------------------------------------------------------------------------

/* Call Centre Data Analysis. */

/* 1. Total number of calls. */

SELECT COUNT(DISTINCT [Call Id]) AS "Total Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii');



/* 2. Total call duration in munutes. */

SELECT SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii');



/* 3. Average call duration in minutes */

SELECT ROUND(AVG([Call Duration (Mins)]), 2) AS "Average Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii');



/* 4. Average CSAT score */

SELECT ROUND(AVG([CSAT Score]), 2) AS "Average CSAT Score"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii');



/* 5. Total calls by call date. */

SELECT [Call Date],
COUNT(DISTINCT [Call Id]) AS "Total Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Call Date]
ORDER BY [Call Date];



/* 6. Total call duration (mins) by call date. */

SELECT [Call Date],
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Call Date]
ORDER BY [Call Date];



/* 7. Total calls by weekday. */

SELECT DATENAME(WEEKDAY, [Call Date]) AS "Dayname",
COUNT( DISTINCT [Call Id]) AS "Total Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY DATEPART(WEEKDAY, [Call Date]), DATENAME(WEEKDAY, [Call Date])
ORDER BY DATEPART(WEEKDAY, [Call Date]);



/* 8. Total call duration (mins) by weekday. */

SELECT DATENAME(WEEKDAY, [Call Date]) AS "Weekday",
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY DATEPART(WEEKDAY, [Call Date]), DATENAME(WEEKDAY, [Call Date])
ORDER BY DATEPART(WEEKDAY, [Call Date]);



/* 9. Total calls by state. */

SELECT [State],
COUNT(DISTINCT [Call Id]) AS "Total Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [State];



/* 10. Total call duration (mins) by state. */

SELECT [State],
COUNT(DISTINCT [Call Id]) AS "Total Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [State];



/* 11. Percentage of calls by the call centre cities.   */

SELECT [Call Centre City],
COUNT(DISTINCT [Call Id]) AS "Total Calls",
COUNT(DISTINCT [Call Id])*100/(SELECT COUNT(DISTINCT [Call Id])
										FROM calls)  AS "% of Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Call Centre City];



/* 12. Percentage of call durations (mins) by the call centre cities.   */

SELECT [Call Centre City],
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)",
ROUND(SUM([Call Duration (Mins)])*100/(SELECT SUM([Call Duration (Mins)])
										FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Call Centre City];



/* 13. Percentage of calls by the channels.   */

SELECT [Channel],
COUNT(DISTINCT [Call Id]) AS "Total Calls",
ROUND(CAST(COUNT(DISTINCT [Call Id]) AS FLOAT)*100/(SELECT COUNT(DISTINCT [Call Id])
										   FROM calls), 2)  AS "% of Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Channel];



/* 14. Percentage of call durations (mins) by the channels.   */

SELECT [Channel],
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)",
ROUND(SUM([Call Duration (Mins)])*100/(SELECT SUM([Call Duration (Mins)])
														 FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Channel];



/* 15. Percentage of calls by the reasons.   */

SELECT [Reason],
COUNT(DISTINCT [Call Id]) AS "Total Calls",
ROUND(CAST(COUNT(DISTINCT [Call Id]) AS FLOAT)*100/(SELECT COUNT(DISTINCT [Call Id])
										   FROM calls), 2) AS "% of Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Reason];



/* 16. Percentage of call durations (mins) by the reasons.   */

SELECT [Reason],
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)",
ROUND(SUM([Call Duration (Mins)])*100/(SELECT SUM([Call Duration (Mins)])
														 FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Reason];



/* 17. Percentage of calls by the response time.   */

SELECT [Response Time],
COUNT(DISTINCT [Call Id]) AS "Total Calls",
ROUND(CAST(COUNT(DISTINCT [Call Id]) AS FLOAT)*100/(SELECT COUNT(DISTINCT [Call Id])
										   FROM calls), 2)  AS "% of Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Response Time];



/* 18. Percentage of call durations (mins) by the response times.   */

SELECT [Response Time],
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)",
ROUND(SUM([Call Duration (Mins)])*100/(SELECT SUM([Call Duration (Mins)])
														 FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Response Time];



/* 19. Percentage of calls by the sentiments.   */

SELECT [Sentiment],
COUNT(DISTINCT [Call Id]) AS "Total Calls",
ROUND(CAST(COUNT(DISTINCT [Call Id]) AS FLOAT)*100/(SELECT COUNT(DISTINCT [Call Id])
										   FROM calls), 2)  AS "% of Calls"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Sentiment];



/* 20. Percentage of call durations (mins) by the sentiments.   */

SELECT [Sentiment],
SUM([Call Duration (Mins)]) AS "Total Call Duration (Mins)",
ROUND(SUM([Call Duration (Mins)])*100/(SELECT SUM([Call Duration (Mins)])
														 FROM calls), 2)  AS "% of Call Duration (Mins)"
FROM calls
WHERE DAY([Call Date]) <> 31 AND [State] NOT IN ('Alaska', 'Hawaii')
GROUP BY [Sentiment];