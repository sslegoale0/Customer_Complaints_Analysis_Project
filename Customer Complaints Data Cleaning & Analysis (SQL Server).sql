/* Customer Complaints Data Cleaning */

CREATE DATABASE customer_complaints;



USE customer_complaints;



SELECT *
FROM complaints;



EXEC sp_columns complaints;



/* 1. Removal of duplicate rows. */

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Complaint ID]
ORDER BY [Complaint ID]) AS "Row Number"
FROM complaints
ORDER BY [Row Number] DESC;

WITH duplicate_complaints AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Complaint ID]
ORDER BY [Complaint ID]) AS "Row Number"
FROM complaints)
SELECT *
FROM duplicate_complaints
WHERE [Row Number] > 1;



SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Company],
[Company public response],
[Company response to consumer],
[Consumer consent provided?],
[Consumer disputed?],
[Date Received],
[Date Submitted],
[Issue],
[Product],
[State],
[Sub-issue],
[Sub-product],
[Submitted via],
[Tags],
[Timely response?],
[ZIP code],
[All Complaints (Selected)],
[Number of Complaints],
[Target],
[Time to Receipt]
ORDER BY [Complaint ID]) AS "Row Number"
FROM complaints
ORDER BY [Row Number], [Complaint ID] DESC;

WITH duplicate_complaints AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY [Company],
[Company public response],
[Company response to consumer],
[Consumer consent provided?],
[Consumer disputed?],
[Date Received],
[Date Submitted],
[Issue],
[Product],
[State],
[Sub-issue],
[Sub-product],
[Submitted via],
[Tags],
[Timely response?],
[ZIP code],
[All Complaints (Selected)],
[Number of Complaints],
[Target],
[Time to Receipt]
ORDER BY [Complaint ID]) AS "Row Number"
FROM complaints)
SELECT *
FROM duplicate_complaints
WHERE [Row Number] > 1
ORDER BY [Row Number] DESC;



/* 2. Data formatting & standardisation */

SELECT DISTINCT [Company]
FROM complaints
ORDER BY [Company];



SELECT DISTINCT [Company public response]
FROM complaints
ORDER BY [Company public response];



SELECT DISTINCT [Company response to consumer]
FROM complaints
ORDER BY [Company response to consumer];



SELECT DISTINCT [Complaint ID]
FROM complaints
ORDER BY [Complaint ID];



SELECT DISTINCT [Consumer consent provided?]
FROM complaints
ORDER BY [Consumer consent provided?];



SELECT DISTINCT [Consumer disputed?]
FROM complaints
ORDER BY [Consumer disputed?];



SELECT DISTINCT [Date Received]
FROM complaints
ORDER BY [Date Received];



SELECT DISTINCT [Date Submitted]
FROM complaints
ORDER BY [Date Submitted];



SELECT DISTINCT [Issue]
FROM complaints
ORDER BY [Issue];



SELECT DISTINCT [Product]
FROM complaints
ORDER BY [Product];



SELECT DISTINCT [State]
FROM complaints
ORDER BY [State];



SELECT DISTINCT [Sub-issue]
FROM complaints
ORDER BY [Sub-issue];



SELECT DISTINCT [Sub-product]
FROM complaints
ORDER BY [Sub-product];



SELECT DISTINCT [Submitted via]
FROM complaints
ORDER BY [Submitted via];



SELECT DISTINCT [Tags]
FROM complaints
ORDER BY [Tags];



SELECT DISTINCT [Timely response?]
FROM complaints
ORDER BY [Timely response?];



SELECT DISTINCT [ZIP code]
FROM complaints
ORDER BY [ZIP code];



SELECT DISTINCT [All Complaints (Selected)]
FROM complaints
ORDER BY [All Complaints (Selected)];



SELECT DISTINCT [Number of Complaints]
FROM complaints
ORDER BY [Number of Complaints];



SELECT DISTINCT [Target]
FROM complaints
ORDER BY [Target];



SELECT DISTINCT [Time to Receipt]
FROM complaints
ORDER BY [Time to Receipt];



/* 3. Imputation of null/blank values. */



/* 4. Removal of redundant/irrelevant columns */

