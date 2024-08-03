SET sql_safe_updates = 0;

SET sql_mode = "Traditional";

---------------------------------------------------------------------------------------------------------------------------------------------

-- Customer Complaints Data Cleaning.

CREATE DATABASE customer_complaints;



-- Imported Customer Complaints data into MySQL using SQLAlchemy.



USE customer_complaints;



SELECT *
FROM complaints;



DESCRIBE complaints;



-- 1. Removal of duplicate rows.

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Complaint ID`) AS "Row Number"
FROM complaints;

WITH duplicate_complaints AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Complaint ID`) AS "Row Number"
FROM complaints
)
SELECT *
FROM duplicate_complaints
WHERE `Row Number` > 1;



-- 2. Data formatting & standardisation.

SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;



SELECT DISTINCT 
FROM calls
ORDER BY ASC;





-- 3. Imputation of null/blank values.




-- 4. Removal of redundant/irrelevant rows.




