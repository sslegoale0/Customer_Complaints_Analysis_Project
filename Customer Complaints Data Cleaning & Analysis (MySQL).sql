SET sql_safe_update = 0;

SET sql_mode = "Traditional";

/* ---------------------------------------------------------------------------------------------------------------------------- */

/* Customer Complaints Data Cleaning */

CREATE DATABASE customer_complaints;



USE customer_complaints;



SELECT *
FROM complaints;



DESCRIBE complaints;


/* 1. Removal of duplicate rows. */

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Complaint ID`
ORDER BY `Complaint ID`) AS "Row Number"
FROM complaints
ORDER BY `Row Number` DESC;

WITH duplicate_complaints AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Complaint ID`
ORDER BY `Complaint ID`) AS "Row Number"
FROM complaints)
SELECT *
FROM duplicate_complaints
WHERE `Row Number` > 1;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Company`,
`Company public response`,
`Company response to consumer`,
`Consumer consent provided?`,
`Consumer disputed?`,
`Date Received`,
`Date Submitted`,
`Issue`,
`Product`,
`State`,
`Sub-issue`,
`Sub-product`,
`Submitted via`,
`Tags`,
`Timely response?`,
`ZIP code`,
`All Complaints (Selected)`,
`Number of Complaints`,
`Target`,
`Time to Receipt`
ORDER BY `Complaint ID`) AS "Row Number"
FROM complaints;

WITH duplicate_complaints AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY `Company`,
`Company public response`,
`Company response to consumer`,
`Consumer consent provided?`,
`Consumer disputed?`,
`Date Received`,
`Date Submitted`,
`Issue`,
`Product`,
`State`,
`Sub-issue`,
`Sub-product`,
`Submitted via`,
`Tags`,
`Timely response?`,
`ZIP code`,
`All Complaints (Selected)`,
`Number of Complaints`,
`Target`,
`Time to Receipt`
ORDER BY `Complaint ID`) AS "Row Number"
FROM complaints)
SELECT *
FROM duplicate_complaints
WHERE `Row Number` > 1;


/* 2. Data formatting & standardisation */

SELECT DISTINCT `Company`
FROM complaints
ORDER BY `Company` ASC;



SELECT DISTINCT `Company public response`
FROM complaints
ORDER BY `Company public response` ASC;



SELECT DISTINCT `Company response to consumer`
FROM complaints
ORDER BY `Company response to consumer` ASC;



SELECT DISTINCT `Complaint ID`
FROM complaints
ORDER BY `Complaint ID` ASC;



SELECT DISTINCT `Consumer consent provided?`
FROM complaints
ORDER BY `Consumer consent provided?` ASC;



SELECT DISTINCT `Consumer disputed?`
FROM complaints
ORDER BY `Consumer disputed?` ASC;



SELECT DISTINCT `Date Received`
FROM complaints
ORDER BY `Date Received` ASC;

ALTER TABLE complaints
MODIFY COLUMN `Date Received` DATE;



SELECT DISTINCT `Date Submitted`
FROM complaints
ORDER BY `Date Submitted`;

ALTER TABLE complaints
MODIFY COLUMN `Date Submitted` DATE;



SELECT DISTINCT `Issue`
FROM complaints
ORDER BY `Issue`;



SELECT DISTINCT `Product`
FROM complaints
ORDER BY `Product`;

UPDATE complaints
SET `Product` = 'Cheque'
WHERE `Product` LIKE 'Checking%';



SELECT DISTINCT `State`
FROM complaints
ORDER BY `State`;



SELECT DISTINCT `Sub-issue`
FROM complaints
ORDER BY `Sub-issue`;

UPDATE complaints
SET `Sub-issue` = NULL
WHERE `Sub-issue` = '""';



SELECT DISTINCT `Sub-product`
FROM complaints
ORDER BY `Sub-product`;

UPDATE complaints
SET `Sub-product` = NULL
WHERE `Sub-product` = '""';



SELECT DISTINCT `Submitted via`
FROM complaints
ORDER BY `Submitted via`;



SELECT DISTINCT `Tags`
FROM complaints
ORDER BY `Tags`;



SELECT DISTINCT `Timely response?`
FROM complaints
ORDER BY `Timely response?`;



SELECT DISTINCT `ZIP code`
FROM complaints
ORDER BY `ZIP code`;



SELECT DISTINCT `All Complaints (Selected)`
FROM complaints
ORDER BY `All Complaints (Selected)`;



SELECT DISTINCT `Number of Complaints`
FROM complaints
ORDER BY `Number of Complaints`;



SELECT DISTINCT `Target`
FROM complaints
ORDER BY `Target`;



SELECT DISTINCT `Time to Receipt`
FROM complaints
ORDER BY `Time to Receipt`;



/* 3. Imputation of null/blank values. */



/* 4. Removal of redundant/irrelevant columns */

ALTER TABLE complaints
DROP COLUMN `All Complaints (Selected)`,
DROP COLUMN `Number of Complaints`,
DROP COLUMN `Target`;



/* ----------------------------------------------------------------------------------------------------------------- */

/* Customer Complaints Data Analysis. */

-- Creating a Service column where I group products

SELECT DISTINCT `Product`
FROM complaints;

SELECT DISTINCT `Product`,
(CASE
WHEN `Product` IN ('Cheque and Savings Account', 'Bank account or service') THEN 'Account'
WHEN `Product` IN ('Student loan', 'Vehicle loan or lease', 'Debt Collection') THEN 'Banking'
WHEN `Product` IN ('Credit card', 'Credit card or prepaid card') THEN 'Credit Card'
WHEN `Product` = 'Mortgages' THEN 'Mortgage'
END) AS "Service"
FROM complaints;

ALTER TABLE complaints
ADD `Service` NVARCHAR(255);

UPDATE complaints
SET `Service` = CASE
WHEN `Product` IN ('Cheque and Savings Account', 'Bank account or service') THEN 'Account'
WHEN `Product` IN ('Student loan', 'Vehicle loan or lease', 'Debt Collection') THEN 'Banking'
WHEN `Product` IN ('Credit card', 'Credit card or prepaid card') THEN 'Credit Card'
WHEN `Product` = 'Mortgages' THEN 'Mortgage'
END;



-- Creating a Progress column where I group Company responses to consumer

SELECT DISTINCT `Company response to consumer`
FROM complaints;

SELECT DISTINCT `Company response to consumer`,
(CASE
WHEN `Company response to consumer` = 'Closed with non-monetary relief' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed with monetary relief' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed with explanation' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed with relief' THEN 'Closed'
WHEN `Company response to consumer` = 'Untimely response' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed without relief' THEN 'Closed'
WHEN `Company response to consumer` = 'In progress' THEN 'Pending'
END) AS "Progress"
FROM complaints;

ALTER TABLE complaints
ADD `Progress` NVARCHAR(255);

UPDATE complaints
SET `Progress` = CASE
WHEN `Company response to consumer` = 'Closed with non-monetary relief' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed with monetary relief' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed with explanation' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed with relief' THEN 'Closed'
WHEN `Company response to consumer` = 'Untimely response' THEN 'Closed'
WHEN `Company response to consumer` = 'Closed without relief' THEN 'Closed'
WHEN `Company response to consumer` = 'In progress' THEN 'Pending'
END;



/* 1. Total Complaints*/

SELECT COUNT(DISTINCT `Complaint ID`) AS "Total Calls"
FROM complaints;



/* 2. Account Complaints */

SELECT COUNT(DISTINCT `Complaint ID`) AS "Total Calls"
FROM complaints
WHERE `Service` = 'Account';



/* 3. Banking Complaints */

SELECT COUNT(DISTINCT `Complaint ID`) AS "Total Calls"
FROM complaints
WHERE `Service` = 'Banking';



/* 4. Credit Card Complaints */

SELECT COUNT(DISTINCT `Complaint ID`) AS "Total Calls"
FROM complaints
WHERE `Service` = 'Credit Card';



/* 5. Mortgage Complaints */

SELECT COUNT(DISTINCT `Complaint ID`) AS "Total Calls"
FROM complaints
WHERE `Service` = 'Mortgage';



/* 6. % of Total Closed Complaints */

SELECT 
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Progress` = 'Closed') AS "Total Closed Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Progress` = 'Closed') * 100
/
(COUNT(DISTINCT `Complaint ID`)), 2) AS "% of Total Closed Complaints"
FROM complaints;



/* 7. % of Closed Account Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account' AND `Progress` = 'Closed') AS "Closed Account Complaints",
ROUND((SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account' AND `Progress` = 'Closed') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account'), 2) AS "% of Closed Account Complaints"
FROM complaints;



/* 8. % of Closed Banking Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking' AND `Progress` = 'Closed') AS "Closed Banking Complaints",
ROUND((SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking' AND `Progress` = 'Closed') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking'), 2) AS "% of Closed Banking Complaints"
FROM complaints;



/* 9. % of Closed Credit Card Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Progress` = 'Closed') AS "Closed Credit Card Complaints",
ROUND((SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Progress` = 'Closed') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card'), 2) AS "% of Closed Credit Card Complaints"
FROM complaints;



/* 10. % of Closed Mortgage Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Progress` = 'Closed') AS "Closed Mortgage Complaints",
ROUND((SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Progress` = 'Closed') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage'), 2) AS "% of Closed Mortgage Complaints"
FROM complaints;



/* 11. % of Total Pending Complaints */

SELECT 
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Progress` = 'Pending') AS "Total Pending Complaints",
ROUND((SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Progress` = 'Pending') * 100
/
(COUNT(DISTINCT `Complaint ID`)), 2) AS "% of Total Pending Complaints"
FROM complaints;



/* 12. % of Pending Account Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account' AND `Progress` = 'Pending') AS "Pending Account Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Account' AND `Progress` = 'Pending') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account'), 2) AS "% of Pending Account Complaints"
FROM complaints;



/* 13. % of Pending Banking Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking' AND `Progress` = 'Pending') AS "Pending Banking Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Banking' AND `Progress` = 'Pending') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking'), 2) AS "% of Pending Banking Complaints"
FROM complaints;



/* 14. % of Pending Credit Card Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Progress` = 'Pending') AS "Pending Credit Card Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Progress` = 'Pending') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card'), 2) AS "% of Pending Credit Card Complaints"
FROM complaints;



/* 15. % of Pending Mortgage Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Progress` = 'Pending') AS "Pending Mortgage Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Progress` = 'Pending')*100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage'), 2) AS "% of Pending Mortgage Complaints"
FROM complaints;



/* 16. % of Total Timely Responded Complaints */

SELECT 
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Timely response?` = 'Yes') AS "Total Timely Responded Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Timely response?` = 'Yes') * 100
/
(COUNT(DISTINCT `Complaint ID`)), 2) AS "% of Total Timely Responded Complaints"
FROM complaints;



/* 17. % of Timely Responded Account Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account' AND `Timely response?` = 'Yes') AS "Timely Responded Account Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Account' AND `Timely response?` = 'Yes') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account'), 2) AS "% of Timely Responded Account Complaints"
FROM complaints;



/* 18. % of Timely Responded Banking Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking' AND `Timely response?` = 'Yes') AS "Timely Responded Banking Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Banking' AND `Timely response?` = 'Yes') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking'), 2) AS "% of Timely Responded Banking Complaints"
FROM complaints;



/* 19. % of Timely Responded Credit Card Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Timely response?` = 'Yes') AS "Timely Responded Credit Card Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID]` AS FLOAT)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Timely response?` = 'Yes')*100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card'), 2) AS "% of Timely Responded Credit Card Complaints"
FROM complaints;



/* 20. % of Timely Responded Mortgage Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Timely response?` = 'Yes') AS "Timely Responded Mortgage Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Timely response?` = 'Yes') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage'), 2) AS "% of Timely Responded Mortgage Complaints"
FROM complaints;



/* 21. % of Total Consumer Disputed Complaints */

SELECT 
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Consumer disputed?` = 'Yes') AS "Total Consumer Disputed Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Consumer disputed?` = 'Yes') * 100
/
(COUNT(DISTINCT `Complaint ID`)), 2) AS "% of Total Consumer Disputed Complaints"
FROM complaints;



/* 22. % of Consumer Disputed Account Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account' AND `Consumer disputed?` = 'Yes') AS "Consumer Disputed Account Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Account' AND `Consumer disputed?` = 'Yes')*100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Account'), 2) AS "% of Consumer Disputed Account Complaints"
FROM complaints;



/* 23. % of Consumer Disputed Banking Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking' AND `Consumer disputed?` = 'Yes') AS "Consumer Disputed Banking Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Banking' AND `Consumer disputed?` = 'Yes')*100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Banking'), 2) AS "% of Consumer Disputed Banking Complaints"
FROM complaints;



/* 24. % of Consumer Disputed Credit Card Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Consumer disputed?` = 'Yes') AS "Consumer Disputed Credit Card Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Credit Card' AND `Consumer disputed?` = 'Yes')*100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Credit Card'), 2) AS "% of Consumer Disputed Credit Card Complaints"
FROM complaints;



/* 25. % of Consumer Disputed Mortgage Complaints */

SELECT DISTINCT (SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Consumer disputed?` = 'Yes') AS "Consumer Disputed Mortgage Complaints",
ROUND((SELECT CAST(COUNT(DISTINCT `Complaint ID`) AS FLOAT)
FROM complaints
WHERE `Service` = 'Mortgage' AND `Consumer disputed?` = 'Yes') * 100
/
(SELECT COUNT(DISTINCT `Complaint ID`)
FROM complaints
WHERE `Service` = 'Mortgage'), 2) AS "% of Consumer Disputed Mortgage Complaints"
FROM complaints;
