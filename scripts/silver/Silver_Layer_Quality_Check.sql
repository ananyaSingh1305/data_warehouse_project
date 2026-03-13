SELECT * FROM DataWarehouse.bronze_crm_cust_info;

use DataWarehouse;

-- Quality Check 1
-- Check for Nulls or duplicates in Primary Key
-- Primary key must be unique and not null.
-- Expectation : No Result

SELECT cst_id, count(*) FROM bronze_crm_cust_info
GROUP BY cst_id
HAVING count(*)>1 OR cst_id IS NULL;

-- There are NULL and duplicate Primary Key.
SELECT * FROM DataWarehouse.bronze_crm_cust_info where cst_id=29449;
SELECT * FROM DataWarehouse.bronze_crm_cust_info where cst_id=29433;
SELECT * FROM DataWarehouse.bronze_crm_cust_info where cst_id=29466;
SELECT * FROM DataWarehouse.bronze_crm_cust_info where cst_id=29473;
SELECT * FROM DataWarehouse.bronze_crm_cust_info where cst_id=29483;
SELECT * FROM DataWarehouse.bronze_crm_cust_info where cst_id=0;

-- Take latest date value as it has proper entries.

select * from
(select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_row
from DataWarehouse.bronze_crm_cust_info) t
where t.flag_row=1 AND t.cst_id!=0;

-- Number Of Rows: 18484

-- Quality Check 2
-- Check for unwanted spaces
-- There should be no unwanted spaces in the start and end of the string.
-- Expectation : No Result

SELECT cst_firstname FROM bronze_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
-- There are few name, similarly check for other columns that have string values.

SELECT cst_lastname FROM bronze_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_marital_status FROM bronze_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_gndr FROM bronze_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
-- There are few values in each case. We need to remove unwanted spaces to make column quality acceptable.

-- Solution

select cst_id, cst_key, TRIM(cst_firstname), TRIM(cst_lastname), TRIM(cst_marital_status), TRIM(cst_gndr), cst_create_date
from bronze_crm_cust_info;

-- COMBINING QUALITY CHECK 1 & 2


SELECT 
t.cst_id, 
t.cst_key, 
TRIM(t.cst_firstname), 
TRIM(t.cst_lastname), 
TRIM(t.cst_marital_status), 
TRIM(t.cst_gndr), 
t.cst_create_date
FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_row
FROM DataWarehouse.bronze_crm_cust_info) t
WHERE t.flag_row=1 AND t.cst_id!=0;

-- Number Of Rows: 18484

-- Quality Check 3
-- Check the consistency of values in low cardinality columns
-- Data Standardization and Consistency

select distinct cst_marital_status from bronze_crm_cust_info;
SELECT cst_marital_status,COUNT(*) FROM bronze_crm_cust_info GROUP BY cst_marital_status;

select distinct cst_gndr from bronze_crm_cust_info;
SELECT cst_gndr,COUNT(*) FROM bronze_crm_cust_info GROUP BY cst_gndr;


select 
cst_id, 
cst_key, 
TRIM(cst_firstname), 
TRIM(cst_lastname),
CASE 
WHEN TRIM(cst_marital_status)='M' THEN 'Married'
WHEN TRIM(cst_marital_status)='S' THEN 'Single'
ELSE 'N/A'
END,
CASE 
WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
ELSE 'N/A'
END,
cst_create_date
from bronze_crm_cust_info;

-- COMBINING QUALITY CHECK 1,2,3

SELECT 
t.cst_id, 
t.cst_key, 
TRIM(t.cst_firstname), 
TRIM(t.cst_lastname), 
CASE 
WHEN TRIM(cst_marital_status)='M' THEN 'Married'
WHEN TRIM(cst_marital_status)='S' THEN 'Single'
ELSE 'N/A'
END AS cst_marital_status,
CASE 
WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
ELSE 'N/A'
END AS cst_gndr,
t.cst_create_date
FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_row
FROM DataWarehouse.bronze_crm_cust_info) t
WHERE t.flag_row=1 AND t.cst_id!=0;

-- Number Of Rows: 18484

-- Quality Check 4
-- Check the date colum has date datatype and not varchar.
-- Data Standardization and Consistency

describe bronze_crm_cust_info;
-- How to convert a column to date format.

/* Create a new DATE column and convert into it.

ALTER TABLE your_table ADD COLUMN new_date DATE;

Then:

UPDATE your_table
SET new_date = STR_TO_DATE(old_varchar_column, '%d-%m-%Y');
*/

/* ---------------------------------------------------------------------------------------- */

-- INSERT INTO SILVER LAYER

INSERT INTO silver_crm_cust_info(
cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

SELECT 
t.cst_id, 
t.cst_key, 
TRIM(t.cst_firstname), 
TRIM(t.cst_lastname), 
CASE 
WHEN TRIM(cst_marital_status)='M' THEN 'Married'
WHEN Tsilver_crm_cust_infoRIM(cst_marital_status)='S' THEN 'Single'
ELSE 'N/A'
END AS cst_marital_status,
CASE 
WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
ELSE 'N/A'
END AS cst_gndr,
t.cst_create_date
FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_row
FROM DataWarehouse.bronze_crm_cust_info) t
WHERE t.flag_row=1 AND t.cst_id!=0;

-- Once inserted, do all quality checks again!!

SELECT cst_id, count(*) FROM silver_crm_cust_info
GROUP BY cst_id
HAVING count(*)>1 OR cst_id IS NULL;

SELECT cst_firstname FROM silver_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
-- There are few name, similarly check for other columns that have string values.

SELECT cst_lastname FROM silver_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_marital_status FROM silver_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_gndr FROM silver_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

select distinct cst_marital_status from silver_crm_cust_info;
SELECT cst_marital_status,COUNT(*) FROM silver_crm_cust_info GROUP BY cst_marital_status;

select distinct cst_gndr from silver_crm_cust_info;
SELECT cst_gndr,COUNT(*) FROM silver_crm_cust_info GROUP BY cst_gndr;