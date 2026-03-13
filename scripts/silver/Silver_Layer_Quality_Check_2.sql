SELECT * FROM DataWarehouse.bronze_crm_prod_info ORDER BY prd_key;

DESCRIBE DataWarehouse.bronze_crm_prod_info;

-- Quality Check 1
-- Check for Nulls or duplicates in Primary Key
-- Primary key must be unique and not null.
-- Expectation : No Result

SELECT prd_id, count(*) 
FROM DataWarehouse.bronze_crm_prod_info 
group by prd_id
having count(*)>1 or prd_id is null;
-- Output: No output; Quality checked.


-- Quality Check 2
-- Break prd_key into 2 parts: cat_id (first 5 positions, with '_') and sales_id

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info
WHERE REPLACE(SUBSTRING(prd_key, 1,5),'-','_') NOT IN (select ID from bronze_erp_px_cat_g1v2);

--

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info
WHERE REPLACE(SUBSTRING(prd_key, 1,5),'-','_') NOT IN (select ID from bronze_erp_px_cat_g1v2);

-- NOTE: CO_PE prod_cat_id is present in bronze_crm_prod_info but not in bronze_erp_px_cat_g1v2.

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info
WHERE SUBSTRING(prd_key, 7,16) NOT IN (select sls_prd_key from bronze_crm_sales_details)
ORDER BY SUBSTRING(prd_key, 7,16);


-- Quality Check 3
-- Check unwanted spaces in prd_nm

SELECT prd_id, prd_nm FROM DataWarehouse.bronze_crm_prod_info WHERE prd_nm != TRIM(prd_nm);
-- No issues.

-- Quality Check 4
-- For prd_cost (number column), find if there are any negative or NULL values.

SELECT prd_id, prd_cost FROM DataWarehouse.bronze_crm_prod_info WHERE prd_cost IS NULL OR prd_cost<1;

-- 2 values, if business demands, we can replace blank/NULL with 0.

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info;

-- Quality Check 5
-- Standardization of prd_line column

SELECT DISTINCT prd_line FROM DataWarehouse.bronze_crm_prod_info;

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
CASE
WHEN TRIM(prd_line)='M' THEN 'Mountain'
WHEN TRIM(prd_line)='R' THEN 'Road'
WHEN TRIM(prd_line)='S' THEN 'Other Sales'
WHEN TRIM(prd_line)='T' THEN 'Touring'
ELSE 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info;



SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
CASE TRIM(prd_line)
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info;

-- Quality Check 6
-- prd_start_dt < prd_end_date
-- As it stores historical data so, end of first history should be younger than start of next record.

SELECT * FROM DataWarehouse.bronze_crm_prod_info 
WHERE prd_start_dt<prd_end_dt
ORDER BY prd_key;

-- OBSERVATION: In all the cases, start date>end date.alter

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
CASE TRIM(prd_line)
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info;


SELECT *, 
ROW_NUMBER() OVER (PARTITION BY prd_key ORDER BY prd_key) AS FLAG,
DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_key, prd_start_dt), INTERVAL 1 DAY) AS LEAD_FOR_END_DATE_2
FROM DataWarehouse.bronze_crm_prod_info 
WHERE prd_key IN ('AC-HE-HL-U509','AC-HE-HL-U509-B','AC-HP-HY-1023-70','AC-LI-LT-T990')
ORDER BY prd_key;

-- 212	AC_HE	HL-U509-R	Sport-100 Helmet- Red	12	Other Sales	2011-07-01	2007-12-28	1
-- 213	AC_HE	HL-U509-R	Sport-100 Helmet- Red	14	Other Sales	2012-07-01	2008-12-27	2
-- 214	AC_HE	HL-U509-R	Sport-100 Helmet- Red	13	Other Sales	2013-07-01	0000-00-00	3
-- As it stores historical data so, end of first history should be younger than start of next record, to avoid overlapping.
-- For last record, it is ok to have end_date as NULL as it means that that prd_cost is still ongoing/current cost.
/* 
For those products 
We completely ignore the given end date.  We derive a new end date by subtracting 1 day from the next start date.
*/

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
CASE TRIM(prd_line)
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'N/A'
END AS prd_line,
prd_start_dt,
DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_key, prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info
ORDER BY prd_cat_id, prd_sales_id;

DESCRIBE DataWarehouse.silver_crm_prod_info;

-- MODIFY DDL OF TABLE:

DROP TABLE IF EXISTS silver_crm_prod_info;

CREATE TABLE silver_crm_prod_info(
    prd_id INT,
    prd_cat_id VARCHAR(50),
    prd_sales_id VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- INSERT DATA IN SILVER LAYER

INSERT INTO silver_crm_prod_info(prd_id,prd_cat_id,prd_sales_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
CASE TRIM(prd_line)
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'N/A'
END AS prd_line,
prd_start_dt,
DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_key, prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info
ORDER BY prd_cat_id, prd_sales_id;

-- Check quality of silver layer

SELECT prd_id, count(*) 
FROM DataWarehouse.silver_crm_prod_info 
group by prd_id
having count(*)>1 or prd_id is null;

SELECT prd_id, prd_nm FROM DataWarehouse.silver_crm_prod_info WHERE prd_nm != TRIM(prd_nm);

SELECT prd_id, prd_cost FROM DataWarehouse.silver_crm_prod_info WHERE prd_cost IS NULL OR prd_cost<1;

SELECT DISTINCT prd_line FROM DataWarehouse.silver_crm_prod_info;

SELECT * FROM DataWarehouse.silver_crm_prod_info 
WHERE prd_start_dt<prd_end_dt
ORDER BY prd_key;

SELECT * FROM DataWarehouse.silver_crm_prod_info;