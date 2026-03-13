SELECT * FROM DataWarehouse.bronze_erp_loc_a101;

-- QUALITY CHECK 1
-- Replace - in CID with nothing.

SELECT 
REPLACE(CID,'-','') AS CID,
CNTRY
FROM DataWarehouse.bronze_erp_loc_a101;

-- QUALITY CHECK 2
-- Making sure there is no invalid CID.

SELECT 
REPLACE(CID,'-','') AS CID,
CNTRY
FROM DataWarehouse.bronze_erp_loc_a101
WHERE REPLACE(CID,'-','') NOT IN (SELECT cst_key FROM silver_crm_cust_info);

-- QUALITY CHECK 3
-- Making sure key column is NOT NULL

SELECT 
REPLACE(CID,'-','') AS CID,
CNTRY
FROM DataWarehouse.bronze_erp_loc_a101 WHERE CID IS NULL;

-- QUALITY CHECK 4
-- Looking for invalid/NULL CNTRY values
-- Removal of Special characters

SELECT 
DISTINCT CNTRY, length(CNTRY), length(TRIM(CNTRY)), length(TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')))
FROM DataWarehouse.bronze_erp_loc_a101;

SELECT 
REPLACE(CID,'-','') AS CID,
CASE 
WHEN TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'DE%' THEN 'Germany'
WHEN TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'US%' OR TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'USA%' THEN 'United States'
WHEN ((TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) ='') OR (TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) IS NULL)) THEN 'N/A'
ELSE TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', ''))
END AS CNTRY
FROM DataWarehouse.bronze_erp_loc_a101;


-- DDL

DROP TABLE IF EXISTS silver_erp_loc_a101;

CREATE TABLE silver_erp_loc_a101(
    CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver_erp_loc_a101(CID, CNTRY)
SELECT 
REPLACE(CID,'-','') AS CID,
CASE 
WHEN TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'DE%' THEN 'Germany'
WHEN TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'US%' OR TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'USA%' THEN 'United States'
WHEN ((TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) ='') OR (TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) IS NULL)) THEN 'N/A'
ELSE TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', ''))
END AS CNTRY
FROM DataWarehouse.bronze_erp_loc_a101;


-- QUALITY CHECK AGAIN!!

SELECT * FROM silver_erp_loc_a101;

SELECT 
DISTINCT CNTRY, length(CNTRY)
FROM DataWarehouse.silver_erp_loc_a101;

/*
CID: Removed invalid values.
CNTRY: Data Standardization and Removal of special characters and Handling missing values.
*/