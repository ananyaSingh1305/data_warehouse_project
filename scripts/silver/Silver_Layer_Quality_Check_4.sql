SELECT * FROM DataWarehouse.bronze_erp_cust_az12;

-- QUALITY CHECK 1
-- In column CID, some start with NAS , some don't. 
-- As there is no specification for NAS usage, so we will remove it.

SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END AS CID,
BDATE,
GEN
FROM DataWarehouse.bronze_erp_cust_az12;


-- QUALITY CHECK 2
-- ALL CID are in silver_crm_cust_info

SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END AS CID,
BDATE,
GEN
FROM DataWarehouse.bronze_erp_cust_az12
WHERE (CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END) NOT IN (SELECT DISTINCT cst_key FROM silver_crm_cust_info);

-- All good

-- QUALITY CHECK 3

SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END AS CID,
BDATE,
GEN
FROM DataWarehouse.bronze_erp_cust_az12
WHERE LENGTH(BDATE)!=10 OR BDATE IS NULL;

-- QUALITY CHECK 4

SELECT DISTINCT GEN, length(GEN), length(TRIM(GEN))
FROM DataWarehouse.bronze_erp_cust_az12;

-- There are special characters that TRIM() is unable to remove. So, use LIKE

SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END AS CID,
BDATE,
CASE 
WHEN GEN LIKE 'M%' THEN 'Male'
WHEN GEN LIKE 'F%' THEN 'Female'
ELSE 'N/A'
END AS GEN
FROM DataWarehouse.bronze_erp_cust_az12;


-- DDL

DROP TABLE IF EXISTS silver_erp_cust_az12;

CREATE TABLE silver_erp_cust_az12(
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- INSERT

INSERT INTO silver_erp_cust_az12(CID,BDATE,GEN)

SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END AS CID,
BDATE,
CASE 
WHEN GEN LIKE 'M%' THEN 'Male'
WHEN GEN LIKE 'F%' THEN 'Female'
ELSE 'N/A'
END AS GEN
FROM DataWarehouse.bronze_erp_cust_az12;

--  QUALITY CHECK AGAIN!!

SELECT * FROM silver_erp_cust_az12;

SELECT distinct GEN FROM silver_erp_cust_az12;

/*
CID: Handled invalid values.
GEN: Data Normalization + Handling missing values.
*/




