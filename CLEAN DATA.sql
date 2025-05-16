-- ========================================
-- PROJECT: LAYOFFS DATA CLEANING IN SQL
-- AUTHOR: M.Asad Imran
-- DESCRIPTION:
--   This project involves cleaning a raw layoffs dataset using SQL.
--   Steps include:
--     1. Removing duplicates
--     2. Standardizing inconsistent values
--     3. Handling nulls and blank entries
--     4. Dropping irrelevant columns
-- DATABASE: MySQL
-- ========================================

-- STEP 1: Load raw data into a staging table for cleaning

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging 
LIKE layoffs;


INSERT layoffs_staging
SELECT *
FROM layoffs;

-- STEP 2: Identify duplicate rows using ROW_NUMBER based on all key fields
-- We'll use a CTE to assign row numbers to duplicates

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions)
FROM layoffs_staging
;


WITH cte_sample as
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off
,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging)
-- STEP 3: Delete all rows with row_num > 1 (i.e., duplicates)
DELETE
FROM cte_sample
WHERE row_num > 1;

-- STEP 4: Create a new table (layoffs_staging2) for further cleaning and formatting
-- We'll include a row_num column here for reference

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off
,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;



SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE company = 'Cazoo';

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- 2. Standardizing
SELECT * 
FROM  layoffs_staging2
;
-- STEP 5: Trim extra spaces in company names

SELECT company, (TRIM(company))
FROM  layoffs_staging2
;

UPDATE layoffs_staging2
SET company= (TRIM(company));


-- STEP 6: Standardize inconsistent industry names (e.g., "Crypto", "cryp")

SELECT DISTINCT industry
FROM  layoffs_staging2
;

SELECT *
FROM  layoffs_staging2
WHERE industry LIKE 'cryp%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT location
FROM  layoffs_staging2
;

SELECT DISTINCT country
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States_'
;


UPDATE layoffs_staging2
SET location = 'Dusseldorf'
WHERE location ='DÃ¼sseldorf'
;
UPDATE layoffs_staging2
SET location = 'Malmo'
WHERE location ='MalmÃ¶'
;
UPDATE layoffs_staging2
SET location = 'Florianpolis'
WHERE location ='FlorianÃ³polis'
;

-- STEP 8: Convert date field from text to DATE format

UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`,'%m/%d/%Y')
;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE ;

-- STEP 9: Identify null or blank industries

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbn%'
;



SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ;


SELECT t1.company,t1.location,t2.location,t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON
    t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL   ;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';



SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON
    t1.company=t2.company
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL   ;

-- STEP 10: Backfill missing industry data using self-join on company name

UPDATE  layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON
    t1.company=t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';



SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ;

-- STEP 11: Remove rows with both total_laid_off and percentage_laid_off as NULL

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ;

-- STEP 12: Drop the helper row_num column after deduplication is complete
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2
;


-- Total records before and after cleaning
SELECT COUNT(*) FROM layoffs;
SELECT COUNT(*) FROM layoffs_staging2;

-- Number of nulls remaining in critical fields
SELECT COUNT(*) AS null_industry FROM layoffs_staging2 WHERE industry IS NULL;
