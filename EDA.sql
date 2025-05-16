-- ========================================================
-- PROJECT: Exploratory Data Analysis (EDA) - Layoffs Data
-- AUTHOR: M.Asad Imran
-- OBJECTIVE: Derive insights from cleaned layoffs data
-- TOOLS: MySQL
-- DATA SOURCE: layoffs_staging2 (cleaned table)
-- ANALYSIS INCLUDES:
--   - Time trends
--   - Company-wise impact
--   - Country/stage-wise trends
--   - Rolling layoffs analysis
-- ========================================================

-- STEP 1: Preview the full cleaned dataset

SELECT * 
FROM layoffs_staging2
;
-- STEP 2: Find max values for layoffs and percentage laid off

SELECT MAX(total_laid_off),MAX(percentage_laid_off) 
FROM layoffs_staging2
;

-- STEP 3: Companies that laid off 100% of their workforce (percentage_laid_off = 1)
-- Ordered by who raised the most funds

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- STEP 4: Total layoffs per company

SELECT company, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- STEP 5: Date range of layoffs (min/max dates)

SELECT MIN(`date`),MAX(`date`) 
FROM layoffs_staging2
;

-- STEP 6: Year-wise total layoffs

SELECT YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY YEAR(`date`)  
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY stage  
ORDER BY 2 DESC;

-- STEP 7: Monthly layoffs (using substring of date)

SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
;

-- STEP 8: Rolling total of layoffs over months and years
-- Shows overall and per-year cumulative trends

WITH Rolling_Total AS
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`,
SUBSTRING(`date`,1,4) AS `YEAR`,
SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`,`YEAR`
ORDER BY 1 ASC
)SELECT `MONTH` ,total_off,
SUM( total_off) OVER(ORDER BY `MONTH`) AS rolling_total,
SUM( total_off) OVER(PARTITION BY`YEAR` ORDER BY `MONTH`) AS rolling_total_per_year
FROM Rolling_Total;

-- STEP 9: Rolling layoffs per month, grouped by country (if needed later)

WITH Rolling_Total AS (
    SELECT 
        YEAR(`date`) AS `YEAR`,
        DATE_FORMAT(`date`, '%Y-%m') AS `MONTH`,
        SUM(total_laid_off) AS total_off,country
    FROM layoffs_staging2
    WHERE `date` IS NOT NULL
    GROUP BY `YEAR`, `MONTH`
    ORDER BY `YEAR`, `MONTH`
)
SELECT 
    `YEAR`,
    `MONTH`,
    total_off,
    SUM(total_off) OVER (PARTITION BY `YEAR` ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

-- STEP 10: Total layoffs by company per year

SELECT company,YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)  
ORDER BY 3 DESC ;

-- STEP 11: Rank companies by layoffs per year and return top 5

WITH Company_Year  (company ,years,sum_total_laid_off) AS
(
SELECT company,YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)  
),Company_year_rank AS
(
SELECT * , DENSE_RANK() OVER (PARTITION BY years ORDER BY sum_total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_year_rank
WHERE Ranking <=5;

-- Total layoffs by industry

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total layoffs by country

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Layoffs per quarter
SELECT QUARTER(`date`) AS quarter, YEAR(`date`) AS year, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`), QUARTER(`date`)
ORDER BY year, quarter;















