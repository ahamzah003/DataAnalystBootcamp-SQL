-- world_layoffs analysis.
-- By: Abdulrahman Hamzat


-- initial assessment
SELECT *
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates [if any]
-- 2. Standardize the Data
-- 4. Null values or blank values
-- 4. Remove Any Columns

-- Make a copy of the data before cleaning
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Assess the table
SELECT *
FROM layoffs_staging;

-- Populate the empty table with the original data
INSERT layoffs_staging
SELECT *
FROM layoffs; 


-- Create a row count
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Check for duplicate rows
WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
					total_laid_off, percentage_laid_off, `date`, 
                    stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Assess some duplicate rows
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Create an empty table `layoffs_staging2`
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populate the empty table with data and a new column for row count
INSERT layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
					total_laid_off, percentage_laid_off, `date`, 
                    stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Re-assess the duplicate rows
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- delete the duplicated rows
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


SELECT company, TRIM(company)
FROM layoffs_staging2;

-- trim white spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT company
FROM layoffs_staging
ORDER BY 1;

-- re-assess the data
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypt%';

-- standardize all representations with "Crypto"
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like "Crypto%";


SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


SELECT *
FROM layoffs_staging2
WHERE country LIKE "United Sta%"
ORDER BY 1;

-- remove the period symbol
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT `date`
FROM layoffs_staging2;

-- set to date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- set the data type to date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Re-assess the table
SELECT *
FROM layoffs_staging2;

-- set all blank values to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- fill blank spaces and Nulls
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT COUNT(*)
FROM layoffs
WHERE percentage_laid_off IS NULL
AND total_laid_off is null;

-- Delete rows with missing entries
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb' OR company = 'Juul';

-- EDA
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
ORDER BY percentage_laid_off DESC;

SELECT company, SUM(total_laid_off) total_layoffs
FROM layoffs_staging2
WHERE percentage_laid_off = 1
GROUP BY company
ORDER BY 2 DESC
LIMIT 5;

SELECT company, percentage_laid_off, funds_raised_millions
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY 3 DESC
LIMIT 5;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`) start_date, MAX(`date`) end_date
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
LIMIT 5;

SELECT country, SUM(total_laid_off) sum_of_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
LIMIT 10;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off) total_layoffs
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
LIMIT 5;

SELECT company, SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT SUBSTRING(`date`,1,4) AS `year`, SUM(total_laid_off) total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `year`
ORDER BY 2 DESC;


WITH rolling_total AS (
SELECT SUBSTRING(`date`,1,4) AS `year`, SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `year`,`month`
ORDER BY 2 ASC
)
SELECT `MONTH`, total_layoffs , SUM(total_layoffs) OVER(PARTITION BY `year` ORDER BY `MONTH`) rolled_total
FROM rolling_total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 1 ASC;


WITH Company_Year (company, years, total_laid_off) AS 
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank;
-- WHERE ranking <= 5;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               ;



