USE data_cleaning_project;

select * from layoffs

--Duplucating the table
select * into layoffs_staging
from layoffs
where 1 = 0;

select * from layoffs_staging;

--inserting tables from layoffs table to layoffs_staging
insert into layoffs_staging
select * from layoffs;

--creating unqie id 
select *, ROW_NUMBER() over(partition by company, industry, total_laid_off, percentage_laid_off, [date] order by company ) row_num
from layoffs_staging;

--finding duplicate 
with  duplicate_cte as(
select *, ROW_NUMBER() over(
partition by company, [location], industry, total_laid_off, percentage_laid_off, [date], country,funds_raised_millions  
order by company ) row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num > 1;

--deleting duplicate 
with  duplicate_cte as(
select *, ROW_NUMBER() over(
partition by company, [location], industry, total_laid_off, percentage_laid_off, [date], country,funds_raised_millions  
order by company ) row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1


---standardizing the table

--Triming 
update layoffs_staging
set company = TRIM(company)

--giving similar indusrty same name
update layoffs_staging
set industry = 'crypto'
where industry like 'crypto%';

-- replacing United State. to United State
UPDATE layoffs_staging
SET country = REPLACE(RTRIM(country), '.', '')
WHERE country LIKE 'United States%';

----converting date (nvarchar) to date formate
	--Fix NULL text first
	SELECT DISTINCT date FROM layoffs_staging
	WHERE date IS NOT NULL 
	AND date != 'NULL'
	ORDER BY date;


	UPDATE layoffs_staging
	SET date = NULL
	WHERE date = 'NULL';

	--Add a new clean column
	ALTER TABLE layoffs_staging
	ADD date_clean DATE;

	--Update the new column
	UPDATE layoffs_staging
	SET date_clean = TRY_CONVERT(DATE, date, 101)
	WHERE date IS NOT NULL;

	-- Drop old varchar date column
	ALTER TABLE layoffs_staging
	DROP COLUMN date;

	-- Rename new column to date
	EXEC sp_rename 'layoffs_staging.date_clean', 'date', 'COLUMN';

---Removing nulls 
	--converting total_laid_off column from nvarchar to int
		-- Preview first
		SELECT total_laid_off, TRY_CAST(total_laid_off AS INT)
		FROM layoffs_staging;

		-- Update
		UPDATE layoffs_staging
		SET total_laid_off = TRY_CAST(total_laid_off AS INT); 

		ALTER TABLE layoffs_staging
		ALTER COLUMN total_laid_off INT;

	--converting funds_raised_millions column from nvarchar to int
		-- Preview first
		SELECT funds_raised_millions, TRY_CAST(funds_raised_millions AS INT)
		FROM layoffs_staging;

		-- Update
		UPDATE layoffs_staging
		SET funds_raised_millions = TRY_CAST(funds_raised_millions AS INT); 

		ALTER TABLE layoffs_staging
		ALTER COLUMN funds_raised_millions INT;

	--converting percentage_laid_off column from nvarchar to int
		-- Preview first
		SELECT percentage_laid_off, TRY_CAST(percentage_laid_off AS float)
		FROM layoffs_staging;

		--converting 'NULL' into NULL
		UPDATE layoffs_staging
		SET percentage_laid_off = NULL
		WHERE percentage_laid_off = 'NULL';
		-- Update
		UPDATE layoffs_staging
		SET percentage_laid_off = TRY_CAST(percentage_laid_off AS float); 

		ALTER TABLE layoffs_staging
		ALTER COLUMN percentage_laid_off float;
	
	--updating the industry column filling empty rows with another same comapny indutsry 
		UPDATE t1
		SET t1.industry = t2.industry
		FROM layoffs_staging t1
		JOIN layoffs_staging t2 ON t1.company = t2.company
		WHERE (t1.industry IS NULL OR t1.industry = '' OR t1.industry = 'NULL')
		AND (t2.industry IS NOT NULL AND t2.industry != 'NULL' AND t2.industry != '');

	-- Convert all text 'NULL' to real NULL in industry column
		UPDATE layoffs_staging
		SET industry = NULL
		WHERE industry = 'NULL';

	--deleteing the rows where total_laid_off IS NULL AND percentage_laid_off IS NULL
		delete FROM layoffs_staging
		WHERE total_laid_off IS NULL
		AND percentage_laid_off IS NULL;

		select * FROM layoffs_staging
		WHERE total_laid_off IS NULL
		AND percentage_laid_off IS NULL;




