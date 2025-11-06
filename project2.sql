/* =========================================
   CREATE & USE A DEMO DB
========================================= */
IF DB_ID('HRDemo') IS NULL
BEGIN
  CREATE DATABASE HRDemo;
END;
GO
USE HRDemo;
GO

/* =========================================
   STEP 1: CREATE STAGING TABLES
========================================= */
IF OBJECT_ID('dbo.jobs_stage','U') IS NOT NULL DROP TABLE dbo.jobs_stage;
IF OBJECT_ID('dbo.applicants_stage','U') IS NOT NULL DROP TABLE dbo.applicants_stage;

CREATE TABLE dbo.jobs_stage (
  job_id            NVARCHAR(50),
  job_title         NVARCHAR(200),
  dept              NVARCHAR(200),
  location          NVARCHAR(200),
  recruiter         NVARCHAR(200),
  posted_date       NVARCHAR(50),
  target_fill_days  NVARCHAR(50)
);

CREATE TABLE dbo.applicants_stage (
  applicant_id     NVARCHAR(50),
  job_id           NVARCHAR(50),
  applicant_name   NVARCHAR(200),
  email            NVARCHAR(320),
  phone            NVARCHAR(100),
  source           NVARCHAR(200),
  application_date NVARCHAR(50),
  stage            NVARCHAR(50),
  stage_date       NVARCHAR(50),
  status           NVARCHAR(50)
);

/* =========================================
   STEP 2: IMPORT CSVs INTO STAGING
   (my exact file paths)
========================================= */
BULK INSERT dbo.jobs_stage
FROM 'D:\Users\THINKPAD\Downloads\jobs.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0d0a',   
  CODEPAGE = '65001',
  FIELDQUOTE = '"',
  TABLOCK
);

BULK INSERT dbo.applicants_stage
FROM 'D:\Users\THINKPAD\Downloads\applicants.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0d0a',
  CODEPAGE = '65001',
  FIELDQUOTE = '"',
  TABLOCK
);

/* =========================================
   STEP 3: CREATE TYPED RAW TABLES
========================================= */
IF OBJECT_ID('dbo.jobs_raw','U') IS NOT NULL DROP TABLE dbo.jobs_raw;
IF OBJECT_ID('dbo.applicants_raw','U') IS NOT NULL DROP TABLE dbo.applicants_raw;

CREATE TABLE dbo.jobs_raw (
  job_id            NVARCHAR(50) PRIMARY KEY,
  job_title         NVARCHAR(200),
  dept              NVARCHAR(200),
  location          NVARCHAR(200),
  recruiter         NVARCHAR(200),
  posted_date       DATE NULL,
  target_fill_days  INT NULL
);

CREATE TABLE dbo.applicants_raw (
  applicant_id     NVARCHAR(50) PRIMARY KEY,
  job_id           NVARCHAR(50),
  applicant_name   NVARCHAR(200),
  email            NVARCHAR(320),
  phone            NVARCHAR(100),
  source           NVARCHAR(200),
  application_date DATE NULL,
  stage            NVARCHAR(50),
  stage_date       DATE NULL,
  status           NVARCHAR(50)
);

/* =========================================
   STEP 4: LOAD FROM STAGING
========================================= */
INSERT INTO dbo.jobs_raw (
  job_id, job_title, dept, location, recruiter, posted_date, target_fill_days
)
SELECT
  LTRIM(RTRIM(job_id)),
  LTRIM(RTRIM(job_title)),
  LTRIM(RTRIM(dept)),
  LTRIM(RTRIM(location)),
  LTRIM(RTRIM(recruiter)),
  TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(posted_date)), '')),
  TRY_CONVERT(int,  NULLIF(LTRIM(RTRIM(target_fill_days)), ''))
FROM dbo.jobs_stage;

INSERT INTO dbo.applicants_raw (
  applicant_id, job_id, applicant_name, email, phone, source,
  application_date, stage, stage_date, status
)
SELECT
  LTRIM(RTRIM(applicant_id)),
  LTRIM(RTRIM(job_id)),
  LTRIM(RTRIM(applicant_name)),
  LTRIM(RTRIM(email)),
  LTRIM(RTRIM(phone)),
  LTRIM(RTRIM(source)),
  TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(application_date)), '')),
  LTRIM(RTRIM(stage)),
  TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(stage_date)), '')),
  LTRIM(RTRIM(status))
FROM dbo.applicants_stage;

/* =========================================
   STEP 5: 
   - normalize emails/phones/sources
   - fill missing stage_date for Applied
   - dedupe by (job_id, email_clean) 
========================================= */
IF OBJECT_ID('dbo.applicants_clean','U') IS NOT NULL DROP TABLE dbo.applicants_clean;

;WITH normalized AS (
  SELECT
    a.applicant_id,
    a.job_id,
    LTRIM(RTRIM(a.applicant_name)) AS applicant_name,
    LOWER(LTRIM(RTRIM(a.email)))   AS email_clean,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(a.phone, ''))),
      ' ', ''), '-', ''), '(', ''), ')', ''), '.', '') AS phone_clean,
    CASE
      WHEN LOWER(LTRIM(RTRIM(a.source))) IN ('linkedin','li') THEN 'LinkedIn'
      WHEN LOWER(LTRIM(RTRIM(a.source))) LIKE 'indeed%'       THEN 'Indeed'
      WHEN LOWER(LTRIM(RTRIM(a.source))) LIKE 'careers page%' THEN 'Careers'
      WHEN LOWER(LTRIM(RTRIM(a.source))) LIKE 'referral%'     THEN 'Referral'
      ELSE 'Other'
    END AS source_clean,
    a.application_date AS application_date_dt,
    a.stage,
    a.stage_date AS stage_date_dt,
    a.status
  FROM dbo.applicants_raw a
),
with_fill AS (
  SELECT
    applicant_id, job_id, applicant_name, email_clean, phone_clean, source_clean,
    application_date_dt,
    stage,
    COALESCE(stage_date_dt, CASE WHEN stage = 'Applied' THEN application_date_dt END) AS stage_date_filled,
    status
  FROM normalized
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY job_id, email_clean
      ORDER BY COALESCE(stage_date_filled, application_date_dt) DESC, applicant_id DESC
    ) AS rn
  FROM with_fill
)
SELECT
  applicant_id,
  job_id,
  applicant_name,
  email_clean,
  phone_clean,
  source_clean,
  application_date_dt AS application_date,
  stage,
  stage_date_filled    AS stage_date,
  status
INTO dbo.applicants_clean
FROM ranked
WHERE rn = 1;

/* =========================================
   STEP 6: BUILD FACT TABLE 
========================================= */
IF OBJECT_ID('dbo.fact_applications','U') IS NOT NULL DROP TABLE dbo.fact_applications;

SELECT
  j.job_id,
  j.job_title,
  j.dept,
  j.location,
  j.recruiter,
  j.posted_date,
  j.target_fill_days,
  a.applicant_id,
  a.applicant_name,
  a.email_clean,
  a.phone_clean,
  a.source_clean,
  a.application_date,
  a.stage,
  a.stage_date,
  a.status
INTO dbo.fact_applications
FROM dbo.jobs_raw j
LEFT JOIN dbo.applicants_clean a
  ON j.job_id = a.job_id;

/* =========================================
   STEP 7: QUICK VALIDATION
========================================= */
SELECT COUNT(*) AS jobs_rows        FROM dbo.jobs_raw;
SELECT COUNT(*) AS applicants_rows  FROM dbo.applicants_raw;
SELECT COUNT(*) AS clean_rows       FROM dbo.applicants_clean;

SELECT job_id, email_clean, COUNT(*) AS dupes
FROM dbo.applicants_clean
GROUP BY job_id, email_clean
HAVING COUNT(*) > 1;

SELECT source_clean, COUNT(*) AS cnt
FROM dbo.applicants_clean
GROUP BY source_clean
ORDER BY cnt DESC;

SELECT TOP (5) * FROM dbo.fact_applications ORDER BY job_id, applicant_name;