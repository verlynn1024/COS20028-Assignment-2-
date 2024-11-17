-- Start Hive
hive

-- Drop and recreate database
DROP DATABASE IF EXISTS indigenous CASCADE;
CREATE DATABASE indigenous;
USE indigenous;

-- Create table
CREATE TABLE austlang (
    language_code STRING,
    language_name STRING,
    language_synonym STRING,
    thesaurus_heading_language STRING,
    thesaurus_heading_people STRING,
    approximate_latitude STRING,
    approximate_longitude STRING,
    state_territory STRING,
    uri STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Load data
LOAD DATA INPATH '/user/training/austlang_dataset_nh.txt' INTO TABLE austlang;

-- Create a table to store the results
CREATE TABLE code_counts AS
SELECT language_code, COUNT(*) as code_count
FROM austlang
GROUP BY language_code
HAVING COUNT(*) > 1;

-- Show results
SELECT * FROM code_counts;

-- Optional: Export results to local file
INSERT OVERWRITE LOCAL DIRECTORY '/home/training/language_code_check'
SELECT * FROM code_counts;
