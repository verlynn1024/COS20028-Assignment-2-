-- Load the raw data from HDFS
raw_data = LOAD '/user/training/austlang_dataset_nh.txt' 
    USING PigStorage('\t') 
    AS (
        language_code:chararray, 
        language_name:chararray, 
        language_synonym:chararray, 
        thesaurus_heading_language:chararray, 
        thesaurus_heading_people:chararray, 
        approximate_latitude:chararray, 
        approximate_longitude:chararray, 
        state_territory:chararray, 
        uri:chararray
    );

-- Group the data by language_code and count occurrences
grouped_codes = GROUP raw_data BY language_code;
code_counts = FOREACH grouped_codes GENERATE 
    group AS language_code, 
    COUNT(raw_data) AS occurrence_count;

-- Filter to find any duplicate codes (appearing more than once)
duplicate_codes = FILTER code_counts BY occurrence_count > 1;

-- Display formatted output
-- Displaying code_counts:
DUMP code_counts;

-- Displaying duplicate_codes:
DUMP duplicate_codes;
