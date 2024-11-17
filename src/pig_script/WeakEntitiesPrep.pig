-- Register the necessary JAR files

REGISTER '/usr/lib/pig/pig.jar';

REGISTER '/usr/lib/pig/lib/*.jar';



-- Set the default parallel parameter

SET default_parallel 4;



-- Load the raw data

raw_data = LOAD '/user/training/assignment2/input/austlang_dataset_nh.txt' 

    USING PigStorage('\t') AS (

        lng_code:chararray,

        lng_name:chararray,

        lng_synonym:chararray,

        lng_thl:chararray,

        lng_thp:chararray,

        a_lng_lat:chararray,

        a_lng_lng:chararray,

        lng_st:chararray,

        lng_uri:chararray

    );



-- Generate rel_code_name

names = FOREACH raw_data GENERATE 

    lng_code,

    FLATTEN(TOKENIZE(lng_name, '/')) as lng_name;

rel_code_name = FOREACH names GENERATE 

    lng_code as code_id,

    TRIM(lng_name) as name_id;

    

-- Generate rel_code_synonym

synonyms = FOREACH raw_data GENERATE

    lng_code,

    FLATTEN(TOKENIZE(lng_synonym, ',')) as lng_synonym;

rel_code_synonym = FOREACH synonyms GENERATE

    lng_code as code_id,

    TRIM(lng_synonym) as synonym_id;



-- Generate rel_code_thl (thesaurus heading language)

thls = FOREACH raw_data GENERATE

    lng_code,

    FLATTEN(TOKENIZE(lng_thl, '/')) as lng_thl;

rel_code_thl = FOREACH thls GENERATE

    lng_code as code_id,

    TRIM(lng_thl) as thl_id;



-- Generate rel_code_thp (thesaurus heading people)

thps = FOREACH raw_data GENERATE

    lng_code,

    FLATTEN(TOKENIZE(lng_thp, '/')) as lng_thp;

rel_code_thp = FOREACH thps GENERATE

    lng_code as code_id,

    TRIM(lng_thp) as thp_id;



-- Generate rel_code_st (state/territory)

states = FOREACH raw_data GENERATE

    lng_code,

    FLATTEN(TOKENIZE(lng_st, ',')) as lng_st;

rel_code_st = FOREACH states GENERATE

    lng_code as code_id,

    TRIM(lng_st) as st_id;



-- Generate rel_code_uri

rel_code_uri = FOREACH raw_data GENERATE

    lng_code as code_id,

    lng_uri as uri_id;



-- Store results

STORE rel_code_name INTO '/user/training/assignment2/output/weak_entities/rel_code_name' USING PigStorage('\t');

STORE rel_code_synonym INTO '/user/training/assignment2/output/weak_entities/rel_code_synonym' USING PigStorage('\t');

STORE rel_code_thl INTO '/user/training/assignment2/output/weak_entities/rel_code_thl' USING PigStorage('\t');

STORE rel_code_thp INTO '/user/training/assignment2/output/weak_entities/rel_code_thp' USING PigStorage('\t');

STORE rel_code_st INTO '/user/training/assignment2/output/weak_entities/rel_code_st' USING PigStorage('\t');

STORE rel_code_uri INTO '/user/training/assignment2/output/weak_entities/rel_code_uri' USING PigStorage('\t');
