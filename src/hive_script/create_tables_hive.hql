USE indigenous;

SELECT DISTINCT 
    rel_code_name.lng_code, 
    lng_name.name AS lng_name, 
    lng_st.st AS lng_st
FROM 
    `rel code name` AS rel_code_name 
    JOIN `lng_name` AS lng_name ON rel_code_name.lng_name = lng_name.name
    JOIN `rel code st` AS rel_code_st ON rel_code_name.lng_code = rel_code_st.lng_code
    JOIN `lng_st` AS lng_st ON rel_code_st.lng_st = lng_st.st
WHERE 
    lng_name.name LIKE 'D%'
ORDER BY 
    rel_code_name.lng_code, 
    lng_name.name, 
    lng_st.st;