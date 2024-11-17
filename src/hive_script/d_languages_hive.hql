USE indigenous;

SELECT 
    lng_code,
    lng_name,
    lng_st
FROM (
    SELECT DISTINCT
        r1.lng_code,
        l.name as lng_name,
        r2.lng_st
    FROM rel_code_name r1
    JOIN lng_name l ON r1.lng_name = l.name
    JOIN rel_code_st r2 ON r1.lng_code = r2.lng_code
) subq
WHERE lng_name LIKE 'D%'
ORDER BY lng_code, lng_name, lng_st;
