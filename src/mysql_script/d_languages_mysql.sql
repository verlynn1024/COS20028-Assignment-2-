
-- Use the indigenous database

USE indigenous;



-- Query to find languages starting with 'D'

SELECT DISTINCT 

    c.code AS lng_code, 

    n.name AS lng_name, 

    s.st AS lng_st

FROM 

    lng_code c

    JOIN rel_code_name rcn ON c.code = rcn.lng_code

    JOIN lng_name n ON rcn.lng_name = n.name

    JOIN rel_code_st rcs ON c.code = rcs.lng_code

    JOIN lng_st s ON rcs.lng_st = s.st

WHERE 

    n.name LIKE 'D%'

ORDER BY 

    c.code, n.name, s.st;
