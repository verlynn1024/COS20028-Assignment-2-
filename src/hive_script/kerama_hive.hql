USE indigenous;
SELECT COUNT(DISTINCT c.code) as record_count

FROM indigenous.lng_code c

JOIN indigenous.rel_code_name rcn ON c.code = rcn.lng_code

JOIN indigenous.lng_name n ON rcn.lng_name = n.name

JOIN indigenous.rel_code_synonym rcs ON c.code = rcs.lng_code

JOIN indigenous.lng_synonym syn ON rcs.lng_synonym = syn.synonym

JOIN indigenous.rel_code_st rst ON c.code = rst.lng_code

JOIN indigenous.lng_st s ON rst.lng_st = s.st

JOIN indigenous.rel_code_latlong l ON c.code = l.lng_code

WHERE syn.synonym LIKE '%Kerama%';
SELECT DISTINCT 

    c.code AS lng_code,

    n.name AS lng_name,

    s.st AS lng_st,

    l.a_lng_lat,

    l.a_lng_lng

FROM 

    indigenous.lng_code c

    JOIN indigenous.rel_code_name rcn ON c.code = rcn.lng_code

    JOIN indigenous.lng_name n ON rcn.lng_name = n.name

    JOIN indigenous.rel_code_synonym rcs ON c.code = rcs.lng_code

    JOIN indigenous.lng_synonym syn ON rcs.lng_synonym = syn.synonym

    JOIN indigenous.rel_code_st rst ON c.code = rst.lng_code

    JOIN indigenous.lng_st s ON rst.lng_st = s.st

    JOIN indigenous.rel_code_latlong l ON c.code = l.lng_code

WHERE 

    syn.synonym LIKE '%Kerama%'

ORDER BY 

    lng_code, lng_name, lng_st;

