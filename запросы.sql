SELECT DISTINCT reason_name
  FROM endpoint_reasons
 WHERE endpoint_id IN (SELECT id
                         FROM endpoints
                        WHERE active = 'true')\
 ORDER BY reason_name;

-- Если endpoint_reasons - таблица событий, а не справочник,
-- то нужно без DISTINCT.
SELECT e.id id,
       e.name name,
       COUNT(DISTINCT reason_name) reasons
  FROM endpoint_reasons er
  JOIN endpoints e
    ON er.endpoint_id = e.id
   AND e.active = 'false'
 GROUP BY e.id, e.name
 ORDER BY id;

SELECT e.id id,
       e.name name,
       reason_hierarchy,
       COUNT() reasons
  FROM endpoint_reasons er
  JOIN endpoints e
    ON er.endpoint_id = e.id
   AND e.active = 'true'
   AND er.reason_name = 'Перебои напряжения'
 GROUP BY e.id, e.name, reason_hierarchy
 ORDER BY id, reason_hierarchy;