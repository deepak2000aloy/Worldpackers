select 
	listing_id,
	trim(REGEXP_REPLACE(title, '[^\u0000-\u007F]+', '', 'g')) as Title,
	host_type,
	split_part(location,',',1) as city,
	split_part(location,',',2) as country,
	volunteering_work,
	working_hours,
	minimum_stay,
	accommodation,
	food,
	rating,
	image_url
from worldpackers;


SELECT  w.listing_id,
		trim(REGEXP_REPLACE(title, '[^\u0000-\u007F]+', '', 'g')) as Title,
		host_type,
		split_part(location,',',1) as city,
		split_part(location,',',2) as country,
		REGEXP_REPLACE(working_hours, '\D', '', 'g')::int AS hours_per_week,
    	minimum_stay,
	accommodation,
	NULLIF(food, 'None') AS food,
	NULLIF(rating, 'None')::float AS rating,
    	convert_to_days(
        CAST(nums.min_num AS int),
        CASE
            WHEN w.minimum_stay ILIKE '%day%' THEN 'day'
            WHEN w.minimum_stay ILIKE '%week%' THEN 'week'
            WHEN w.minimum_stay ILIKE '%month%' THEN 'month'
            ELSE NULL
        END
    ) AS min_days,
	image_url
FROM
    worldpackers w
    LEFT JOIN LATERAL (
        SELECT
            (regexp_matches(w.minimum_stay, '\d+', 'g'))[1] AS min_num
    ) nums ON TRUE; 


SELECT 
  REGEXP_REPLACE(working_hours, '\D', '', 'g')::int AS hours_per_week from worldpackers;


select NULLIF(rating, 'None')::float AS clean_rating from worldpackers;
