SELECT
    COALESCE(q1.date, q2.date, q3.date, q4.date, TO_CHAR(NOW()::date, 'dd/mm/yyyy')) AS date,
    COALESCE(q1.de_offers_daily, 0) AS de_offers_daily,
    COALESCE(q2.de_offers_daily_remote, 0) AS de_offers_daily_remote,
    COALESCE(q3.daily_max, 0) AS daily_max,
    COALESCE(q3.daily_min, 0) AS daily_min,
    COALESCE(q3.daily_avg, 0) AS daily_avg,
    COALESCE(q4.overall_max, 0) AS overall_max,
    COALESCE(q4.overall_min, 0) AS overall_min,
    COALESCE(q4.overall_avg, 0) AS overall_avg
FROM (
    SELECT 
        COUNT(*) AS de_offers_daily,
        TO_CHAR(j.date::date, 'dd/mm/yyyy') AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%engineer%' 
		AND TO_CHAR(j.date::date, 'mm/dd/yyyy') = TO_CHAR(NOW()::date, 'mm/dd/yyyy')
    GROUP BY j.date
) AS q1
RIGHT JOIN (
    SELECT 
        COUNT(*) AS de_offers_daily_remote,
        TO_CHAR(j.date::date, 'mm/dd/yyyy') AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%engineer%'
        AND j.region ILIKE '%remote%'
		AND TO_CHAR(j.date::date, 'dd/mm/yyyy') = TO_CHAR(NOW()::date, 'mm/dd/yyyy')
    GROUP BY j.date
) AS q2 ON q1.date = q2.date
RIGHT JOIN (
    SELECT 
        MAX(salary) AS daily_max,
        MIN(salary) AS daily_min,
        AVG(salary) AS daily_avg,
        TO_CHAR(j.date::date, 'dd/mm/yyyy') AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%engineer%'
		AND j.date::date = NOW()::date
    GROUP BY j.date
) as q3 ON q2.date = q3.date
RIGHT JOIN (
    SELECT 
        MAX(salary) AS overall_max,
        MIN(salary) AS overall_min,
        AVG(salary) AS overall_avg,
        TO_CHAR(NOW()::date, 'dd/mm/yyyy') AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%engineer%'
) AS q4 ON q3.date = q4.date
ORDER BY date;