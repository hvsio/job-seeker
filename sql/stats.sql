SELECT
    NOW()::date  AS date,
    COALESCE(daily_offers.de_offers_daily, 0) AS de_offers_daily,
    COALESCE(daily_remote_offers.de_offers_daily_remote, 0) AS de_offers_daily_remote,
    COALESCE(daily_statistics.daily_max, 0) AS daily_max,
    COALESCE(daily_statistics.daily_min, 0) AS daily_min,
    COALESCE(ROUND(daily_statistics.daily_avg, 2), 0) AS daily_avg,
    COALESCE(overall_statistics.overall_max, 0) AS overall_max,
    COALESCE(overall_statistics.overall_min, 0) AS overall_min,
    COALESCE(ROUND(overall_statistics.overall_avg, 2), 0) AS overall_avg
FROM (
    SELECT 
        COUNT(*) AS de_offers_daily,
        NOW()::date  AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%data engineer%' 
		AND j.date::date = NOW()::date 
) AS daily_offers
LEFT JOIN (
    SELECT 
        COUNT(*) AS de_offers_daily_remote,
        NOW()::date  as date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%data engineer%'
        AND j.region ILIKE '%remote%'
		AND j.date::date = NOW()::date 
) AS daily_remote_offers ON daily_offers.date = daily_remote_offers.date
LEFT JOIN (
    SELECT 
        MAX(salary) AS daily_max,
        MIN(salary) AS daily_min,
        AVG(salary) AS daily_avg,
        NOW()::date AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%data engineer%'
		AND j.date::date = NOW()::date 
) as daily_statistics ON daily_remote_offers.date = daily_statistics.date
LEFT JOIN (
    SELECT 
        MAX(salary) AS overall_max,
        MIN(salary) AS overall_min,
        AVG(salary) AS overall_avg,
        NOW()::date  AS date
    FROM 
        job_data.jobs AS j
    WHERE 
        j.job_title ILIKE '%data engineer%'
) AS overall_statistics ON daily_statistics.date = overall_statistics.date
ORDER BY date;