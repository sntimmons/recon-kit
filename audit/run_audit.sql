-- Q1: Summary counts by match_source
SELECT
    match_source,
    COUNT(*) AS pair_count
FROM matched_pairs_raw
GROUP BY match_source
ORDER BY pair_count DESC;

-- Q2: Salary / payrate mismatches (both sides non-blank and different)
SELECT
    pair_id,
    match_source,
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    old_salary,
    new_salary,
    old_payrate,
    new_payrate
FROM matched_pairs_raw
WHERE (
    (old_salary  != '' AND new_salary  != '' AND old_salary  != new_salary)
    OR
    (old_payrate != '' AND new_payrate != '' AND old_payrate != new_payrate)
)
LIMIT 200;

-- Q3: Worker status / type mismatches (both sides non-blank and different)
SELECT
    pair_id,
    match_source,
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    old_worker_status,
    new_worker_status,
    old_worker_type,
    new_worker_type
FROM matched_pairs_raw
WHERE (
    (old_worker_status != '' AND new_worker_status != '' AND old_worker_status != new_worker_status)
    OR
    (old_worker_type   != '' AND new_worker_type   != '' AND old_worker_type   != new_worker_type)
)
LIMIT 200;

-- Q4: Position / district / location_state mismatches (both sides non-blank and different)
SELECT
    pair_id,
    match_source,
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    old_position,
    new_position,
    old_district,
    new_district,
    old_location_state,
    new_location_state
FROM matched_pairs_raw
WHERE (
    (old_position      != '' AND new_position      != '' AND old_position      != new_position)
    OR
    (old_district      != '' AND new_district      != '' AND old_district      != new_district)
    OR
    (old_location_state != '' AND new_location_state != '' AND old_location_state != new_location_state)
)
LIMIT 200;

-- Q5: Hire date mismatches (both sides non-blank and different)
SELECT
    pair_id,
    match_source,
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    old_hire_date,
    new_hire_date
FROM matched_pairs_raw
WHERE
    old_hire_date != ''
    AND new_hire_date != ''
    AND old_hire_date != new_hire_date
LIMIT 200;

-- Q6: Hire date mismatch clustering by worker_id prefix
SELECT
    substr(old_worker_id, 1, 5) AS old_id_prefix,
    COUNT(*) AS mismatch_count
FROM matched_pairs_raw
WHERE
    NULLIF(old_hire_date, '') IS NOT NULL
    AND NULLIF(new_hire_date, '') IS NOT NULL
    AND old_hire_date <> new_hire_date
GROUP BY old_id_prefix
ORDER BY mismatch_count DESC;

-- Q7: Salary numeric variance
SELECT
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    CAST(old_salary AS REAL) AS old_salary_num,
    CAST(new_salary AS REAL) AS new_salary_num,
    (CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) AS salary_delta,
    CASE
        WHEN old_salary = '' OR new_salary = '' THEN 'missing'
        WHEN ABS((CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) / NULLIF(CAST(old_salary AS REAL),0)) > 0.10 THEN 'high'
        WHEN ABS((CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) / NULLIF(CAST(old_salary AS REAL),0)) > 0.03 THEN 'medium'
        ELSE 'low'
    END AS severity_band
FROM matched_pairs_raw
WHERE
    NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND old_salary <> new_salary
LIMIT 500;

-- Q8: Salary delta distribution
SELECT
    CASE
        WHEN ABS((CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) / NULLIF(CAST(old_salary AS REAL),0)) >= 0.50 THEN '>=50%'
        WHEN ABS((CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) / NULLIF(CAST(old_salary AS REAL),0)) >= 0.25 THEN '25-49%'
        WHEN ABS((CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) / NULLIF(CAST(old_salary AS REAL),0)) >= 0.10 THEN '10-24%'
        WHEN ABS((CAST(new_salary AS REAL) - CAST(old_salary AS REAL)) / NULLIF(CAST(old_salary AS REAL),0)) >= 0.03 THEN '3-9%'
        ELSE '<3%'
    END AS delta_band,
    COUNT(*) AS cnt
FROM matched_pairs_raw
WHERE
    NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND old_salary <> new_salary
GROUP BY delta_band
ORDER BY cnt DESC;

-- Q9: Salary ratio clustering
SELECT
    ROUND(
        CAST(new_salary AS REAL) / NULLIF(CAST(old_salary AS REAL),0),
        2
    ) AS ratio_rounded,
    COUNT(*) AS cnt
FROM matched_pairs_raw
WHERE
    NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND old_salary <> new_salary
GROUP BY ratio_rounded
HAVING cnt > 50
ORDER BY cnt DESC
LIMIT 50;

-- Q10: Extreme salary shifts (>=50%)
SELECT
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    old_salary,
    new_salary,
    ROUND(
        CAST(new_salary AS REAL) / NULLIF(CAST(old_salary AS REAL),0),
        3
    ) AS ratio,
    old_worker_status,
    new_worker_status,
    old_worker_type,
    new_worker_type
FROM matched_pairs_raw
WHERE
    NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND ABS(
        (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
        / NULLIF(CAST(old_salary AS REAL),0)
    ) >= 0.50
LIMIT 300;

-- Q11: Ratio bins for extreme deltas (>=50%)
WITH extreme AS (
    SELECT
        ROUND(CAST(new_salary AS REAL) / NULLIF(CAST(old_salary AS REAL),0), 2) AS ratio_bin
    FROM matched_pairs_raw
    WHERE
        NULLIF(old_salary, '') IS NOT NULL
        AND NULLIF(new_salary, '') IS NOT NULL
        AND ABS(
            (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
            / NULLIF(CAST(old_salary AS REAL),0)
        ) >= 0.50
)
SELECT ratio_bin, COUNT(*) AS cnt
FROM extreme
GROUP BY ratio_bin
ORDER BY cnt DESC
LIMIT 50;

-- Q12: Top roles driving extreme ratios
SELECT
    old_position,
    new_position,
    COUNT(*) AS cnt,
    ROUND(AVG(CAST(new_salary AS REAL) / NULLIF(CAST(old_salary AS REAL),0)), 3) AS avg_ratio
FROM matched_pairs_raw
WHERE
    NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND ABS(
        (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
        / NULLIF(CAST(old_salary AS REAL),0)
    ) >= 0.50
GROUP BY old_position, new_position
ORDER BY cnt DESC
LIMIT 50;

-- Q13: Does salary align to payrate*2080 on either side?
SELECT
    COUNT(*) AS total_extreme,
    SUM(CASE
            WHEN old_payrate IS NOT NULL AND old_payrate != ''
             AND ABS(CAST(old_salary AS REAL) - (CAST(old_payrate AS REAL) * 2080)) / NULLIF(CAST(old_salary AS REAL),0) < 0.05
            THEN 1 ELSE 0 END) AS old_matches_hourly_2080,
    SUM(CASE
            WHEN new_payrate IS NOT NULL AND new_payrate != ''
             AND ABS(CAST(new_salary AS REAL) - (CAST(new_payrate AS REAL) * 2080)) / NULLIF(CAST(new_salary AS REAL),0) < 0.05
            THEN 1 ELSE 0 END) AS new_matches_hourly_2080
FROM matched_pairs_raw
WHERE
    NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND ABS(
        (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
        / NULLIF(CAST(old_salary AS REAL),0)
    ) >= 0.50;

-- Q14: Same-role extreme deltas
SELECT
    old_worker_id,
    new_worker_id,
    old_full_name_norm,
    old_position,
    new_position,
    old_salary,
    new_salary,
    ROUND(CAST(new_salary AS REAL) / NULLIF(CAST(old_salary AS REAL),0), 3) AS ratio
FROM matched_pairs_raw
WHERE
    old_position = new_position
    AND NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND ABS(
        (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
        / NULLIF(CAST(old_salary AS REAL),0)
    ) >= 0.50
LIMIT 200;

-- Q15: Average salary delta by role for same-role extreme cases
SELECT
    old_position,
    COUNT(*) AS cnt,
    ROUND(AVG(
        CAST(new_salary AS REAL) / NULLIF(CAST(old_salary AS REAL),0)
    ), 3) AS avg_ratio
FROM matched_pairs_raw
WHERE
    old_position = new_position
    AND NULLIF(old_salary, '') IS NOT NULL
    AND NULLIF(new_salary, '') IS NOT NULL
    AND ABS(
        (CAST(new_salary AS REAL) - CAST(old_salary AS REAL))
        / NULLIF(CAST(old_salary AS REAL),0)
    ) >= 0.50
GROUP BY old_position
ORDER BY cnt DESC;
