-- Reconstruct CCG-level measure numerators and denominators
WITH
  denominator_for_all_months AS (
  SELECT
    stats.month AS month,
    practices.ccg_id AS pct_id,
    SUM(stats.total_list_size / 1000.0) AS denominator
  FROM
    ebmdatalab.hscic.practice_statistics_all_years AS stats
  INNER JOIN
    ebmdatalab.hscic.practices AS practices
  ON
    stats.practice = practices.code
  WHERE
    setting = 4
    AND month >= '{date_from}'
    AND month <= '{date_to}'
  GROUP BY
    month, pct_id
    ),
  numerator AS (
  SELECT
    pct AS pct_id,
    p.month,
    SUM(items) AS items,
    SUM(actual_cost) AS cost
  FROM
    `ebmdatalab.hscic.normalised_prescribing_standard` AS p
  INNER JOIN
    ebmdatalab.hscic.practices AS practices
  ON
    p.practice = practices.code
  WHERE
    setting = 4
    AND p.month >= '{date_from}'
    AND p.month <= '{date_to}'
    AND ({where_condition})
  GROUP BY
    pct_id, p.month)
SELECT
  denominator_for_all_months.month,
  denominator_for_all_months.pct_id,
  COALESCE(items, 0) AS items,
  COALESCE(cost, 0) AS cost,
  denominator
FROM
  denominator_for_all_months
LEFT JOIN
  numerator
ON
  denominator_for_all_months.month = numerator.month
  AND denominator_for_all_months.pct_id = numerator.pct_id
