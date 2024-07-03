/*Query 1: Maximum Perimeter Mean by Diagnosis*/
SELECT
        diagnosis,
        MAX(perimeter_mean) AS max_perimeter
    FROM
       "BREASTCANCER.CSV"
    GROUP BY
        diagnosis;
/*Query 2: Select Specific Columns for Malignant Cases*/
SELECT
    id,
    diagnosis,
    radius_mean,
    texture_mean,
    area_mean
FROM
   "BREASTCANCER.CSV"
WHERE
    diagnosis = 'M';
/*Query 3: Case Statement for Diagnosis Category*/

SELECT
    id,
    diagnosis,
    CASE
        WHEN diagnosis = 'M' THEN 'Malignant'
        WHEN diagnosis = 'B' THEN 'Benign'
        ELSE 'Unknown'
    END AS diagnosis_category
FROM  "BREASTCANCER.CSV";
/*Query 4: Count of Diagnosis Types (B and M)*/

SELECT
    (SELECT COUNT(*) FROM "BREASTCANCER.CSV" WHERE diagnosis = 'B') AS count_B,
    (SELECT COUNT(*) FROM "BREASTCANCER.CSV" WHERE diagnosis = 'M') AS count_M;
/*Query 5: Rank by Radius Mean*/

SELECT
    id,
    diagnosis,
    radius_mean,
    ROW_NUMBER() OVER (ORDER BY radius_mean DESC) AS rank_by_radius_mean
FROM "BREASTCANCER.CSV";

/*Query 6: Range and Count of Radius Mean*/
SELECT
    CASE
        WHEN radius_mean >= 10 AND radius_mean < 15 THEN '10-15'
        WHEN radius_mean >= 15 AND radius_mean < 20 THEN '15-20'
        WHEN radius_mean >= 20 AND radius_mean < 25 THEN '20-25'
        ELSE 'Other'
    END AS radius_range,
    COUNT(*) AS count_radius
FROM "BREASTCANCER.CSV"
GROUP BY radius_range;

/*Query 7: Radius Mean Greater Than Average Radius Mean*/

SELECT *
FROM "BREASTCANCER.CSV"
WHERE radius_mean > (
    SELECT AVG(radius_mean)
    FROM "BREASTCANCER.CSV"
);
/*Query 8: Update Radius Mean Based on Age Condition*/

UPDATE "BREASTCANCER.CSV" bcd
SET radius_mean = 22.0
WHERE EXISTS (
    SELECT 1
    FROM patient_info pi
    WHERE bcd.id = pi.id AND pi.age > 50
);

