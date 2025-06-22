--source data create a temp 

CREATE TEMP TABLE temp_src AS
SELECT 
    id, firstname, lastname, gender, email, dateofbirth, currentaddressid
FROM webshop.raw_customer;


-- Step 1: Expire existing active records that changed
UPDATE dim_customer_type2 AS target
SET 
    is_current = false,
    end_date = CURRENT_DATE
FROM temp_src AS source
WHERE target.id = source.id
  AND target.is_current = true
  AND (
    target.firstname IS DISTINCT FROM source.firstname OR
    target.lastname IS DISTINCT FROM source.lastname OR
    target.email IS DISTINCT FROM source.email OR
    target.currentaddressid IS DISTINCT FROM source.currentaddressid
  );




-- Step 2: Insert new versioned record for new or changed rows
INSERT INTO dim_customer_type2 (
    id, 
    firstname, 
    lastname, 
    gender, 
    email, 
    dateofbirth, 
    currentaddressid,
    is_current, 
    start_date, 
    end_date
)
SELECT 
    src.id,
    src.firstname,
    src.lastname,
    src.gender,
    src.email,
    src.dateofbirth,
    src.currentaddressid,
    true AS is_current,
    CURRENT_DATE AS start_date,
    NULL AS end_date
FROM temp_src as src
LEFT JOIN dim_customer_type2 tgt
    ON src.id = tgt.id AND tgt.is_current = true
WHERE 
    tgt.id IS NULL -- new record
    OR (
        src.firstname IS DISTINCT FROM tgt.firstname OR
        src.lastname IS DISTINCT FROM tgt.lastname OR
        src.gender IS DISTINCT FROM tgt.gender OR
        src.email IS DISTINCT FROM tgt.email OR
        src.currentaddressid IS DISTINCT FROM tgt.currentaddressid 
    );



--drop the temp table

DROP TABLE temp_src;

