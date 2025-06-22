WITH src AS (
    SELECT 
        id, 
        firstname, 
        lastname, 
        gender, 
        email, 
        dateofbirth, 
        currentaddressid
    FROM webshop.raw_customer
)

MERGE INTO webshop.dim_customer AS target
USING src AS source
ON target.id = source.id

WHEN MATCHED AND (
    target.firstname IS DISTINCT FROM source.firstname OR
    target.lastname IS DISTINCT FROM source.lastname OR
    target.email IS DISTINCT FROM source.email OR
    target.currentaddressid IS DISTINCT FROM source.currentaddressid
) THEN
    UPDATE SET
        firstname = source.firstname,
        lastname = source.lastname,
        email = source.email,
        currentaddressid = source.currentaddressid,
        upd_dt = CURRENT_DATE

WHEN NOT MATCHED THEN
    INSERT (
        id, 
        firstname, 
        lastname, 
        gender, 
        email, 
        dateofbirth, 
        currentaddressid,
        ins_dt,
        upd_dt
    )
    VALUES (
        source.id, 
        source.firstname, 
        source.lastname, 
        source.gender, 
        source.email, 
        source.dateofbirth, 
        source.currentaddressid,
        CURRENT_DATE,
        NULL
    );
