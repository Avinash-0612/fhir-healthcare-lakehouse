-- Staging model: Raw patient data cleaning
-- Source: Silver layer (FHIR Patient resources)

WITH source_data AS (
    SELECT * 
    FROM {{ source('silver', 'patients') }}
    WHERE compliance_flag = 'HIPAA-MASKED'
),

cleaned AS (
    SELECT
        patient_id,
        name_initial AS patient_name,
        birthDate AS date_of_birth,
        gender,
        zip_region AS geographic_region,
        mrn_masked AS medical_record_number,
        ingestion_timestamp,
        data_source
        
    FROM source_data
    
    WHERE patient_id IS NOT NULL
      AND birthDate IS NOT NULL
),

validation AS (
    SELECT 
        *,
        -- Calculate age for analytics
        FLOOR(DATEDIFF(CURRENT_DATE, date_of_birth) / 365.25) AS age_years,
        
        -- Age groups for reporting
        CASE 
            WHEN FLOOR(DATEDIFF(CURRENT_DATE, date_of_birth) / 365.25) < 18 THEN 'Pediatric'
            WHEN FLOOR(DATEDIFF(CURRENT_DATE, date_of_birth) / 365.25) < 65 THEN 'Adult'
            ELSE 'Senior'
        END AS age_group
        
    FROM cleaned
)

SELECT * FROM validation
