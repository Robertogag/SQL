CREATE TABLE keepcoding.ivr_summary AS
SELECT 
    d.calls_ivr_id AS ivr_id,
    d.calls_phone_number,
    d.calls_ivr_result,
    CASE 
        WHEN d.calls_vdn_label LIKE 'ATC%' THEN 'FRONT'
        WHEN d.calls_vdn_label LIKE 'TECH%' THEN 'TECH'
        WHEN d.calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
        ELSE 'RESTO'
    END AS vdn_aggregation,
    d.calls_start_date,
    d.calls_end_date,
    d.calls_total_duration,
    d.calls_customer_segment,
    d.calls_ivr_language,
    d.calls_steps_module,
    d.calls_module_aggregation,
    IF(d.calls_module_aggregation LIKE '%AVERIA_MASIVA%', 1, 0) AS masiva_lg,
    IF(d.step_name = 'CUSTOMERINFOBYPHONE.TX' AND d.step_description_error IS NULL, 1, 0) AS info_by_phone_lg,
    IF(d.step_name = 'CUSTOMERINFOBYDNI.TX' AND d.step_description_error IS NULL, 1, 0) AS info_by_dni_lg,
    IF(DATETIME_DIFF(d.calls_start_date, LAG(d.calls_start_date) OVER (PARTITION BY d.calls_phone_number ORDER BY d.calls_phone_number, d.calls_start_date), HOUR) < 24, 1, 0) AS repeated_phone_24H,
    IF(DATETIME_DIFF(LEAD(d.calls_start_date) OVER (PARTITION BY d.calls_phone_number ORDER BY d.calls_phone_number, d.calls_start_date), d.calls_end_date, HOUR) < 24, 1, 0) AS cause_recall_phone_24H,
    COALESCE(d.document_type, "NULL") AS document_type,
    COALESCE(d.document_identification, "NULL") AS document_identification,
    COALESCE(d.customer_phone, "NULL") AS customer_phone,
    COALESCE(d.billing_account_id, "NULL") AS billing_account_id
FROM `keepcoding.ivr_detail` d
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(d.calls_ivr_id AS STRING) ORDER BY d.calls_ivr_id, d.calls_start_date DESC) = 1;