DROP table IF EXISTS bronze_bec_ods_stg.CST_COST_ELEMENTS;
CREATE TABLE bronze_bec_ods_stg.CST_COST_ELEMENTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.CST_COST_ELEMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(COST_ELEMENT_ID, '0'), last_update_date) IN (
      SELECT
        COALESCE(COST_ELEMENT_ID, '0') AS COST_ELEMENT_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.CST_COST_ELEMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(COST_ELEMENT_ID, '0')
    )
);