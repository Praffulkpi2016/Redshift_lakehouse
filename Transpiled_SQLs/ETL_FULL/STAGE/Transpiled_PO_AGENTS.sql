DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_AGENTS;
CREATE TABLE bronze_bec_ods_stg.PO_AGENTS AS
SELECT
  *
FROM bec_raw_dl_ext.PO_AGENTS
WHERE
  kca_operation <> 'DELETE'
  AND (AGENT_ID, last_update_date) IN (
    SELECT
      AGENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_AGENTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      AGENT_ID
  );