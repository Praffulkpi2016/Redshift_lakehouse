DROP table IF EXISTS gold_bec_dwh.DIM_SC_CONTRACT_GROUPS;
CREATE TABLE gold_bec_dwh.DIM_SC_CONTRACT_GROUPS AS
(
  SELECT
    OKG.ID,
    OKG.CGP_PARENT_ID,
    OKG.INCLUDED_CHR_ID,
    OKG.SCS_CODE,
    OKGT.NAME,
    OKGT.SHORT_DESCRIPTION,
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(OKG.INCLUDED_CHR_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.OKC_K_GRPINGS AS OKG, silver_bec_ods.OKC_K_GROUPS_TL AS OKGT
  WHERE
    1 = 1 AND OKG.CGP_PARENT_ID = OKGT.ID
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_sc_contract_groups' AND batch_name = 'sc';