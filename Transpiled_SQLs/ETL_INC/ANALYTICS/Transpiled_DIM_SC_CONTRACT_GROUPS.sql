/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_SC_CONTRACT_GROUPS
WHERE
  (
    COALESCE(INCLUDED_CHR_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.INCLUDED_CHR_ID, 0) AS INCLUDED_CHR_ID
    FROM gold_bec_dwh.DIM_SC_CONTRACT_GROUPS AS dw, (
      SELECT
        OKG.INCLUDED_CHR_ID
      FROM silver_bec_ods.OKC_K_GRPINGS AS OKG, silver_bec_ods.OKC_K_GROUPS_TL AS OKGT
      WHERE
        1 = 1
        AND OKG.CGP_PARENT_ID = OKGT.ID
        AND (
          OKG.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_sc_contract_groups' AND batch_name = 'sc'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INCLUDED_CHR_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_SC_CONTRACT_GROUPS (
  id,
  cgp_parent_id,
  included_chr_id,
  scs_code,
  name,
  SHORT_DESCRIPTION,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND OKG.CGP_PARENT_ID = OKGT.ID
    AND (
      OKG.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_sc_contract_groups' AND batch_name = 'sc'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_SC_CONTRACT_GROUPS SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(INCLUDED_CHR_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.INCLUDED_CHR_ID, 0) AS INCLUDED_CHR_ID
    FROM gold_bec_dwh.DIM_SC_CONTRACT_GROUPS AS dw, (
      SELECT
        OKG.INCLUDED_CHR_ID
      FROM (
        SELECT
          *
        FROM BEC_ODS.OKC_K_GRPINGS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS OKG, (
        SELECT
          *
        FROM silver_bec_ods.OKC_K_GROUPS_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS OKGT
      WHERE
        1 = 1 AND OKG.CGP_PARENT_ID = OKGT.ID
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INCLUDED_CHR_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_sc_contract_groups' AND batch_name = 'sc';