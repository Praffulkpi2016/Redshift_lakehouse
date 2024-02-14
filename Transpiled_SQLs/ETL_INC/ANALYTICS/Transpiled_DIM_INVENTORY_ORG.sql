/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_INVENTORY_ORG
WHERE
  COALESCE(INV_ORGANIZATION_ID, 0) IN (
    SELECT
      COALESCE(ods.INV_ORGANIZATION_ID, 0) AS INV_ORGANIZATION_ID
    FROM gold_bec_dwh.DIM_INVENTORY_ORG AS dw, (
      SELECT
        HOU.ORGANIZATION_ID AS INV_ORGANIZATION_ID
      FROM silver_bec_ods.HR_ALL_ORGANIZATION_UNITS AS HOU, silver_bec_ods.HR_ORGANIZATION_INFORMATION AS HOI1, silver_bec_ods.HR_ORGANIZATION_INFORMATION AS HOI2, silver_bec_ods.MTL_PARAMETERS AS MP, silver_bec_ods.GL_LEDGERS AS LGR, silver_bec_ods.org_organization_definitions AS ood, silver_bec_ods.hr_operating_units AS hou1
      WHERE
        HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
        AND HOU.ORGANIZATION_ID = HOI2.ORGANIZATION_ID
        AND HOU.ORGANIZATION_ID = MP.ORGANIZATION_ID
        AND HOI1.ORG_INFORMATION1 = 'INV'
        AND HOI1.ORG_INFORMATION2 = 'Y'
        AND (
          HOI1.ORG_INFORMATION_CONTEXT || ''
        ) = 'CLASS'
        AND (
          HOI2.ORG_INFORMATION_CONTEXT || ''
        ) = 'Accounting Information'
        AND CASE
          WHEN RTRIM(TRANSLATE(HOI2.ORG_INFORMATION1, '0123456789', ' ')) IS NULL
          THEN -99999
          ELSE CAST(HOI2.ORG_INFORMATION1 AS INT)
        END = LGR.LEDGER_ID
        AND LGR.OBJECT_TYPE_CODE = 'L'
        AND COALESCE(LGR.COMPLETE_FLAG, 'Y') = 'Y'
        AND ood.ORGANIZATION_ID = mp.ORGANIZATION_ID
        AND ood.operating_unit = hou1.organization_id
        AND (
          HOU.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_inventory_org' AND batch_name = 'ap'
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
      ) || '-' || COALESCE(ods.INV_ORGANIZATION_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_INVENTORY_ORG (
  inv_organization_id,
  business_group_id,
  enable_date,
  disable_date,
  inv_organization_code,
  inv_organization_name,
  set_of_books_id,
  chart_of_accounts_id,
  inventory_enabled_flag,
  operating_unit,
  currency_code,
  operating_unit_name,
  legal_entity,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    HOU.ORGANIZATION_ID AS INV_ORGANIZATION_ID,
    HOU.BUSINESS_GROUP_ID,
    HOU.DATE_FROM AS ENABLE_DATE,
    HOU.DATE_TO AS DISABLE_DATE,
    MP.ORGANIZATION_CODE AS INV_ORGANIZATION_CODE,
    HOU.NAME AS INV_ORGANIZATION_NAME,
    LGR.LEDGER_ID AS SET_OF_BOOKS_ID,
    LGR.CHART_OF_ACCOUNTS_ID AS CHART_OF_ACCOUNTS_ID,
    HOI1.ORG_INFORMATION2 AS INVENTORY_ENABLED_FLAG,
    ood.operating_unit,
    LGR.CURRENCY_CODE,
    hou1.`name` AS operating_unit_name,
    CASE
      WHEN HOI2.ORG_INFORMATION_CONTEXT = 'Accounting Information'
      THEN CAST(HOI2.ORG_INFORMATION2 AS INT)
      ELSE NULL
    END AS LEGAL_ENTITY,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(HOU.organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.HR_ALL_ORGANIZATION_UNITS AS HOU, silver_bec_ods.HR_ORGANIZATION_INFORMATION AS HOI1, silver_bec_ods.HR_ORGANIZATION_INFORMATION AS HOI2, silver_bec_ods.MTL_PARAMETERS AS MP, silver_bec_ods.GL_LEDGERS AS LGR, silver_bec_ods.org_organization_definitions AS ood, silver_bec_ods.hr_operating_units AS hou1
  WHERE
    HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
    AND HOU.ORGANIZATION_ID = HOI2.ORGANIZATION_ID
    AND HOU.ORGANIZATION_ID = MP.ORGANIZATION_ID
    AND HOI1.ORG_INFORMATION1 = 'INV'
    AND HOI1.ORG_INFORMATION2 = 'Y'
    AND (
      HOI1.ORG_INFORMATION_CONTEXT || ''
    ) = 'CLASS'
    AND (
      HOI2.ORG_INFORMATION_CONTEXT || ''
    ) = 'Accounting Information'
    AND CASE
      WHEN RTRIM(TRANSLATE(HOI2.ORG_INFORMATION1, '0123456789', ' ')) IS NULL
      THEN -99999
      ELSE CAST(HOI2.ORG_INFORMATION1 AS INT)
    END = LGR.LEDGER_ID
    AND LGR.OBJECT_TYPE_CODE = 'L'
    AND COALESCE(LGR.COMPLETE_FLAG, 'Y') = 'Y'
    AND ood.ORGANIZATION_ID = mp.ORGANIZATION_ID
    AND ood.operating_unit = hou1.organization_id
    AND (
      HOU.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_inventory_org' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_inventory_org SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(INV_ORGANIZATION_ID, 0) IN (
    SELECT
      COALESCE(ods.INV_ORGANIZATION_ID, 0) AS INV_ORGANIZATION_ID
    FROM gold_bec_dwh.DIM_INVENTORY_ORG AS dw, (
      SELECT
        HOU.ORGANIZATION_ID AS INV_ORGANIZATION_ID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.HR_ALL_ORGANIZATION_UNITS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS HOU, (
        SELECT
          *
        FROM silver_bec_ods.HR_ORGANIZATION_INFORMATION
        WHERE
          is_deleted_flg <> 'Y'
      ) AS HOI1, (
        SELECT
          *
        FROM silver_bec_ods.HR_ORGANIZATION_INFORMATION
        WHERE
          is_deleted_flg <> 'Y'
      ) AS HOI2, (
        SELECT
          *
        FROM silver_bec_ods.MTL_PARAMETERS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS MP, (
        SELECT
          *
        FROM silver_bec_ods.GL_LEDGERS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS LGR, (
        SELECT
          *
        FROM silver_bec_ods.org_organization_definitions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ood, (
        SELECT
          *
        FROM silver_bec_ods.hr_operating_units
        WHERE
          is_deleted_flg <> 'Y'
      ) AS hou1
      WHERE
        HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
        AND HOU.ORGANIZATION_ID = HOI2.ORGANIZATION_ID
        AND HOU.ORGANIZATION_ID = MP.ORGANIZATION_ID
        AND HOI1.ORG_INFORMATION1 = 'INV'
        AND HOI1.ORG_INFORMATION2 = 'Y'
        AND (
          HOI1.ORG_INFORMATION_CONTEXT || ''
        ) = 'CLASS'
        AND (
          HOI2.ORG_INFORMATION_CONTEXT || ''
        ) = 'Accounting Information'
        AND CASE
          WHEN RTRIM(TRANSLATE(HOI2.ORG_INFORMATION1, '0123456789', ' ')) IS NULL
          THEN -99999
          ELSE CAST(HOI2.ORG_INFORMATION1 AS INT)
        END = LGR.LEDGER_ID
        AND LGR.OBJECT_TYPE_CODE = 'L'
        AND COALESCE(LGR.COMPLETE_FLAG, 'Y') = 'Y'
        AND ood.ORGANIZATION_ID = mp.ORGANIZATION_ID
        AND ood.operating_unit = hou1.organization_id
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INV_ORGANIZATION_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inventory_org' AND batch_name = 'ap';