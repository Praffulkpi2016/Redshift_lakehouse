DROP table IF EXISTS gold_bec_dwh.DIM_INVENTORY_ORG;
CREATE TABLE gold_bec_dwh.DIM_INVENTORY_ORG AS
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inventory_org' AND batch_name = 'ap';