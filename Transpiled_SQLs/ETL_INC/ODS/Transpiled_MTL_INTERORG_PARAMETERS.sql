/* Delete Records */
DELETE FROM silver_bec_ods.mtl_interorg_parameters
WHERE
  (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID) IN (
    SELECT
      stg.FROM_ORGANIZATION_ID,
      stg.TO_ORGANIZATION_ID
    FROM silver_bec_ods.mtl_interorg_parameters AS ods, bronze_bec_ods_stg.mtl_interorg_parameters AS stg
    WHERE
      ods.FROM_ORGANIZATION_ID = stg.FROM_ORGANIZATION_ID
      AND ods.TO_ORGANIZATION_ID = stg.TO_ORGANIZATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.mtl_interorg_parameters (
  FROM_ORGANIZATION_ID,
  TO_ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  INTRANSIT_TYPE,
  DISTANCE_UOM_CODE,
  TO_ORGANIZATION_DISTANCE,
  FOB_POINT,
  MATL_INTERORG_TRANSFER_CODE,
  ROUTING_HEADER_ID,
  INTERNAL_ORDER_REQUIRED_FLAG,
  INTRANSIT_INV_ACCOUNT,
  INTERORG_TRNSFR_CHARGE_PERCENT,
  INTERORG_TRANSFER_CR_ACCOUNT,
  INTERORG_RECEIVABLES_ACCOUNT,
  INTERORG_PAYABLES_ACCOUNT,
  INTERORG_PRICE_VAR_ACCOUNT,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  GLOBAL_ATTRIBUTE_CATEGORY,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  ELEMENTAL_VISIBILITY_ENABLED,
  MANUAL_RECEIPT_EXPENSE,
  PROFIT_IN_INV_ACCOUNT,
  SHIKYU_ENABLED_FLAG,
  SHIKYU_DEFAULT_ORDER_TYPE_ID,
  SHIKYU_OEM_VAR_ACCOUNT_ID,
  SHIKYU_TP_OFFSET_ACCOUNT_ID,
  INTERORG_PROFIT_ACCOUNT,
  PRICELIST_ID,
  SUBCONTRACTING_TYPE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    FROM_ORGANIZATION_ID,
    TO_ORGANIZATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    INTRANSIT_TYPE,
    DISTANCE_UOM_CODE,
    TO_ORGANIZATION_DISTANCE,
    FOB_POINT,
    MATL_INTERORG_TRANSFER_CODE,
    ROUTING_HEADER_ID,
    INTERNAL_ORDER_REQUIRED_FLAG,
    INTRANSIT_INV_ACCOUNT,
    INTERORG_TRNSFR_CHARGE_PERCENT,
    INTERORG_TRANSFER_CR_ACCOUNT,
    INTERORG_RECEIVABLES_ACCOUNT,
    INTERORG_PAYABLES_ACCOUNT,
    INTERORG_PRICE_VAR_ACCOUNT,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    GLOBAL_ATTRIBUTE_CATEGORY,
    GLOBAL_ATTRIBUTE1,
    GLOBAL_ATTRIBUTE2,
    GLOBAL_ATTRIBUTE3,
    GLOBAL_ATTRIBUTE4,
    GLOBAL_ATTRIBUTE5,
    GLOBAL_ATTRIBUTE6,
    GLOBAL_ATTRIBUTE7,
    GLOBAL_ATTRIBUTE8,
    GLOBAL_ATTRIBUTE9,
    GLOBAL_ATTRIBUTE10,
    GLOBAL_ATTRIBUTE11,
    GLOBAL_ATTRIBUTE12,
    GLOBAL_ATTRIBUTE13,
    GLOBAL_ATTRIBUTE14,
    GLOBAL_ATTRIBUTE15,
    GLOBAL_ATTRIBUTE16,
    GLOBAL_ATTRIBUTE17,
    GLOBAL_ATTRIBUTE18,
    GLOBAL_ATTRIBUTE19,
    GLOBAL_ATTRIBUTE20,
    ELEMENTAL_VISIBILITY_ENABLED,
    MANUAL_RECEIPT_EXPENSE,
    PROFIT_IN_INV_ACCOUNT,
    SHIKYU_ENABLED_FLAG,
    SHIKYU_DEFAULT_ORDER_TYPE_ID,
    SHIKYU_OEM_VAR_ACCOUNT_ID,
    SHIKYU_TP_OFFSET_ACCOUNT_ID,
    INTERORG_PROFIT_ACCOUNT,
    PRICELIST_ID,
    SUBCONTRACTING_TYPE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.mtl_interorg_parameters
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        FROM_ORGANIZATION_ID,
        TO_ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.mtl_interorg_parameters
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        FROM_ORGANIZATION_ID,
        TO_ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.mtl_interorg_parameters SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.mtl_interorg_parameters SET IS_DELETED_FLG = 'Y'
WHERE
  (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID) IN (
    SELECT
      FROM_ORGANIZATION_ID,
      TO_ORGANIZATION_ID
    FROM bec_raw_dl_ext.mtl_interorg_parameters
    WHERE
      (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, KCA_SEQ_ID) IN (
        SELECT
          FROM_ORGANIZATION_ID,
          TO_ORGANIZATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.mtl_interorg_parameters
        GROUP BY
          FROM_ORGANIZATION_ID,
          TO_ORGANIZATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_interorg_parameters';