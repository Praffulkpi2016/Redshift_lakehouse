/* Delete Records */
DELETE FROM silver_bec_ods.MTL_INTERORG_SHIP_METHODS
WHERE
  (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, FROM_LOCATION_ID, TO_LOCATION_ID, SHIP_METHOD) IN (
    SELECT
      stg.FROM_ORGANIZATION_ID,
      stg.TO_ORGANIZATION_ID,
      stg.FROM_LOCATION_ID,
      stg.TO_LOCATION_ID,
      stg.SHIP_METHOD
    FROM silver_bec_ods.MTL_INTERORG_SHIP_METHODS AS ods, bronze_bec_ods_stg.MTL_INTERORG_SHIP_METHODS AS stg
    WHERE
      ods.FROM_ORGANIZATION_ID = stg.FROM_ORGANIZATION_ID
      AND ods.TO_ORGANIZATION_ID = stg.TO_ORGANIZATION_ID
      AND ods.FROM_LOCATION_ID = stg.FROM_LOCATION_ID
      AND ods.TO_LOCATION_ID = stg.TO_LOCATION_ID
      AND ods.SHIP_METHOD = stg.SHIP_METHOD
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_INTERORG_SHIP_METHODS (
  from_organization_id,
  to_organization_id,
  ship_method,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  time_uom_code,
  intransit_time,
  default_flag,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  from_location_id,
  to_location_id,
  load_weight_uom_code,
  volume_uom_code,
  currency_code,
  daily_load_weight_capacity,
  cost_per_unit_load_weight,
  daily_volume_capacity,
  cost_per_unit_load_volume,
  to_region_id,
  destination_type,
  origin_type,
  from_region_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    from_organization_id,
    to_organization_id,
    ship_method,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    time_uom_code,
    intransit_time,
    default_flag,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    from_location_id,
    to_location_id,
    load_weight_uom_code,
    volume_uom_code,
    currency_code,
    daily_load_weight_capacity,
    cost_per_unit_load_weight,
    daily_volume_capacity,
    cost_per_unit_load_volume,
    to_region_id,
    destination_type,
    origin_type,
    from_region_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_INTERORG_SHIP_METHODS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, FROM_LOCATION_ID, TO_LOCATION_ID, SHIP_METHOD, kca_seq_id) IN (
      SELECT
        FROM_ORGANIZATION_ID,
        TO_ORGANIZATION_ID,
        FROM_LOCATION_ID,
        TO_LOCATION_ID,
        SHIP_METHOD,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_INTERORG_SHIP_METHODS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        FROM_ORGANIZATION_ID,
        TO_ORGANIZATION_ID,
        FROM_LOCATION_ID,
        TO_LOCATION_ID,
        SHIP_METHOD
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_INTERORG_SHIP_METHODS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_INTERORG_SHIP_METHODS SET IS_DELETED_FLG = 'Y'
WHERE
  (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, FROM_LOCATION_ID, TO_LOCATION_ID, SHIP_METHOD) IN (
    SELECT
      FROM_ORGANIZATION_ID,
      TO_ORGANIZATION_ID,
      FROM_LOCATION_ID,
      TO_LOCATION_ID,
      SHIP_METHOD
    FROM bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
    WHERE
      (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, FROM_LOCATION_ID, TO_LOCATION_ID, SHIP_METHOD, KCA_SEQ_ID) IN (
        SELECT
          FROM_ORGANIZATION_ID,
          TO_ORGANIZATION_ID,
          FROM_LOCATION_ID,
          TO_LOCATION_ID,
          SHIP_METHOD,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
        GROUP BY
          FROM_ORGANIZATION_ID,
          TO_ORGANIZATION_ID,
          FROM_LOCATION_ID,
          TO_LOCATION_ID,
          SHIP_METHOD
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_interorg_ship_methods';