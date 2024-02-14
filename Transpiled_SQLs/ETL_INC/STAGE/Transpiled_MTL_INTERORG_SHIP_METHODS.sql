TRUNCATE table bronze_bec_ods_stg.MTL_INTERORG_SHIP_METHODS;
INSERT INTO bronze_bec_ods_stg.MTL_INTERORG_SHIP_METHODS (
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
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(FROM_ORGANIZATION_ID, 0), COALESCE(TO_ORGANIZATION_ID, 0), COALESCE(FROM_LOCATION_ID, 0), COALESCE(TO_LOCATION_ID, 0), COALESCE(SHIP_METHOD, '0'), kca_seq_id) IN (
      SELECT
        COALESCE(FROM_ORGANIZATION_ID, 0),
        COALESCE(TO_ORGANIZATION_ID, 0),
        COALESCE(FROM_LOCATION_ID, 0),
        COALESCE(TO_LOCATION_ID, 0),
        COALESCE(SHIP_METHOD, '0'),
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(FROM_ORGANIZATION_ID, 0),
        COALESCE(TO_ORGANIZATION_ID, 0),
        COALESCE(FROM_LOCATION_ID, 0),
        COALESCE(TO_LOCATION_ID, 0),
        COALESCE(SHIP_METHOD, '0')
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_interorg_ship_methods'
      )
    )
);