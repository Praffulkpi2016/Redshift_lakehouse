/* Delete Records */
DELETE FROM gold_bec_dwh.dim_po_locations
WHERE
  (
    COALESCE(LOCATION_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.LOCATION_ID, 0) AS LOCATION_ID
    FROM gold_bec_dwh.dim_po_locations AS dw, silver_bec_ods.hr_locations_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LOCATION_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_locations' AND batch_name = 'po'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_PO_LOCATIONS (
  location_id,
  location_code,
  address_line_1,
  address_line_2,
  address_line_3,
  bill_to_site_flag,
  country,
  location_desc,
  postal_code,
  telephone_number_1,
  telephone_number_2,
  telephone_number_3,
  town_or_city,
  creation_date,
  derived_locale,
  CREATED_BY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    location_id,
    location_code,
    address_line_1,
    address_line_2,
    address_line_3,
    bill_to_site_flag,
    country,
    description AS location_desc,
    postal_code,
    telephone_number_1,
    telephone_number_2,
    telephone_number_3,
    town_or_city,
    creation_date,
    derived_locale,
    CREATED_BY,
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
    ) || '-' || COALESCE(LOCATION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.hr_locations_all
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_locations' AND batch_name = 'po'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_po_locations SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(LOCATION_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.LOCATION_ID, 0) AS LOCATION_ID
    FROM gold_bec_dwh.dim_po_locations AS dw, silver_bec_ods.hr_locations_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LOCATION_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_locations' AND batch_name = 'po';