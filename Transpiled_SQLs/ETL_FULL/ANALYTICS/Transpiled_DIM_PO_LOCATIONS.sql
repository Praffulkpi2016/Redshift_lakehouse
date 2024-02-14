DROP table IF EXISTS gold_bec_dwh.DIM_PO_LOCATIONS;
CREATE TABLE gold_bec_dwh.DIM_PO_LOCATIONS AS
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
    ) || '-' || COALESCE(location_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.hr_locations_all
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_locations' AND batch_name = 'po';