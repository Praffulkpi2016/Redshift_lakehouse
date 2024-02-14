/* Delete Records */
DELETE FROM silver_bec_ods.WSH_DELIVERY_LEGS
WHERE
  (
    COALESCE(DELIVERY_LEG_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.DELIVERY_LEG_ID, 0) AS DELIVERY_LEG_ID
    FROM silver_bec_ods.WSH_DELIVERY_LEGS AS ods, bronze_bec_ods_stg.WSH_DELIVERY_LEGS AS stg
    WHERE
      COALESCE(ods.DELIVERY_LEG_ID, 0) = COALESCE(stg.DELIVERY_LEG_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WSH_DELIVERY_LEGS (
  delivery_leg_id,
  delivery_id,
  sequence_number,
  loading_order_flag,
  pick_up_stop_id,
  drop_off_stop_id,
  gross_weight,
  net_weight,
  weight_uom_code,
  volume,
  volume_uom_code,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  load_tender_status,
  shipper_title,
  shipper_phone,
  pod_flag,
  pod_by,
  pod_date,
  expected_pod_date,
  booking_office,
  shipper_export_ref,
  carrier_export_ref,
  doc_notify_party,
  aetc_number,
  shipper_signed_by,
  shipper_date,
  carrier_signed_by,
  carrier_date,
  doc_issue_office,
  doc_issued_by,
  doc_date_issued,
  shipper_hm_by,
  shipper_hm_date,
  carrier_hm_by,
  carrier_hm_date,
  booking_number,
  port_of_loading,
  port_of_discharge,
  service_contract,
  bill_freight_to,
  fte_trip_id,
  reprice_required,
  actual_arrival_date,
  actual_departure_date,
  actual_receipt_date,
  tracking_drilldown_flag,
  status_code,
  tracking_remarks,
  carrier_est_departure_date,
  carrier_est_arrival_date,
  loading_start_datetime,
  loading_end_datetime,
  unloading_start_datetime,
  unloading_end_datetime,
  delivered_quantity,
  loaded_quantity,
  received_quantity,
  origin_stop_id,
  destination_stop_id,
  parent_delivery_leg_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    delivery_leg_id,
    delivery_id,
    sequence_number,
    loading_order_flag,
    pick_up_stop_id,
    drop_off_stop_id,
    gross_weight,
    net_weight,
    weight_uom_code,
    volume,
    volume_uom_code,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    load_tender_status,
    shipper_title,
    shipper_phone,
    pod_flag,
    pod_by,
    pod_date,
    expected_pod_date,
    booking_office,
    shipper_export_ref,
    carrier_export_ref,
    doc_notify_party,
    aetc_number,
    shipper_signed_by,
    shipper_date,
    carrier_signed_by,
    carrier_date,
    doc_issue_office,
    doc_issued_by,
    doc_date_issued,
    shipper_hm_by,
    shipper_hm_date,
    carrier_hm_by,
    carrier_hm_date,
    booking_number,
    port_of_loading,
    port_of_discharge,
    service_contract,
    bill_freight_to,
    fte_trip_id,
    reprice_required,
    actual_arrival_date,
    actual_departure_date,
    actual_receipt_date,
    tracking_drilldown_flag,
    status_code,
    tracking_remarks,
    carrier_est_departure_date,
    carrier_est_arrival_date,
    loading_start_datetime,
    loading_end_datetime,
    unloading_start_datetime,
    unloading_end_datetime,
    delivered_quantity,
    loaded_quantity,
    received_quantity,
    origin_stop_id,
    destination_stop_id,
    parent_delivery_leg_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.WSH_DELIVERY_LEGS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(DELIVERY_LEG_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(DELIVERY_LEG_ID, 0) AS DELIVERY_LEG_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.WSH_DELIVERY_LEGS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(DELIVERY_LEG_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WSH_DELIVERY_LEGS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WSH_DELIVERY_LEGS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    DELIVERY_LEG_ID
  ) IN (
    SELECT
      DELIVERY_LEG_ID
    FROM bec_raw_dl_ext.WSH_DELIVERY_LEGS
    WHERE
      (DELIVERY_LEG_ID, KCA_SEQ_ID) IN (
        SELECT
          DELIVERY_LEG_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WSH_DELIVERY_LEGS
        GROUP BY
          DELIVERY_LEG_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wsh_delivery_legs';