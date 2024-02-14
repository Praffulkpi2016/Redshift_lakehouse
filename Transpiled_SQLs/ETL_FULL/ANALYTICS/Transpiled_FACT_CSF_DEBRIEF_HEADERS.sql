DROP table IF EXISTS gold_bec_dwh.FACT_CSF_DEBRIEF_HEADERS;
CREATE TABLE gold_bec_dwh.FACT_CSF_DEBRIEF_HEADERS AS
SELECT
  jtb.owner_type_code,
  jtb.owner_id,
  cdh.debrief_header_id,
  cdh.debrief_number,
  cdh.debrief_date,
  cdh.debrief_status_id,
  jtb.task_id,
  jtb.base_task_id,
  cdh.task_assignment_id,
  cdh.processed_flag,
  jtb.scheduled_start_date,
  cdh.creation_date,
  cdh.last_update_date,
  cdh.created_by,
  cdh.last_updated_by,
  cdh.last_update_login,
  cdh.attribute_category,
  cdh.attribute1,
  cdh.attribute2,
  cdh.attribute3,
  cdh.attribute4,
  cdh.attribute5,
  cdh.attribute6,
  cdh.attribute7,
  cdh.attribute8,
  cdh.attribute9,
  cdh.attribute10,
  cdh.attribute11,
  cdh.attribute12,
  cdh.attribute13,
  cdh.attribute14,
  cdh.attribute15,
  jtb.task_number,
  jtb.task_priority_id,
  jtb.creation_date AS task_creation_date,
  jtb.source_object_type_code,
  jtb.task_type_id,
  jtv.task_name,
  jtv.description,
  jts1.NAME AS task_status1,
  jts2.NAME AS task_status2,
  jpa.party_id,
  jpa.party_name,
  jpa.party_number,
  jpa.address1 || ', ' || jpa.address2 || ', ' || jpa.address3 || ', ' || jpa.address4 AS ADDRESS,
  jpa.city || ', ' || jpa.state || ' - ' || jpa.postal_code AS CITY_STATE_ZIP,
  jta.resource_id,
  jta.resource_type_code,
  cia.incident_number,
  cia.incident_id,
  cia.incident_date,
  cia.customer_po_number,
  cia.creation_date AS sr_creation_date,
  cia.summary AS sr_summary,
  cia.incident_type_id,
  cia.incident_severity_id,
  jtb.source_object_id,
  cia.customer_id,
  jtb.cust_account_id,
  cia.SHIP_TO_SITE_ID,
  cia.BILL_TO_SITE_ID,
  cia.incident_status_id,
  cii.INSTALL_LOCATION_ID,
  cii.location_id,
  cii.install_date,
  cii.external_reference AS padid,
  cii.INSTANCE_ID,
  cii.instance_number,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || jtb.owner_id AS owner_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cdh.debrief_header_id AS debrief_header_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || jtb.task_id AS task_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || jta.resource_id AS resource_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cia.incident_id AS incident_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cia.customer_id AS customer_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cii.location_id AS location_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cii.INSTANCE_ID AS INSTANCE_ID_KEY,
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
  ) || '-' || COALESCE(cdh.debrief_header_id, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    task_status_id,
    NAME
  FROM silver_bec_ods.jtf_task_statuses_tl
  WHERE
    is_deleted_flg <> 'Y' AND language = 'US'
) AS jts1, (
  SELECT
    task_status_id,
    NAME
  FROM silver_bec_ods.jtf_task_statuses_tl
  WHERE
    is_deleted_flg <> 'Y' AND language = 'US'
) AS jts2, (
  SELECT
    *
  FROM silver_bec_ods.hz_parties
  WHERE
    is_deleted_flg <> 'Y'
) AS jpa, (
  SELECT
    *
  FROM silver_bec_ods.cs_incidents_all_b
  WHERE
    is_deleted_flg <> 'Y'
) AS cia, (
  SELECT
    *
  FROM silver_bec_ods.csf_debrief_headers
  WHERE
    is_deleted_flg <> 'Y'
) AS cdh, (
  SELECT
    task_name,
    task_id,
    description
  FROM silver_bec_ods.jtf_tasks_tl
  WHERE
    is_deleted_flg <> 'Y' AND language = 'US'
) AS jtv, (
  SELECT
    *
  FROM silver_bec_ods.jtf_tasks_b
  WHERE
    is_deleted_flg <> 'Y'
) AS jtb, (
  SELECT
    *
  FROM silver_bec_ods.csi_item_instances
  WHERE
    is_deleted_flg <> 'Y'
) AS cii, (
  SELECT
    task_assignment_id,
    task_id,
    assignee_role,
    assignment_status_id,
    resource_id,
    resource_type_code
  FROM silver_bec_ods.jtf_task_assignments
  WHERE
    is_deleted_flg <> 'Y'
) AS jta
WHERE
  jtb.task_id = jtv.task_id
  AND jtb.source_object_id = cia.INCIDENT_ID()
  AND cdh.task_assignment_id = jta.task_assignment_id
  AND jta.task_id = jtb.task_id
  AND jta.assignee_role = 'ASSIGNEE'
  AND COALESCE(jtb.deleted_flag, 'N') <> 'Y'
  AND jtb.customer_id = jpa.PARTY_ID()
  AND jtb.task_status_id = jts1.task_status_id
  AND jta.assignment_status_id = jts2.task_status_id
  AND cia.CUSTOMER_PRODUCT_ID = cii.INSTANCE_ID();
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_csf_debrief_headers' AND batch_name = 'inv';