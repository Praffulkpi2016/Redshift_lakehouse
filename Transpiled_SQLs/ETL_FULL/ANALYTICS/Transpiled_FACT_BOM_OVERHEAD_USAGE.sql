DROP table IF EXISTS gold_bec_dwh.FACT_BOM_OVERHEAD_USAGE;
CREATE TABLE gold_bec_dwh.FACT_BOM_OVERHEAD_USAGE AS
(
  SELECT
    bor.assembly_item_id,
    bor.organization_id,
    bor.alternate_routing_designator,
    bso.operation_code AS standard_operation_code,
    bos.operation_seq_num,
    bos.operation_description,
    bos.department_id,
    br.resource_seq_num,
    br.resource_id,
    ml1.meaning AS basis_type,
    br.usage_rate_or_amount * 60 AS usage_in_mins,
    br.usage_rate_or_amount AS usage_in_hrs,
    bos.effectivity_date,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || bor.assembly_item_id AS assembly_item_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || bor.organization_id AS organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || bos.department_id AS department_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || br.resource_id AS resource_id_KEY,
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
    ) || '-' || COALESCE(bor.assembly_item_id, 0) || '-' || COALESCE(bor.organization_id, 0) || '-' || COALESCE(br.resource_id, 0) || '-' || COALESCE(bos.operation_seq_num, 0) || '-' || COALESCE(br.resource_seq_num, 0) || '-' || COALESCE(bor.alternate_routing_designator, 'NA') || '-' || COALESCE(bos.effectivity_date, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.bom_operational_routings
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bor, (
    SELECT
      *
    FROM silver_bec_ods.bom_operation_sequences
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bos, (
    SELECT
      *
    FROM silver_bec_ods.bom_standard_operations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bso, (
    SELECT
      *
    FROM silver_bec_ods.bom_operation_resources
    WHERE
      is_deleted_flg <> 'Y'
  ) AS br, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ml1
  WHERE
    bor.routing_sequence_id = bos.ROUTING_SEQUENCE_ID()
    AND bos.operation_sequence_id = br.OPERATION_SEQUENCE_ID()
    AND ml1.LOOKUP_TYPE() = 'BOM_BASIS_TYPE'
    AND br.basis_type = ml1.LOOKUP_CODE()
    AND bos.standard_operation_id = bso.STANDARD_OPERATION_ID()
    AND bos.disable_date IS NULL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_bom_overhead_usage' AND batch_name = 'wip';