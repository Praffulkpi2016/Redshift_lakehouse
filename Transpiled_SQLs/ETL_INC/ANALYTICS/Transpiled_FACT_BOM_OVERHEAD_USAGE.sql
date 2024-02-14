TRUNCATE table gold_bec_dwh.fact_bom_overhead_usage_tmp;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh.fact_bom_overhead_usage_tmp
(
  SELECT DISTINCT
    bor.assembly_item_id,
    bor.organization_id,
    br.resource_id
  FROM silver_bec_ods.bom_operational_routings AS bor, silver_bec_ods.bom_operation_sequences AS bos, silver_bec_ods.bom_standard_operations AS bso, silver_bec_ods.bom_operation_resources AS br
  WHERE
    bor.routing_sequence_id = bos.ROUTING_SEQUENCE_ID()
    AND bos.operation_sequence_id = br.OPERATION_SEQUENCE_ID()
    AND bos.standard_operation_id = bso.STANDARD_OPERATION_ID()
    AND bos.disable_date IS NULL
    AND (
      bor.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_bom_overhead_usage' AND batch_name = 'wip'
      )
      OR bos.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_bom_overhead_usage' AND batch_name = 'wip'
      )
      OR bso.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_bom_overhead_usage' AND batch_name = 'wip'
      )
      OR br.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_bom_overhead_usage' AND batch_name = 'wip'
      )
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh.fact_bom_overhead_usage
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.fact_bom_overhead_usage_tmp AS tmp
    WHERE
      COALESCE(tmp.assembly_item_id, 0) = COALESCE(fact_bom_overhead_usage.assembly_item_id, 0)
      AND COALESCE(tmp.resource_id, 0) = COALESCE(fact_bom_overhead_usage.resource_id, 0)
      AND COALESCE(tmp.organization_id, 0) = COALESCE(fact_bom_overhead_usage.organization_id, 0)
  );
/* Insert records into fact table */
INSERT INTO gold_bec_dwh.fact_bom_overhead_usage
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
  ) AS ml1, gold_bec_dwh.fact_bom_overhead_usage_tmp AS tmp
  WHERE
    bor.routing_sequence_id = bos.ROUTING_SEQUENCE_ID()
    AND bos.operation_sequence_id = br.OPERATION_SEQUENCE_ID()
    AND ml1.LOOKUP_TYPE() = 'BOM_BASIS_TYPE'
    AND br.basis_type = ml1.LOOKUP_CODE()
    AND bos.standard_operation_id = bso.STANDARD_OPERATION_ID()
    AND bos.disable_date IS NULL
    AND COALESCE(tmp.assembly_item_id, 0) = COALESCE(bor.assembly_item_id, 0)
    AND COALESCE(tmp.resource_id, 0) = COALESCE(br.resource_id, 0)
    AND COALESCE(tmp.organization_id, 0) = COALESCE(bor.organization_id, 0)
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_bom_overhead_usage' AND batch_name = 'wip';