/* DELETE */
DELETE FROM gold_bec_dwh.DIM_PO_WIP_JOBS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.po_requisition_headers_all AS prh
    INNER JOIN silver_bec_ods.po_requisition_lines_all AS prl
      ON prh.requisition_header_id = prl.requisition_header_id
    INNER JOIN silver_bec_ods.po_req_distributions_all AS prd
      ON prl.requisition_line_id = prd.requisition_line_id
    INNER JOIN silver_bec_ods.po_distributions_all AS pod
      ON prd.distribution_id = pod.req_distribution_id
    INNER JOIN silver_bec_ods.wip_entities AS we
      ON prl.wip_entity_id = we.wip_entity_id
    INNER JOIN silver_bec_ods.wip_discrete_jobs AS wdj
      ON we.wip_entity_id = wdj.wip_entity_id
    INNER JOIN silver_bec_ods.mtl_system_items_b AS msi
      ON wdj.primary_item_id = msi.inventory_item_id
      AND wdj.organization_id = msi.organization_id
    WHERE
      COALESCE(DIM_PO_WIP_JOBS.distribution_id, 0) = COALESCE(prd.distribution_id, 0)
      AND COALESCE(DIM_PO_WIP_JOBS.wip_entity_id, 0) = COALESCE(wdj.wip_entity_id, 0)
      AND (
        prd.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po'
        )
        OR wdj.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po'
        )
      )
  );
/* insert */
INSERT INTO gold_bec_dwh.DIM_PO_WIP_JOBS
(
  SELECT
    prh.requisition_header_id,
    pod.po_header_id,
    we.wip_entity_id,
    prh.segment1 AS req_num,
    we.wip_entity_name,
    wdj.description AS wo_description,
    msi.segment1 AS wo_part_no,
    msi.description AS wo_part_description,
    prd.distribution_id,
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
    ) || '-' || COALESCE(prd.distribution_id, 0) || '-' || COALESCE(wdj.wip_entity_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.po_requisition_headers_all AS prh, silver_bec_ods.po_requisition_lines_all AS prl, silver_bec_ods.po_req_distributions_all AS prd, silver_bec_ods.po_distributions_all AS pod, silver_bec_ods.wip_entities AS we, silver_bec_ods.wip_discrete_jobs AS wdj, silver_bec_ods.mtl_system_items_b AS msi
  WHERE
    1 = 1
    AND (
      prd.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po'
      )
      OR wdj.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po'
      )
    )
    AND prh.interface_source_code = 'WIP'
    AND prh.requisition_header_id = prl.requisition_header_id
    AND prl.requisition_line_id = prd.requisition_line_id
    AND prd.distribution_id = pod.req_distribution_id
    AND prl.wip_entity_id = we.wip_entity_id
    AND we.wip_entity_id = wdj.wip_entity_id
    AND wdj.primary_item_id = msi.inventory_item_id
    AND wdj.organization_id = msi.organization_id
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_PO_WIP_JOBS SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.po_requisition_headers_all AS prh
    INNER JOIN silver_bec_ods.po_requisition_lines_all AS prl
      ON prh.requisition_header_id = prl.requisition_header_id
    INNER JOIN silver_bec_ods.po_req_distributions_all AS prd
      ON prl.requisition_line_id = prd.requisition_line_id
    INNER JOIN silver_bec_ods.po_distributions_all AS pod
      ON prd.distribution_id = pod.req_distribution_id
    INNER JOIN silver_bec_ods.wip_entities AS we
      ON prl.wip_entity_id = we.wip_entity_id
    INNER JOIN silver_bec_ods.wip_discrete_jobs AS wdj
      ON we.wip_entity_id = wdj.wip_entity_id
    INNER JOIN silver_bec_ods.mtl_system_items_b AS msi
      ON wdj.primary_item_id = msi.inventory_item_id
      AND wdj.organization_id = msi.organization_id
    WHERE
      COALESCE(DIM_PO_WIP_JOBS.distribution_id, 0) = COALESCE(prd.distribution_id, 0)
      AND COALESCE(DIM_PO_WIP_JOBS.wip_entity_id, 0) = COALESCE(wdj.wip_entity_id, 0)
      AND (
        prd.is_deleted_flg <> 'Y' OR wdj.is_deleted_flg <> 'Y'
      )
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po';