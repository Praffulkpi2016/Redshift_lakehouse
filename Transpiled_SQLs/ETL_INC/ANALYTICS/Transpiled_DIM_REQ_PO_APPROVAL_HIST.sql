/* delete */
DELETE FROM gold_bec_dwh.DIM_REQ_PO_APPROVAL_HIST
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        req_po.requisition_id,
        req_po.req_num,
        req_po.req_type,
        req_po.req_action,
        req_po.req_action_date,
        req_po.req_revision,
        req_po.seq_no,
        req_po.req_creation_date,
        req_po.req_approved_by,
        req_po.req_status,
        req_po.interface_source_code,
        req_po.po_header_id,
        req_po.po_num,
        req_po.po_type,
        req_po.po_action,
        req_po.po_action_date,
        req_po.po_creation_date,
        req_po.po_revision,
        req_po.po_seq,
        req_po.po_approved_by,
        req_po.po_status,
        req_po.po_release_no,
        req_po.ship_to_organization_id,
        req_po.last_update_date,
        ood.organization_id,
        ood.operating_unit AS org_id,
        DATE_FORMAT(req_po.PO_ACTION_DATE, 'MON-yy') AS period,
        UNIX_TIMESTAMP(PO_ACTION_DATE) - UNIX_TIMESTAMP(req_po.PO_CREATION_DATE) AS po_creation_approve,
        UNIX_TIMESTAMP(PO_ACTION_DATE) - UNIX_TIMESTAMP(req_po.REQ_ACTION_DATE) AS pr_approve_po_aprove,
        UNIX_TIMESTAMP(REQ_ACTION_DATE) - UNIX_TIMESTAMP(req_po.REQ_CREATION_DATE) AS pr_creation_approve,
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
        ) || '-' || COALESCE(req_po.requisition_id, 0) || '-' || COALESCE(req_po.po_header_id, 0) || '-' || COALESCE(req_po.po_release_no, 0) || '-' || COALESCE(ood.organization_id, 0) AS dw_load_id
      FROM (
        SELECT
          req.requisition_id,
          req.req_num,
          req.req_type,
          req.req_action,
          req.req_action_date,
          req.req_revision,
          req.seq_no,
          req.req_creation_date,
          req.req_approved_by,
          req.req_status,
          req.interface_source_code,
          req.distribution_id,
          blanket.po_header_id,
          blanket.po_num,
          blanket.po_type,
          blanket.po_action,
          blanket.po_action_date,
          blanket.po_creation_date,
          blanket.po_revision,
          blanket.po_seq,
          blanket.po_approved_by,
          blanket.po_status,
          blanket.po_release_no,
          blanket.ship_to_organization_id,
          blanket.last_update_date
        FROM (
          SELECT
            pah.object_id AS requisition_id,
            prh.segment1 AS req_num,
            pah.object_type_code AS req_type,
            pah.action_code AS req_action,
            pah.action_date AS req_action_date,
            (
              COALESCE(pah.object_revision_num, 0)
            ) AS req_revision,
            pah.sequence_num AS seq_no,
            prh.creation_date AS req_creation_date,
            ppf.full_name AS req_approved_by,
            prh.authorization_status AS req_status,
            prh.interface_source_code,
            prd.distribution_id
          FROM silver_bec_ods.po_action_history AS pah, silver_bec_ods.per_all_people_f AS ppf, silver_bec_ods.po_requisition_headers_all AS prh, silver_bec_ods.po_requisition_lines_all AS prl, silver_bec_ods.po_req_distributions_all AS prd
          WHERE
            pah.object_type_code = 'REQUISITION'
            AND pah.action_code = 'APPROVE'
            AND pah.sequence_num IN (
              SELECT
                MAX(sequence_num)
              FROM silver_bec_ods.po_action_history
              WHERE
                object_id = pah.object_id AND object_type_code = 'REQUISITION' AND action_code = 'APPROVE'
            )
            AND pah.employee_id = ppf.person_id
            AND ppf.effective_end_date > CURRENT_TIMESTAMP()
            AND pah.object_id = prh.requisition_header_id
            AND prh.requisition_header_id = prl.requisition_header_id
            AND prl.requisition_line_id = prd.requisition_line_id
        ) AS req, (
          SELECT
            poa.po_header_id,
            poa.segment1 AS po_num,
            ph.object_id AS po_release_id,
            poa.type_lookup_code AS po_type,
            ph.action_code AS po_action,
            ph.action_date AS po_action_date,
            poa.creation_date AS po_creation_date,
            (
              COALESCE(ph.object_revision_num, 0)
            ) AS po_revision,
            ph.sequence_num AS po_seq,
            per.full_name AS po_approved_by,
            poa.authorization_status AS po_status,
            pda.req_distribution_id,
            pra.release_num AS po_release_no,
            pll.ship_to_organization_id,
            poa.last_update_date
          FROM silver_bec_ods.po_headers_all AS poa, silver_bec_ods.po_releases_all AS pra, silver_bec_ods.po_line_locations_all AS pll, silver_bec_ods.po_distributions_all AS pda, silver_bec_ods.po_action_history AS ph, silver_bec_ods.per_all_people_f AS per
          WHERE
            poa.po_header_id = pra.po_header_id
            AND pra.po_release_id = pll.po_release_id
            AND pll.line_location_id = pda.line_location_id
            AND NOT pda.req_distribution_id IS NULL
            AND pra.po_release_id = ph.object_id
            AND ph.object_type_code = 'RELEASE'
            AND ph.action_code = 'APPROVE'
            AND ph.employee_id = per.person_id
            AND per.effective_end_date > CURRENT_TIMESTAMP()
            AND ph.sequence_num IN (
              SELECT
                MIN(sequence_num)
              FROM silver_bec_ods.po_action_history
              WHERE
                object_id = ph.object_id AND object_type_code = 'RELEASE' AND action_code = 'APPROVE'
            )
        ) AS blanket
        WHERE
          req.distribution_id = blanket.req_distribution_id
        UNION ALL
        SELECT
          req.requisition_id,
          req.req_num,
          req.req_type,
          req.req_action,
          req.req_action_date,
          req.req_revision,
          req.seq_no,
          req.req_creation_date,
          req.req_approved_by,
          req.req_status,
          req.interface_source_code,
          req.distribution_id,
          po.po_header_id,
          po.po_num,
          po.po_type,
          po.po_action,
          po.po_action_date,
          po.po_creation_date,
          po.po_revision,
          po.sequence_num,
          po.po_approved_by,
          po.po_status,
          NULL,
          po.ship_to_organization_id,
          po.last_update_date
        FROM (
          SELECT
            pah.object_id AS requisition_id,
            prh.segment1 AS req_num,
            pah.object_type_code AS req_type,
            pah.action_code AS req_action,
            pah.action_date AS req_action_date,
            (
              COALESCE(pah.object_revision_num, 0)
            ) AS req_revision,
            pah.sequence_num AS seq_no,
            prh.creation_date AS req_creation_date,
            ppf.full_name AS req_approved_by,
            prh.authorization_status AS req_status,
            prh.interface_source_code,
            prd.distribution_id
          FROM silver_bec_ods.po_action_history AS pah, silver_bec_ods.per_all_people_f AS ppf, silver_bec_ods.po_requisition_headers_all AS prh, silver_bec_ods.po_requisition_lines_all AS prl, silver_bec_ods.po_req_distributions_all AS prd
          WHERE
            pah.object_type_code = 'REQUISITION'
            AND pah.action_code = 'APPROVE'
            AND pah.sequence_num IN (
              SELECT
                MAX(sequence_num)
              FROM silver_bec_ods.po_action_history
              WHERE
                object_id = pah.object_id AND object_type_code = 'REQUISITION' AND action_code = 'APPROVE'
            )
            AND pah.employee_id = ppf.person_id
            AND ppf.effective_end_date > CURRENT_TIMESTAMP()
            AND pah.object_id = prh.requisition_header_id
            AND prh.requisition_header_id = prl.requisition_header_id
            AND prl.requisition_line_id = prd.requisition_line_id
        ) AS req, (
          SELECT
            poh.po_header_id,
            poh.segment1 AS po_num,
            ph.object_id AS po_release_id,
            poh.type_lookup_code AS po_type,
            ph.action_code AS po_action,
            ph.action_date AS po_action_date,
            poh.creation_date AS po_creation_date,
            (
              COALESCE(ph.object_revision_num, 0)
            ) AS po_revision,
            ph.sequence_num,
            per.full_name AS po_approved_by,
            poh.authorization_status AS po_status,
            pda.req_distribution_id,
            pll.ship_to_organization_id,
            poh.last_update_date
          FROM silver_bec_ods.po_headers_all AS poh, silver_bec_ods.po_line_locations_all AS pll, silver_bec_ods.po_distributions_all AS pda, silver_bec_ods.po_action_history AS ph, silver_bec_ods.per_all_people_f AS per
          WHERE
            pda.po_header_id = poh.po_header_id
            AND pda.line_location_id = pll.line_location_id
            AND pll.po_header_id = poh.po_header_id
            AND poh.po_header_id = ph.object_id
            AND ph.object_type_code = 'PO'
            AND ph.action_code = 'APPROVE'
            AND ph.employee_id = per.person_id
            AND per.effective_end_date > CURRENT_TIMESTAMP()
            AND ph.sequence_num IN (
              SELECT
                MIN(sequence_num)
              FROM silver_bec_ods.po_action_history
              WHERE
                object_id = ph.object_id AND object_type_code = 'PO' AND action_code = 'APPROVE'
            )
        ) AS po
        WHERE
          req.distribution_id = po.req_distribution_id
      ) AS req_po, silver_bec_ods.org_organization_definitions AS ood
      WHERE
        req_po.ship_to_organization_id = ood.organization_id
      GROUP BY
        req_po.requisition_id,
        req_po.req_num,
        req_po.req_type,
        req_po.req_action,
        req_po.req_action_date,
        req_po.req_revision,
        req_po.seq_no,
        req_po.req_creation_date,
        req_po.req_approved_by,
        req_po.req_status,
        req_po.interface_source_code,
        req_po.po_header_id,
        req_po.po_num,
        req_po.po_type,
        req_po.po_action,
        req_po.po_action_date,
        req_po.po_creation_date,
        req_po.po_revision,
        req_po.po_seq,
        req_po.po_approved_by,
        req_po.po_status,
        req_po.po_release_no,
        req_po.ship_to_organization_id,
        req_po.last_update_date,
        ood.organization_id,
        ood.operating_unit
      ORDER BY
        req_po.req_num NULLS LAST
    ) AS ods
    WHERE
      1 = 1
      AND COALESCE(DIM_REQ_PO_APPROVAL_HIST.requisition_id, 0) = COALESCE(ods.requisition_id, 0)
      AND COALESCE(DIM_REQ_PO_APPROVAL_HIST.po_header_id, 0) = COALESCE(ods.po_header_id, 0)
      AND COALESCE(DIM_REQ_PO_APPROVAL_HIST.po_release_no, 0) = COALESCE(ods.po_release_no, 0)
      AND COALESCE(DIM_REQ_PO_APPROVAL_HIST.organization_id, 0) = COALESCE(ods.organization_id, 0)
  );
/* insert */
INSERT INTO gold_bec_dwh.DIM_REQ_PO_APPROVAL_HIST
(
  SELECT
    req_po.requisition_id,
    req_po.req_num,
    req_po.req_type,
    req_po.req_action,
    req_po.req_action_date,
    req_po.req_revision,
    req_po.seq_no,
    req_po.req_creation_date,
    req_po.req_approved_by,
    req_po.req_status,
    req_po.interface_source_code,
    req_po.po_header_id,
    req_po.po_num,
    req_po.po_type,
    req_po.po_action,
    req_po.po_action_date,
    req_po.po_creation_date,
    req_po.po_revision,
    req_po.po_seq,
    req_po.po_approved_by,
    req_po.po_status,
    req_po.po_release_no,
    req_po.ship_to_organization_id,
    req_po.last_update_date,
    ood.organization_id,
    ood.operating_unit AS org_id,
    DATE_FORMAT(req_po.PO_ACTION_DATE, 'MON-yy') AS period,
    UNIX_TIMESTAMP(PO_ACTION_DATE) - UNIX_TIMESTAMP(req_po.PO_CREATION_DATE) AS po_creation_approve,
    UNIX_TIMESTAMP(PO_ACTION_DATE) - UNIX_TIMESTAMP(req_po.REQ_ACTION_DATE) AS pr_approve_po_aprove,
    UNIX_TIMESTAMP(REQ_ACTION_DATE) - UNIX_TIMESTAMP(req_po.REQ_CREATION_DATE) AS pr_creation_approve,
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
    ) || '-' || COALESCE(req_po.requisition_id, 0) || '-' || COALESCE(req_po.po_header_id, 0) || '-' || COALESCE(req_po.po_release_no, 0) || '-' || COALESCE(ood.organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      req.requisition_id,
      req.req_num,
      req.req_type,
      req.req_action,
      req.req_action_date,
      req.req_revision,
      req.seq_no,
      req.req_creation_date,
      req.req_approved_by,
      req.req_status,
      req.interface_source_code,
      req.distribution_id,
      blanket.po_header_id,
      blanket.po_num,
      blanket.po_type,
      blanket.po_action,
      blanket.po_action_date,
      blanket.po_creation_date,
      blanket.po_revision,
      blanket.po_seq,
      blanket.po_approved_by,
      blanket.po_status,
      blanket.po_release_no,
      blanket.ship_to_organization_id,
      blanket.last_update_date
    FROM (
      SELECT
        pah.object_id AS requisition_id,
        prh.segment1 AS req_num,
        pah.object_type_code AS req_type,
        pah.action_code AS req_action,
        pah.action_date AS req_action_date,
        (
          COALESCE(pah.object_revision_num, 0)
        ) AS req_revision,
        pah.sequence_num AS seq_no,
        prh.creation_date AS req_creation_date,
        ppf.full_name AS req_approved_by,
        prh.authorization_status AS req_status,
        prh.interface_source_code,
        prd.distribution_id
      FROM silver_bec_ods.po_action_history AS pah, silver_bec_ods.per_all_people_f AS ppf, silver_bec_ods.po_requisition_headers_all AS prh, silver_bec_ods.po_requisition_lines_all AS prl, silver_bec_ods.po_req_distributions_all AS prd
      WHERE
        pah.object_type_code = 'REQUISITION'
        AND pah.action_code = 'APPROVE'
        AND pah.sequence_num IN (
          SELECT
            MAX(sequence_num)
          FROM silver_bec_ods.po_action_history
          WHERE
            object_id = pah.object_id AND object_type_code = 'REQUISITION' AND action_code = 'APPROVE'
        )
        AND pah.employee_id = ppf.person_id
        AND ppf.effective_end_date > CURRENT_TIMESTAMP()
        AND pah.object_id = prh.requisition_header_id
        AND prh.requisition_header_id = prl.requisition_header_id
        AND prl.requisition_line_id = prd.requisition_line_id
    ) AS req, (
      SELECT
        poa.po_header_id,
        poa.segment1 AS po_num,
        ph.object_id AS po_release_id,
        poa.type_lookup_code AS po_type,
        ph.action_code AS po_action,
        ph.action_date AS po_action_date,
        poa.creation_date AS po_creation_date,
        (
          COALESCE(ph.object_revision_num, 0)
        ) AS po_revision,
        ph.sequence_num AS po_seq,
        per.full_name AS po_approved_by,
        poa.authorization_status AS po_status,
        pda.req_distribution_id,
        pra.release_num AS po_release_no,
        pll.ship_to_organization_id,
        poa.last_update_date
      FROM silver_bec_ods.po_headers_all AS poa, silver_bec_ods.po_releases_all AS pra, silver_bec_ods.po_line_locations_all AS pll, silver_bec_ods.po_distributions_all AS pda, silver_bec_ods.po_action_history AS ph, silver_bec_ods.per_all_people_f AS per
      WHERE
        poa.po_header_id = pra.po_header_id
        AND pra.po_release_id = pll.po_release_id
        AND pll.line_location_id = pda.line_location_id
        AND NOT pda.req_distribution_id IS NULL
        AND pra.po_release_id = ph.object_id
        AND ph.object_type_code = 'RELEASE'
        AND ph.action_code = 'APPROVE'
        AND ph.employee_id = per.person_id
        AND per.effective_end_date > CURRENT_TIMESTAMP()
        AND ph.sequence_num IN (
          SELECT
            MIN(sequence_num)
          FROM silver_bec_ods.po_action_history
          WHERE
            object_id = ph.object_id AND object_type_code = 'RELEASE' AND action_code = 'APPROVE'
        )
    ) AS blanket
    WHERE
      req.distribution_id = blanket.req_distribution_id
    UNION ALL
    SELECT
      req.requisition_id,
      req.req_num,
      req.req_type,
      req.req_action,
      req.req_action_date,
      req.req_revision,
      req.seq_no,
      req.req_creation_date,
      req.req_approved_by,
      req.req_status,
      req.interface_source_code,
      req.distribution_id,
      po.po_header_id,
      po.po_num,
      po.po_type,
      po.po_action,
      po.po_action_date,
      po.po_creation_date,
      po.po_revision,
      po.sequence_num,
      po.po_approved_by,
      po.po_status,
      NULL,
      po.ship_to_organization_id,
      po.last_update_date
    FROM (
      SELECT
        pah.object_id AS requisition_id,
        prh.segment1 AS req_num,
        pah.object_type_code AS req_type,
        pah.action_code AS req_action,
        pah.action_date AS req_action_date,
        (
          COALESCE(pah.object_revision_num, 0)
        ) AS req_revision,
        pah.sequence_num AS seq_no,
        prh.creation_date AS req_creation_date,
        ppf.full_name AS req_approved_by,
        prh.authorization_status AS req_status,
        prh.interface_source_code,
        prd.distribution_id
      FROM silver_bec_ods.po_action_history AS pah, silver_bec_ods.per_all_people_f AS ppf, silver_bec_ods.po_requisition_headers_all AS prh, silver_bec_ods.po_requisition_lines_all AS prl, silver_bec_ods.po_req_distributions_all AS prd
      WHERE
        pah.object_type_code = 'REQUISITION'
        AND pah.action_code = 'APPROVE'
        AND pah.sequence_num IN (
          SELECT
            MAX(sequence_num)
          FROM silver_bec_ods.po_action_history
          WHERE
            object_id = pah.object_id AND object_type_code = 'REQUISITION' AND action_code = 'APPROVE'
        )
        AND pah.employee_id = ppf.person_id
        AND ppf.effective_end_date > CURRENT_TIMESTAMP()
        AND pah.object_id = prh.requisition_header_id
        AND prh.requisition_header_id = prl.requisition_header_id
        AND prl.requisition_line_id = prd.requisition_line_id
    ) AS req, (
      SELECT
        poh.po_header_id,
        poh.segment1 AS po_num,
        ph.object_id AS po_release_id,
        poh.type_lookup_code AS po_type,
        ph.action_code AS po_action,
        ph.action_date AS po_action_date,
        poh.creation_date AS po_creation_date,
        (
          COALESCE(ph.object_revision_num, 0)
        ) AS po_revision,
        ph.sequence_num,
        per.full_name AS po_approved_by,
        poh.authorization_status AS po_status,
        pda.req_distribution_id,
        pll.ship_to_organization_id,
        poh.last_update_date
      FROM silver_bec_ods.po_headers_all AS poh, silver_bec_ods.po_line_locations_all AS pll, silver_bec_ods.po_distributions_all AS pda, silver_bec_ods.po_action_history AS ph, silver_bec_ods.per_all_people_f AS per
      WHERE
        pda.po_header_id = poh.po_header_id
        AND pda.line_location_id = pll.line_location_id
        AND pll.po_header_id = poh.po_header_id
        AND poh.po_header_id = ph.object_id
        AND ph.object_type_code = 'PO'
        AND ph.action_code = 'APPROVE'
        AND ph.employee_id = per.person_id
        AND per.effective_end_date > CURRENT_TIMESTAMP()
        AND ph.sequence_num IN (
          SELECT
            MIN(sequence_num)
          FROM silver_bec_ods.po_action_history
          WHERE
            object_id = ph.object_id AND object_type_code = 'PO' AND action_code = 'APPROVE'
        )
    ) AS po
    WHERE
      req.distribution_id = po.req_distribution_id
  ) AS req_po, silver_bec_ods.org_organization_definitions AS ood
  WHERE
    req_po.ship_to_organization_id = ood.organization_id
  GROUP BY
    req_po.requisition_id,
    req_po.req_num,
    req_po.req_type,
    req_po.req_action,
    req_po.req_action_date,
    req_po.req_revision,
    req_po.seq_no,
    req_po.req_creation_date,
    req_po.req_approved_by,
    req_po.req_status,
    req_po.interface_source_code,
    req_po.po_header_id,
    req_po.po_num,
    req_po.po_type,
    req_po.po_action,
    req_po.po_action_date,
    req_po.po_creation_date,
    req_po.po_revision,
    req_po.po_seq,
    req_po.po_approved_by,
    req_po.po_status,
    req_po.po_release_no,
    req_po.ship_to_organization_id,
    req_po.last_update_date,
    ood.organization_id,
    ood.operating_unit
  ORDER BY
    req_po.req_num NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_req_po_approval_hist' AND batch_name = 'po';