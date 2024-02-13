/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.DIM_REQ_PO_APPROVAL_HIST;

create table bec_dwh.DIM_REQ_PO_APPROVAL_HIST 
DISTKEY (requisition_id)
SORTKEY (req_num)
as 
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
    ood.operating_unit org_id,
	TO_CHAR(req_po.PO_ACTION_DATE,'MON-YY') period,
	datediff(second, req_po.PO_CREATION_DATE , PO_ACTION_DATE ) as po_creation_approve,
	datediff(second, req_po.REQ_ACTION_DATE , PO_ACTION_DATE ) as pr_approve_po_aprove,
	datediff(second, req_po.REQ_CREATION_DATE , REQ_ACTION_DATE ) as pr_creation_approve,
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
       || '-' || nvl (req_po.requisition_id, 0)
	   || '-' || nvl (req_po.po_header_id, 0) 
	   || '-' || nvl (req_po.po_release_no, 0)
	   || '-' || nvl (ood.organization_id,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
    (
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
        FROM
            (
                SELECT
                    pah.object_id            requisition_id,
                    prh.segment1             req_num,
                    pah.object_type_code     req_type,
                    pah.action_code          req_action,
                    pah.action_date          req_action_date,
                    ( nvl(
                        pah.object_revision_num, 0
                    ) )                      req_revision,
                    pah.sequence_num         seq_no,
                    prh.creation_date        req_creation_date,
                    ppf.full_name            req_approved_by,
                    prh.authorization_status req_status,
                    prh.interface_source_code,
                    prd.distribution_id
                FROM
                    bec_ods.po_action_history          pah,
                    bec_ods.per_all_people_f               ppf,
                    bec_ods.po_requisition_headers_all prh,
                    bec_ods.po_requisition_lines_all   prl,
                    bec_ods.po_req_distributions_all   prd
                WHERE
                    pah.object_type_code = 'REQUISITION'
                    AND pah.action_code = 'APPROVE'
                    AND pah.sequence_num IN (
                        SELECT
                            MAX(sequence_num)
                        FROM
                            bec_ods.po_action_history
                        WHERE
                            object_id = pah.object_id
                            AND object_type_code = 'REQUISITION'
                            AND action_code = 'APPROVE'
                    )
                    AND pah.employee_id = ppf.person_id
                    AND ppf.effective_end_date > sysdate
                    AND pah.object_id = prh.requisition_header_id
                    AND prh.requisition_header_id = prl.requisition_header_id
                    AND prl.requisition_line_id = prd.requisition_line_id
            ) req,
            (
                SELECT
                    poa.po_header_id,
                    poa.segment1             po_num,
                    ph.object_id             po_release_id,
                    poa.type_lookup_code     po_type,
                    ph.action_code           po_action,
                    ph.action_date           po_action_date,
                    poa.creation_date        po_creation_date,
                    ( nvl(
                        ph.object_revision_num, 0
                    ) )                      po_revision,
                    ph.sequence_num          po_seq,
                    per.full_name            po_approved_by,
                    poa.authorization_status po_status,
                    pda.req_distribution_id,
                    pra.release_num          po_release_no,
                    pll.ship_to_organization_id,
					poa.last_update_date
                FROM
                   bec_ods.po_headers_all        poa,
                    bec_ods.po_releases_all       pra,
                    bec_ods.po_line_locations_all pll,
                    bec_ods.po_distributions_all  pda,
                    bec_ods.po_action_history     ph,
                    bec_ods.per_all_people_f          per
                WHERE
                    poa.po_header_id = pra.po_header_id
                    AND pra.po_release_id = pll.po_release_id
                    AND pll.line_location_id = pda.line_location_id
                    AND pda.req_distribution_id IS NOT NULL
                    AND pra.po_release_id = ph.object_id
                    AND ph.object_type_code = 'RELEASE'
                    AND ph.action_code = 'APPROVE'
                    AND ph.employee_id = per.person_id
                    AND per.effective_end_date > sysdate
                    AND ph.sequence_num IN (
                        SELECT
                            MIN(sequence_num)
                        FROM
                            bec_ods.po_action_history
                        WHERE
                            object_id = ph.object_id
                            AND object_type_code = 'RELEASE'
                            AND action_code = 'APPROVE'
                    )
            ) blanket
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
        FROM
            (
                SELECT
                    pah.object_id            requisition_id,
                    prh.segment1             req_num,
                    pah.object_type_code     req_type,
                    pah.action_code          req_action,
                    pah.action_date          req_action_date,
                    ( nvl(
                        pah.object_revision_num, 0
                    ) )                      req_revision,
                    pah.sequence_num         seq_no,
                    prh.creation_date        req_creation_date,
                    ppf.full_name            req_approved_by,
                    prh.authorization_status req_status,
                    prh.interface_source_code,
                    prd.distribution_id
                FROM
                   bec_ods.po_action_history          pah,
                    bec_ods.per_all_people_f               ppf,
                    bec_ods.po_requisition_headers_all prh,
                    bec_ods.po_requisition_lines_all   prl,
                    bec_ods.po_req_distributions_all   prd
                WHERE
                    pah.object_type_code = 'REQUISITION'
                    AND pah.action_code = 'APPROVE'
                    AND pah.sequence_num IN (
                        SELECT
                            MAX(sequence_num)
                        FROM
                            bec_ods.po_action_history
                        WHERE
                            object_id = pah.object_id
                            AND object_type_code = 'REQUISITION'
                            AND action_code = 'APPROVE'
                    )
                    AND pah.employee_id = ppf.person_id
                    AND ppf.effective_end_date > sysdate
                    AND pah.object_id = prh.requisition_header_id
                    AND prh.requisition_header_id = prl.requisition_header_id
                    AND prl.requisition_line_id = prd.requisition_line_id
            ) req,
            (
                SELECT
                    poh.po_header_id,
                    poh.segment1             po_num,
                    ph.object_id             po_release_id,
                    poh.type_lookup_code     po_type,
                    ph.action_code           po_action,
                    ph.action_date           po_action_date,
                    poh.creation_date        po_creation_date,
                    ( nvl(
                        ph.object_revision_num, 0
                    ) )                      po_revision,
                    ph.sequence_num,
                    per.full_name            po_approved_by,
                    poh.authorization_status po_status,
                    pda.req_distribution_id,
                    pll.ship_to_organization_id,
					poh.last_update_date
                FROM
                    bec_ods.po_headers_all        poh,
                    bec_ods.po_line_locations_all pll,
                    bec_ods.po_distributions_all  pda,
                    bec_ods.po_action_history     ph,
                    bec_ods.per_all_people_f          per
                WHERE
                    pda.po_header_id = poh.po_header_id
                    AND pda.line_location_id = pll.line_location_id
                    AND pll.po_header_id = poh.po_header_id
                    AND poh.po_header_id = ph.object_id
                    AND ph.object_type_code = 'PO'
                    AND ph.action_code = 'APPROVE'
                    AND ph.employee_id = per.person_id
                    AND per.effective_end_date > sysdate
                    AND ph.sequence_num IN (
                        SELECT
                            MIN(sequence_num)
                        FROM
                            bec_ods.po_action_history
                        WHERE
                            object_id = ph.object_id
                            AND object_type_code = 'PO'
                            AND action_code = 'APPROVE'
                    )
            ) po
        WHERE
            req.distribution_id = po.req_distribution_id
    )                            req_po,
    bec_ods.org_organization_definitions ood
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
    req_po.req_num
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_req_po_approval_hist'
	and batch_name = 'po';

commit;