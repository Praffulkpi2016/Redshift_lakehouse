/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for DIMENSIONS.
# File Version: KPI v1.0
*/
begin;
--DELETE
DELETE FROM bec_dwh.DIM_PO_WIP_JOBS
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.po_requisition_headers_all prh
    INNER JOIN bec_ods.po_requisition_lines_all prl ON prh.requisition_header_id = prl.requisition_header_id
    INNER JOIN bec_ods.po_req_distributions_all prd ON prl.requisition_line_id = prd.requisition_line_id
    INNER JOIN bec_ods.po_distributions_all pod ON prd.distribution_id = pod.req_distribution_id
    INNER JOIN bec_ods.wip_entities we ON prl.wip_entity_id = we.wip_entity_id
    INNER JOIN bec_ods.wip_discrete_jobs wdj ON we.wip_entity_id = wdj.wip_entity_id
    INNER JOIN bec_ods.mtl_system_items_b msi ON wdj.primary_item_id = msi.inventory_item_id AND wdj.organization_id = msi.organization_id
    WHERE NVL(DIM_PO_WIP_JOBS.distribution_id, 0) = NVL(prd.distribution_id, 0)
    AND NVL(DIM_PO_WIP_JOBS.wip_entity_id, 0) = NVL(wdj.wip_entity_id, 0)
    AND (prd.kca_seq_date > (SELECT (executebegints - prune_days) FROM bec_etl_ctrl.batch_dw_info WHERE dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po')
    OR wdj.kca_seq_date > (SELECT (executebegints - prune_days) FROM bec_etl_ctrl.batch_dw_info WHERE dw_table_name = 'dim_po_wip_jobs' AND batch_name = 'po'))
);
commit;
--insert
insert into bec_dwh.DIM_PO_WIP_JOBS 
(
select
prh.requisition_header_id,
pod.po_header_id,
we.wip_entity_id,
prh.segment1 req_num,
we.wip_entity_name,
wdj.description wo_description,
msi.segment1 wo_part_no,
msi.description wo_part_description,
prd.distribution_id,
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
 ||'-'|| nvl(prd.distribution_id, 0) 
  ||'-'||nvl(wdj.wip_entity_id, 0) as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM 
bec_ods.po_requisition_headers_all prh,
bec_ods.po_requisition_lines_all prl,
bec_ods.po_req_distributions_all prd,
bec_ods.po_distributions_all pod,
bec_ods.wip_entities we,
bec_ods.wip_discrete_jobs wdj,
bec_ods.mtl_system_items_b  msi
WHERE 1=1
AND (prd.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
where dw_table_name ='dim_po_wip_jobs' and batch_name = 'po')
OR 
wdj.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
where dw_table_name ='dim_po_wip_jobs' and batch_name = 'po'))
AND prh.interface_source_code = 'WIP'
AND prh.requisition_header_id = prl.requisition_header_id
AND prl.requisition_line_id = prd.requisition_line_id
AND prd.distribution_id = pod.req_distribution_id
AND prl.wip_entity_id = we.wip_entity_id
AND we.wip_entity_id = wdj.wip_entity_id
AND wdj.primary_item_id = msi.inventory_item_id
AND wdj.organization_id = msi.organization_id
);
commit;
--Soft delete
update bec_dwh.DIM_PO_WIP_JOBS set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
    SELECT 1
    FROM bec_ods.po_requisition_headers_all prh
    INNER JOIN bec_ods.po_requisition_lines_all prl ON prh.requisition_header_id = prl.requisition_header_id
    INNER JOIN bec_ods.po_req_distributions_all prd ON prl.requisition_line_id = prd.requisition_line_id
    INNER JOIN bec_ods.po_distributions_all pod ON prd.distribution_id = pod.req_distribution_id
    INNER JOIN bec_ods.wip_entities we ON prl.wip_entity_id = we.wip_entity_id
    INNER JOIN bec_ods.wip_discrete_jobs wdj ON we.wip_entity_id = wdj.wip_entity_id
    INNER JOIN bec_ods.mtl_system_items_b msi ON wdj.primary_item_id = msi.inventory_item_id AND wdj.organization_id = msi.organization_id
    WHERE NVL(DIM_PO_WIP_JOBS.distribution_id, 0) = NVL(prd.distribution_id, 0)
    AND NVL(DIM_PO_WIP_JOBS.wip_entity_id, 0) = NVL(wdj.wip_entity_id, 0)
    AND (prd.is_deleted_flg <> 'Y' or wdj.is_deleted_flg <> 'Y')
);
commit;


end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_wip_jobs'
	and batch_name = 'po';

commit;