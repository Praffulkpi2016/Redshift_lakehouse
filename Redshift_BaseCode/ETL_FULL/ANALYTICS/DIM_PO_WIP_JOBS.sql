/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for DIMENSIONS.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.DIM_PO_WIP_JOBS;

create table bec_dwh.DIM_PO_WIP_JOBS
DISTKEY (wip_entity_id) 
SORTKEY (requisition_header_id, wip_entity_id)
as 
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
AND prh.interface_source_code = 'WIP'
AND prh.requisition_header_id = prl.requisition_header_id
AND prl.requisition_line_id = prd.requisition_line_id
AND prd.distribution_id = pod.req_distribution_id
AND prl.wip_entity_id = we.wip_entity_id
AND we.wip_entity_id = wdj.wip_entity_id
AND wdj.primary_item_id = msi.inventory_item_id
AND wdj.organization_id = msi.organization_id
);

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