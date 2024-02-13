/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
--delete statement
DELETE FROM bec_dwh.DIM_PO_NAME
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.po_headers_all poh
    JOIN bec_ods.po_lines_all pol ON poh.po_header_id = pol.po_header_id
    WHERE NVL(DIM_PO_NAME.po_header_id, 0) = NVL(poh.po_header_id, 0)
    AND NVL(DIM_PO_NAME.po_line_id, 0) = NVL(pol.po_line_id, 0)
    AND pol.kca_seq_date > (
        SELECT MAX(executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_po_name' AND batch_name = 'po'
    )
);

commit;
--insert
insert into bec_dwh.dim_po_name
(
select
poh.po_header_id ,
pol.po_line_id,
poh.segment1 po_number,
pol.line_num,
poh.creation_date ,
poh.last_update_date,
--audit columns
'N' as is_deleted_flg,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'||nvl(poh.po_header_id,0)
||'-'||nvl(pol.po_line_id,0) as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date 
from
bec_ods.po_headers_all poh,
bec_ods.po_lines_all pol
where 1=1
and poh.po_header_id = pol.po_header_id
and pol.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_po_name' and batch_name = 'po')
);
commit;
--soft delete statement
update bec_dwh.dim_po_name set is_deleted_flg = 'Y'
WHERE NOT EXISTS  (
    SELECT 1
    FROM bec_ods.po_headers_all poh
    JOIN bec_ods.po_lines_all pol ON poh.po_header_id = pol.po_header_id
    WHERE NVL(DIM_PO_NAME.po_header_id, 0) = NVL(poh.po_header_id, 0)
    AND NVL(DIM_PO_NAME.po_line_id, 0) = NVL(pol.po_line_id, 0)
	and (poh.is_deleted_flg <> 'Y' or pol.is_deleted_flg <> 'Y')
);
commit;
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_name'
	and batch_name = 'po';

commit;