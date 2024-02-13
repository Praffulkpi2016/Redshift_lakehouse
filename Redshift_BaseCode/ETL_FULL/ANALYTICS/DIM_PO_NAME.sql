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
drop table if exists bec_dwh.DIM_PO_NAME cascade;
create table bec_dwh.dim_po_name diststyle all 
sortkey(po_header_id)
as 
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
and (poh.is_deleted_flg <> 'Y' or pol.is_deleted_flg <> 'Y')
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_name'
and batch_name = 'po';

COMMIT; 