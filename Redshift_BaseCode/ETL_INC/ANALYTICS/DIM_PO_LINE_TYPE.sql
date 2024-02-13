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

-- Delete Records

delete from bec_dwh.DIM_PO_LINE_TYPE
where (nvl(line_type_id,0) ) in (
select  nvl(ods.line_type_id,0)  as LINE_TYPE_ID from bec_dwh.DIM_PO_LINE_TYPE dw, bec_ods.PO_LINE_TYPES_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.line_type_id,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_line_type' and batch_name = 'po')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_PO_LINE_TYPE
(
	line_type_id, 
	line_type, 
    line_type_desc,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
	line_type_id, 
	line_type, 
    DESCRIPTION line_type_desc,
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
    || '-'
       || nvl(LINE_TYPE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.PO_LINE_TYPES_TL
Where NVL(Language,'US') = 'US' 
AND	 (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_line_type' and batch_name = 'po')
 )
);
commit;
-- Soft delete

update bec_dwh.DIM_PO_LINE_TYPE set is_deleted_flg = 'Y'
where (nvl(line_type_id,0) ) not in (
select nvl(ods.line_type_id,0)  as LINE_TYPE_ID from bec_dwh.DIM_PO_LINE_TYPE dw, bec_ods.PO_LINE_TYPES_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.line_type_id,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_line_type' and batch_name = 'po';

COMMIT;