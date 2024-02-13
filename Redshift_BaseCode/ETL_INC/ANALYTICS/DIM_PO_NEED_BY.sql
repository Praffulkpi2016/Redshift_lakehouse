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

delete from bec_dwh.dim_po_need_by
where (nvl(po_header_id,0), nvl(po_line_id,0)) in (
select nvl(ods.po_header_id) as po_header_id, nvl(ods.po_line_id,0) as po_line_id 
from bec_dwh.dim_po_need_by dw, 
(select po_header_id,po_line_id
 from bec_ods.po_line_locations_all 
 where( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
					  where dw_table_name ='dim_po_need_by' and batch_name = 'po')   
					or  is_deleted_flg = 'Y')
)ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'|| nvl(ods.po_header_id,0)
||'-'|| nvl(ods.po_line_id,0)
);

commit;

-- Insert records

insert into bec_dwh.DIM_PO_NEED_BY
(
	po_header_id, 
	po_line_id,
	 max_need_by ,
	 min_need_by , 
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
	po_header_id, 
	po_line_id,
	MAX ( need_by_date) max_need_by,
	MIN ( need_by_date) min_need_by,
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
    ||'-'|| nvl(po_header_id,0)
	||'-'|| nvl(po_line_id, 0)   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	FROM bec_ods.po_line_locations_all  
	where 1=1 
	and is_deleted_flg <> 'Y'
	and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='dim_po_need_by' and batch_name = 'po')
 )
	GROUP BY  po_header_id,  po_line_id
);

commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_need_by' and batch_name = 'po';

COMMIT;