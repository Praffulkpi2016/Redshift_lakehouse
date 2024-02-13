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

drop table if exists bec_dwh.DIM_PO_NEED_BY;

create table bec_dwh.DIM_PO_NEED_BY diststyle all sortkey(po_header_id,po_line_id )
as 
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
    || '-'
       || nvl(po_header_id, 0)|| '-'
       || nvl(po_line_id, 0)   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	FROM bec_ods.po_line_locations_all 
	where is_deleted_flg <> 'Y'
	GROUP BY  po_header_id,  po_line_id
);

commit;

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_need_by'
	and batch_name = 'po';

commit;