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

drop table if exists bec_dwh.DIM_PO_TYPES;

create table bec_dwh.DIM_PO_TYPES diststyle all sortkey(PO_TYPE)
as 
(
select
	LOOKUP_CODE as PO_TYPE,
	LOOKUP_TYPE,
	DISPLAYED_FIELD as PO_TYPE_DISPLAY, 
	DESCRIPTION  as PO_TYPE_DESC,
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
       || nvl(LOOKUP_CODE, '0')
       ||'-'||nvl(LOOKUP_TYPE,'0') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.PO_LOOKUP_CODES
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_types'
	and batch_name = 'po';

commit;
