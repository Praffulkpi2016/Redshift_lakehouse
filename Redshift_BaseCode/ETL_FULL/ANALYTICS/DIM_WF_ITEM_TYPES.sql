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

drop table if exists bec_dwh.DIM_WF_ITEM_TYPES;

create table bec_dwh.DIM_WF_ITEM_TYPES
	diststyle all sortkey(NAME)
as
(
select 
	B.NAME,
	B.PROTECT_LEVEL,
	B.CUSTOM_LEVEL,
	B.WF_SELECTOR,
	B.READ_ROLE,
	B.WRITE_ROLE,
	B.EXECUTE_ROLE,
	B.PERSISTENCE_TYPE,
	B.PERSISTENCE_DAYS,
	T.DISPLAY_NAME,
	T.DESCRIPTION,
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
|| '-' || nvl(B.NAME, 'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	BEC_ODS.WF_ITEM_TYPES B
	,
	BEC_ODS.WF_ITEM_TYPES_TL T
where
	B.NAME = T.NAME
	and T.LANGUAGE = 'US'
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_wf_item_types'
	and batch_name = 'om';

commit;