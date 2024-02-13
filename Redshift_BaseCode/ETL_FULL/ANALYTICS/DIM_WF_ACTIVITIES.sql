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

drop table if exists bec_dwh.DIM_WF_ACTIVITIES;

create table bec_dwh.DIM_WF_ACTIVITIES
DISTKEY (ITEM_TYPE)
SORTKEY (ITEM_TYPE, TYPE, RUNNABLE_FLAG, BEGIN_DATE, END_DATE)
as
(
select 
	B.ITEM_TYPE,
	B.NAME,
	B.VERSION,
	B.TYPE,
	B.RERUN,
	B.EXPAND_ROLE,
	B.PROTECT_LEVEL,
	B.CUSTOM_LEVEL,
	B.BEGIN_DATE,
	B.END_DATE,
	B.FUNCTION,
	B.RESULT_TYPE,
	B.COST,
	B.READ_ROLE,
	B.WRITE_ROLE,
	B.EXECUTE_ROLE,
	B.ICON_NAME,
	B.MESSAGE,
	B.ERROR_PROCESS,
	B.RUNNABLE_FLAG,
	B.ERROR_ITEM_TYPE,
	B.FUNCTION_TYPE,
	B.EVENT_NAME,
	B.DIRECTION,
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
|| '-' || nvl(B.ITEM_TYPE, 'NA') 
|| '-' || nvl(B.NAME, 'NA')
|| '-' || nvl(B.VERSION, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
BEC_ODS.WF_ACTIVITIES B,
BEC_ODS.WF_ACTIVITIES_TL  T
WHERE 1=1
AND T.LANGUAGE = 'US'
AND B.TYPE = 'PROCESS'
AND B.runnable_flag = 'Y'
AND sysdate BETWEEN B.begin_date AND nvl(B.end_date, sysdate)
AND B.ITEM_TYPE = T.ITEM_TYPE
AND B.NAME = T.NAME
AND B.VERSION = T.VERSION
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_wf_activities'
	and batch_name = 'om';

commit;