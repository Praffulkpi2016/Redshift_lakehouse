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

drop table if exists bec_dwh.DIM_OM_SALES_REPS;

create table bec_dwh.DIM_OM_SALES_REPS
	diststyle all sortkey(SALESREP_ID,ORG_ID)
as
(
SELECT 
RS.ORG_ID,
RS.SALESREP_ID,
RS.SALESREP_NUMBER,
RS.NAME ,
RES.RESOURCE_NAME SALESREP_NAME,
RS.START_DATE_ACTIVE,
RS.END_DATE_ACTIVE,
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
||'-'|| nvl(RS.SALESREP_ID, 0) 
||'-'|| nvl(RS.ORG_ID, 0) as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM
	BEC_ODS.JTF_RS_SALESREPS RS,
	BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL RES
WHERE
	1 = 1
	AND RS.RESOURCE_ID = RES.RESOURCE_ID
	AND RS.STATUS = 'A'
	AND RES."LANGUAGE" = 'US'
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_om_sales_reps'
	and batch_name = 'om';

commit;