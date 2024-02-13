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
delete from bec_dwh.DIM_OM_SALES_REPS
where (nvl(SALESREP_ID, 0),nvl(ORG_ID, 0)) 
in 
(
select nvl(ods.SALESREP_ID, 0) as SALESREP_ID,nvl(ods.ORG_ID, 0) as ORG_ID
from bec_dwh.DIM_OM_SALES_REPS dw,
(
select
RS.ORG_ID,
RS.SALESREP_ID,
RS.SALESREP_NUMBER,
RS.NAME ,
RES.RESOURCE_NAME SALESREP_NAME,
RS.START_DATE_ACTIVE,
RS.END_DATE_ACTIVE
FROM
	BEC_ODS.JTF_RS_SALESREPS RS,
	BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL RES
WHERE
	1 = 1
	AND RS.RESOURCE_ID = RES.RESOURCE_ID
	AND RS.STATUS = 'A'
	AND RES."LANGUAGE" = 'US'
	AND (RS.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_sales_reps' and batch_name = 'om')
OR
RES.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_sales_reps' and batch_name = 'om')
 
)
)ods
where 1=1 
and dw.dw_load_id = 
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(ods.SALESREP_ID, 0) 
||'-'|| nvl(odS.ORG_ID, 0)
);
commit;

insert into bec_dwh.DIM_OM_SALES_REPS
(
ORG_ID,
SALESREP_ID,
SALESREP_NUMBER,
NAME,
SALESREP_NAME,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
IS_DELETED_FLG,
source_app_id,
dw_load_id,
dw_insert_date,
dw_update_date
)
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
		AND (RS.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_sales_reps' and batch_name = 'om')
OR
RES.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_sales_reps' and batch_name = 'om')
 
)
);
commit;

-- soft delete
update bec_dwh.DIM_OM_SALES_REPS set is_deleted_flg = 'Y'
where (nvl(SALESREP_ID, 0),nvl(ORG_ID, 0)) 
not in 
(
select nvl(ods.SALESREP_ID, 0) as SALESREP_ID,nvl(ods.ORG_ID, 0) as ORG_ID
from bec_dwh.DIM_OM_SALES_REPS dw,
(
select
RS.ORG_ID,
RS.SALESREP_ID
FROM
	(select * from BEC_ODS.JTF_RS_SALESREPS where is_deleted_flg <> 'Y') RS,
	(select * from BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL where is_deleted_flg <> 'Y') RES
WHERE
	1 = 1
	AND RS.RESOURCE_ID = RES.RESOURCE_ID
	AND RS.STATUS = 'A'
	AND RES."LANGUAGE" = 'US'
)ods
where 1=1 
and dw.dw_load_id =
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(ods.SALESREP_ID, 0) 
||'-'|| nvl(odS.ORG_ID, 0)
);
commit;
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