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

drop table if exists bec_dwh.DIM_AR_SALESREPS;

create table bec_dwh.DIM_AR_SALESREPS diststyle all sortkey(SALESREP_ID)
as
(
select
	SALESREP_ID,
	RESOURCE_ID,
	SALES_CREDIT_TYPE_ID,
	NAME,
	STATUS,
	PERSON_ID,
	SALESREP_NUMBER,
	ORG_ID,
	last_update_date,
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
    || '-' || nvl(salesrep_id, 0) 
    || '-' || nvl(org_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.JTF_RS_SALESREPS
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ar_salesreps'
	and batch_name = 'ar';

commit;