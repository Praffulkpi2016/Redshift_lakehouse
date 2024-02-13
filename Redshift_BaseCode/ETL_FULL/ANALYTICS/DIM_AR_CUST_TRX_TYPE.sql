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

drop table if exists bec_dwh.DIM_AR_CUST_TRX_TYPE;

create table bec_dwh.DIM_AR_CUST_TRX_TYPE diststyle all sortkey(CUST_TRX_TYPE_ID)
as
(
select 
	CUST_TRX_TYPE_ID,
	LAST_UPDATE_DATE,
	STATUS,
	NAME CUST_TRX_TYPE_NAME,
	DESCRIPTION CUST_TRX_TYPE_DESCRIPTION,
	"TYPE" CUST_TRX_TYPE,
	SET_OF_BOOKS_ID,
	END_DATE,
	START_DATE,
	ORG_ID,
	LEGAL_ENTITY_ID ,
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
    || '-' || nvl(CUST_TRX_TYPE_ID, 0)
	|| '-' || nvl(ORG_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.RA_CUST_TRX_TYPES_ALL
);
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ar_cust_trx_type'
	and batch_name = 'ar';

commit;
