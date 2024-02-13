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

delete from bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS
where (nvl(CUSTOMER_ID,0),nvl(PARTY_ID,0)) in
(
select nvl(ods.CUSTOMER_ID,0),nvl(ods.PARTY_ID,0) from bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS dw, 
(select
  RC.CUST_ACCOUNT_ID "CUSTOMER_ID",hzp.party_id "PARTY_ID"
  from bec_ods.HZ_PARTIES HZP,
	bec_ods.HZ_CUST_ACCOUNTS RC
	WHERE 1 = 1
AND HZP.PARTY_ID = RC.PARTY_ID
and ( RC.kca_seq_date  > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_customer_accounts' and batch_name = 'ar')
OR
 hzp.kca_seq_date  > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_customer_accounts' and batch_name = 'ar')
 
)
) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'||nvl(ods.CUSTOMER_ID,0) ||'-'||nvl(ods.PARTY_ID,0)
);

commit;

-- Insert records

insert into bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS
(
customer_id,
	customer_category_code,
	customer_class_code,
	customer_group_code,
	customer_name,
	customer_number,
	customer_type,
	party_id,
	party_number,
	party_type,
	sales_channel_code,
	customer_status,
	category_code,
	address1,
	address2,
	address3,
	address4,
	city,
	country,
	county,
	postal_code,
	state_province,
	creation_date,
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select
    RC.CUST_ACCOUNT_ID "CUSTOMER_ID",
  HZP.CATEGORY_CODE "CUSTOMER_CATEGORY_CODE",
  rc.customer_class_code,
  'A' "CUSTOMER_GROUP_CODE",
  NVL(RC.ACCOUNT_NAME, HZP.PARTY_NAME) "CUSTOMER_NAME",
  rc.ACCOUNT_NUMBER "CUSTOMER_NUMBER",
  rc.customer_type,
  hzp.party_id "PARTY_ID",
  hzp.party_number,
  hzp.party_type,
  rc.sales_channel_code,
  rc.status "CUSTOMER_STATUS",
  hzp.category_code,
  hzp.address1,
  hzp.address2,
  hzp.address3,
  hzp.address4,
  hzp.city,
  hzp.country,
  hzp.county,
  hzp.postal_code,
  NVL (hzp.province, hzp.state) "STATE_PROVINCE",
  rc.creation_date,
  rc.last_update_date,
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
       || nvl(RC.CUST_ACCOUNT_ID, 0) || '-'|| nvl(RC.PARTY_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.HZ_PARTIES HZP,
	bec_ods.HZ_CUST_ACCOUNTS RC
WHERE 1 = 1
  --AND SUBSTRB(HZP.PARTY_NAME,1,50) = RC.CUSTOMER_NAME
AND HZP.PARTY_ID = RC.PARTY_ID
and (RC.kca_seq_date  > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_customer_accounts' and batch_name = 'ar')
OR
 hzp.kca_seq_date  > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_customer_accounts' and batch_name = 'ar')
)
 );

-- Soft delete

update bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS set is_deleted_flg = 'Y'
where (nvl(CUSTOMER_ID,0),nvl(PARTY_ID,0)) not in (
select nvl(ods.CUSTOMER_ID,0),nvl(ods.PARTY_ID,0) from bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS dw, 
(select
  RC.CUST_ACCOUNT_ID "CUSTOMER_ID",
  hzp.party_id "PARTY_ID",RC.last_update_date,RC.kca_operation
  from (select * from bec_ods.HZ_PARTIES where is_deleted_flg <> 'Y') HZP,
	   (select * from bec_ods.HZ_CUST_ACCOUNTS where is_deleted_flg <> 'Y') RC
	WHERE 1 = 1
AND HZP.PARTY_ID = RC.PARTY_ID
) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
||'-'||nvl(ods.CUSTOMER_ID,0)
||'-'||nvl(ods.PARTY_ID,0) 
);

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_customer_accounts' and batch_name = 'ar';

COMMIT;