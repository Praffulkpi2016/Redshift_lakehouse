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

drop table if exists bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS;

create table bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS diststyle all sortkey(CUSTOMER_ID,PARTY_ID)
as
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
  rc.creation_date ,
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
	
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ar_customer_accounts'
	and batch_name = 'ar';

commit;
