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

delete
from
	bec_dwh.DIM_AR_CUSTOMER_SITES
where
	nvl(SITE_USE_ID,0) in
(
	select
		nvl(ods.SITE_USE_ID,0)	as SITE_USE_ID from
		bec_dwh.DIM_AR_CUSTOMER_SITES dw,
		bec_ods.HZ_CUST_SITE_USES_ALL ods
	where
		dw.dw_load_id = (select	system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')
        || '-' || nvl(ods.SITE_USE_ID, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_customer_sites'
				and batch_name = 'ar')
				 )
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_AR_CUSTOMER_SITES
(
	salesrep_id,
	resource_id,
	salesrep_name,
	salesrep_status,
	start_date_active,
	end_date_active,
	salesrep_number,
	org_id,
	person_id,
	party_id,
	party_number,
	party_name,
	party_type,
	country,
	address1,
	address2,
	address3,
	address4,
	city,
	postal_code,
	state,
	province,
	parties_status,
	county,
	category_code,
	cust_account_id,
	account_number,
	cust_account_status,
	customer_type,
	customer_class_code,
	warehouse_id,
	account_name,
	cust_acct_site_id,
	site_use_id,
	site_use_code,
	primary_flag,
	cust_site_acct_status,
	site_location,
	cust_site_use_warehouse_id,
	cust_site_use_org_id,
	last_update_date,
	customer_name,
	customer_number,
	customer_category,
	customer_class,
	customer_channel,
	profile_class_id,
	collector_id,
	collector_name,
	employee_id,
	description,
	collector_staus,
	inactive_date,
	collector_alias,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
RS.SALESREP_ID,
RS.RESOURCE_ID,
--RS.LAST_UPDATE_DATE,
RS.NAME,
RS.STATUS,
RS.START_DATE_ACTIVE,
RS.END_DATE_ACTIVE,
RS.SALESREP_NUMBER,
RS.ORG_ID,
RS.PERSON_ID,
PARTIES.PARTY_ID,
PARTIES.PARTY_NUMBER,
PARTIES.PARTY_NAME,
PARTIES.PARTY_TYPE,
PARTIES.COUNTRY,
PARTIES.ADDRESS1,
PARTIES.ADDRESS2,
PARTIES.ADDRESS3,
PARTIES.ADDRESS4,
PARTIES.CITY,
PARTIES.POSTAL_CODE,
PARTIES.STATE,
PARTIES.PROVINCE,
PARTIES.STATUS PARTIES_STATUS,
PARTIES.COUNTY,
PARTIES.CATEGORY_CODE,
HZCA.CUST_ACCOUNT_ID,
HZCA.ACCOUNT_NUMBER,
HZCA.STATUS CUST_ACCOUNT_STATUS,
HZCA.CUSTOMER_TYPE,
HZCA.CUSTOMER_CLASS_CODE,
HZCA.WAREHOUSE_ID,
HZCA.ACCOUNT_NAME,
HCAS.CUST_ACCT_SITE_ID,
HCSS.SITE_USE_ID,
HCSS.SITE_USE_CODE,
HCSS.PRIMARY_FLAG,
HCSS.STATUS CUST_ACCT_STATUS,
HCSS.LOCATION,
HCSS.WAREHOUSE_ID CUST_SITE_USE_WAREHOUSE_ID,
HCSS.ORG_ID CUST_SITE_USE_ORG_ID,
HCSS.last_update_date,
RC.CUSTOMER_NAME  CUSTOMER_NAME,
RC.CUSTOMER_NUMBER CUSTOMER_NUMBER,
RC.CUSTOMER_CATEGORY_CODE CUSTOMER_CATEGORY,
RC.CUSTOMER_CLASS_CODE CUSTOMER_CLASS,
RC.SALES_CHANNEL_CODE CUSTOMER_CHANNEL,
PRO.PROFILE_CLASS_ID,
COLL.COLLECTOR_ID,
COLL.NAME COLLECTOR_NAME,
COLL.EMPLOYEE_ID,
COLL.DESCRIPTION,
COLL.STATUS COLLECTOR_STAUS,
COLL.INACTIVE_DATE,
COLL.ALIAS,
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
    || '-' || nvl(HCSS.SITE_USE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.JTF_RS_SALESREPS RS,
bec_ods.HZ_PARTIES PARTIES,
bec_ods.HZ_CUST_ACCOUNTS HZCA,
bec_ods.HZ_CUST_ACCT_SITES_ALL HCAS,
bec_ods.HZ_CUST_SITE_USES_ALL HCSS,
bec_ods.AR_CUSTOMERS RC,
bec_ods.HZ_CUSTOMER_PROFILES PRO,
bec_ods.HZ_CUST_PROFILE_CLASSES PRC,
bec_ods.AR_COLLECTORS COLL
WHERE HCSS.CUST_ACCT_SITE_ID = HCAS.CUST_ACCT_SITE_ID
AND HCAS.CUST_ACCOUNT_ID     = HZCA.CUST_ACCOUNT_ID
AND HZCA.PARTY_ID            = PARTIES.PARTY_ID
AND HZCA.CUST_ACCOUNT_ID     = RC.CUSTOMER_ID
AND HCSS.PRIMARY_SALESREP_ID = RS.SALESREP_ID(+)
AND HCSS.SITE_USE_ID         = PRO.SITE_USE_ID(+)
AND PRO.PROFILE_CLASS_ID     = PRC.PROFILE_CLASS_ID(+)
AND PRO.COLLECTOR_ID         = COLL.COLLECTOR_ID(+)
AND HCSS.ORG_ID              = RS.ORG_ID(+)
			and (HCSS.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_customer_sites'
				and batch_name = 'ar')
				 )
 );
-- Soft delete

update
	bec_dwh.DIM_AR_CUSTOMER_SITES
set
	is_deleted_flg = 'Y'
where
	(nvl(site_use_id,0)) not in (
	select
		nvl(ods.site_use_id,0) as site_use_id
	from
		bec_dwh.DIM_AR_CUSTOMER_SITES dw,
		bec_ods.HZ_CUST_SITE_USES_ALL ods
	where
		dw.dw_load_id = (select	system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(ods.site_use_id, 0)
	and ods.is_deleted_flg <> 'Y'
);

commit;
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_customer_sites' and batch_name = 'ar';

COMMIT;