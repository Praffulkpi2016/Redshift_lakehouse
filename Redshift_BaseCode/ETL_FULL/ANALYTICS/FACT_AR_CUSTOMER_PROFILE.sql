/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.FACT_AR_CUSTOMER_PROFILE;

create table bec_dwh.FACT_AR_CUSTOMER_PROFILE 
	diststyle all
	sortkey (CUST_ACCOUNT_PROFILE_ID,PARTY_ID, CUST_ACCT_PROFILE_AMT_ID)
as
(
select
	HPA.CUST_ACCT_PROFILE_AMT_ID as CUST_ACCT_PROFILE_AMT_ID,
	HPA.CUST_ACCOUNT_PROFILE_ID as CUST_ACCOUNT_PROFILE_ID,
	HPA.CURRENCY_CODE as CURRENCY_CODE,
	HPA.TRX_CREDIT_LIMIT as TRX_CREDIT_LIMIT,
	HPA.OVERALL_CREDIT_LIMIT as OVERALL_CREDIT_LIMIT,
	HPA.MIN_DUNNING_AMOUNT as MIN_DUNNING_AMOUNT,
	HPA.MIN_DUNNING_INVOICE_AMOUNT as MIN_DUNNING_INVOICE_AMOUNT,
	HPA.MAX_INTEREST_CHARGE as MAX_INTEREST_CHARGE,
	HPA.MIN_STATEMENT_AMOUNT as MIN_STATEMENT_AMOUNT,
	HPA.INTEREST_RATE as INTEREST_RATE,
	HPA.ATTRIBUTE_CATEGORY as ATTRIBUTE_CATEGORY,
	HPA.MIN_FC_BALANCE_AMOUNT as MIN_FC_BALANCE_AMOUNT,
	HPA.MIN_FC_INVOICE_AMOUNT as MIN_FC_INVOICE_AMOUNT,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||HPA.CUST_ACCOUNT_ID as CUST_ACCOUNT_ID_KEY,
	HPA.CUST_ACCOUNT_ID as CUST_ACCOUNT_ID,
	HPA.SITE_USE_ID as CUSTOMER_SITE_USE_ID,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| HPA.SITE_USE_ID as CUSTOMER_SITE_USE_ID_KEY,
	HPA.EXPIRATION_DATE as EXPIRATION_DATE,
	HPA.EXCHANGE_RATE_TYPE as EXCHANGE_RATE_TYPE,
	HPA.INTEREST_TYPE as INTEREST_TYPE,
	HPA.INTEREST_FIXED_AMOUNT as INTEREST_FIXED_AMOUNT,
	HPA.INTEREST_SCHEDULE_ID as INTEREST_SCHEDULE_ID,
	HPA.PENALTY_TYPE as PENALTY_TYPE,
	HPA.PENALTY_RATE as PENALTY_RATE,
	HPA.MIN_INTEREST_CHARGE as MIN_INTEREST_CHARGE,
	HPA.PENALTY_FIXED_AMOUNT as PENALTY_FIXED_AMOUNT,
	HPA.last_update_date as last_update_date,
	HCP.CUST_ACCOUNT_ID             AS CUST_PROF_ACCOUNT_ID,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||HCP.COLLECTOR_ID as COLLECTOR_ID_KEY,
	HCP.CREDIT_CHECKING as CREDIT_CHECKING,
	HCP.NEXT_CREDIT_REVIEW_DATE as NEXT_CREDIT_REVIEW_DATE,
	HCP.CREDIT_HOLD as CREDIT_HOLD,
	HCP.SITE_USE_ID,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||HCP.SITE_USE_ID as CUST_PROF_SITE_ID_KEY,
	HCP.PROFILE_CLASS_ID,
	HCP.CREDIT_RATING as CREDIT_RATING,
	HCP.STANDARD_TERMS as STANDARD_TERMS,
	HCP.LAST_CREDIT_REVIEW_DATE as LAST_CREDIT_REVIEW_DATE,
	HCP.REVIEW_CYCLE as REVIEW_CYCLE,
	HCP.LATE_CHARGE_CALCULATION_TRX as LATE_CHARGE_CALCULATION_TRX,
	RES.RESOURCE_NAME as CREDIT_ANALYST_NAME,
	HCP.ATTRIBUTE1 as REVOLVE_CREDIT_FLAG,
	HCP.CREDIT_CLASSIFICATION as CREDIT_CLASSIFICATION,
	HCP.PARTY_ID,
	'N' AS IS_DELETED_FLG,

			(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')		
		|| '-' || nvl(HCP.CUST_ACCOUNT_PROFILE_ID, 0)
		|| '-' || nvl(HCP.PARTY_ID, 0)
		|| '-' || nvl(HPA.CUST_ACCT_PROFILE_AMT_ID, 0)
		as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.HZ_CUST_PROFILE_AMTS where is_deleted_flg <> 'Y') HPA
inner join (select * from BEC_ODS.HZ_CUSTOMER_PROFILES where is_deleted_flg <> 'Y') HCP
on
	HPA.cust_account_profile_id = HCP.cust_account_profile_id
left outer join (
	select
		B.RESOURCE_ID RESOURCE_ID,
		T.RESOURCE_NAME RESOURCE_NAME,
		B.END_DATE_ACTIVE END_DATE_ACTIVE
	from
		(select * from BEC_ODS.JTF_RS_RESOURCE_EXTNS where is_deleted_flg <> 'Y') B
	inner join (select * from BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL where is_deleted_flg <> 'Y') T
                on
		B.RESOURCE_ID = T.RESOURCE_ID
		and B.CATEGORY = T.CATEGORY
		and T.LANGUAGE = 'US'
                ) RES
on
	HCP.CREDIT_ANALYST_ID = RES.RESOURCE_ID
where
	1 = 1
	and HCP.STATUS = 'A'
	and HCP.CUST_ACCOUNT_ID <> '-1'	
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_customer_profile'
	and batch_name = 'ar';

commit;