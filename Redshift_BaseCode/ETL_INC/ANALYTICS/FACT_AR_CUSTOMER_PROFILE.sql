/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/

begin;
-- Delete Records

delete
from
	bec_dwh.FACT_AR_CUSTOMER_PROFILE
where
	(nvl(CUST_ACCOUNT_PROFILE_ID,0),
	nvl(PARTY_ID,0),
	nvl(CUST_ACCT_PROFILE_AMT_ID,0)) in (
	select
		nvl(ods.CUST_ACCOUNT_PROFILE_ID,0) as CUST_ACCOUNT_PROFILE_ID,
		nvl(ods.PARTY_ID,0) as PARTY_ID,
		nvl(ods.CUST_ACCT_PROFILE_AMT_ID,0) as CUST_ACCT_PROFILE_AMT_ID
	from
		bec_dwh.FACT_AR_CUSTOMER_PROFILE dw,
		(
		select
		HCP.CUST_ACCOUNT_PROFILE_ID
		,HCP.PARTY_ID  
		,HPA.CUST_ACCT_PROFILE_AMT_ID
FROM	bec_ods.HZ_CUST_PROFILE_AMTS HPA
inner join BEC_ODS.HZ_CUSTOMER_PROFILES HCP
on
	HPA.cust_account_profile_id = HCP.cust_account_profile_id
left outer join (
	select
		B.RESOURCE_ID RESOURCE_ID,
		T.RESOURCE_NAME RESOURCE_NAME,
		B.END_DATE_ACTIVE END_DATE_ACTIVE,
		B.is_deleted_flg,
		T.is_deleted_flg is_deleted_flg1
	from
		BEC_ODS.JTF_RS_RESOURCE_EXTNS B
	inner join BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL T
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
			and (HPA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_customer_profile'
				and batch_name = 'ar')
			or HPA.is_deleted_flg = 'Y'
			or HCP.is_deleted_flg = 'Y'
			or RES.is_deleted_flg = 'Y'
			or RES.is_deleted_flg1 = 'Y')
			) ods
	where
		dw.dw_load_id = 	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')		
		|| '-' || nvl(ODS.CUST_ACCOUNT_PROFILE_ID, 0)
		|| '-' || nvl(ODS.PARTY_ID, 0) 
		|| '-' || nvl(ODS.CUST_ACCT_PROFILE_AMT_ID, 0)
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AR_CUSTOMER_PROFILE 
(
cust_acct_profile_amt_id
,cust_account_profile_id
,currency_code
,trx_credit_limit
,overall_credit_limit
,min_dunning_amount
,min_dunning_invoice_amount
,max_interest_charge
,min_statement_amount
,interest_rate
,attribute_category
,min_fc_balance_amount
,min_fc_invoice_amount
,cust_account_id_key
,cust_account_id
,customer_site_use_id
,customer_site_use_id_key
,expiration_date
,exchange_rate_type
,interest_type
,interest_fixed_amount
,interest_schedule_id
,penalty_type
,penalty_rate
,min_interest_charge
,penalty_fixed_amount
,last_update_date
,cust_prof_account_id
,collector_id_key
,credit_checking
,next_credit_review_date
,credit_hold
,site_use_id
,cust_prof_site_id_key
,profile_class_id
,credit_rating
,standard_terms
,last_credit_review_date
,review_cycle
,late_charge_calculation_trx
,credit_analyst_name
,revolve_credit_flag
,credit_classification
,party_id
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
	)
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
	'N' as is_deleted_flg,
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
		and (HPA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_customer_profile'
				and batch_name = 'ar'))
);

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_ar_customer_profile'
	and batch_name = 'ar';

COMMIT;