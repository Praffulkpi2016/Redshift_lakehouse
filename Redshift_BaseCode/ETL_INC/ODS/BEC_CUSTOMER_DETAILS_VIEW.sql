/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

TRUNCATE TABLE bec_ods.BEC_CUSTOMER_DETAILS_VIEW;

INSERT INTO bec_ods.BEC_CUSTOMER_DETAILS_VIEW (
	PARTY_NAME, 
	PARTY_NUMBER, 
	PARTY_SITE_NUMBER, 
	PARTY_SITE_ID, 
	ADDRESS1, 
	ADDRESS2, 
	ADDRESS3, 
	ADDRESS4, 
	ADDRESS5, 
	COUNTY, 
	CITY, 
	COUNTRY, 
	STATE, 
	POSTAL_CODE, 
	CUST_ACCOUNT_ID, 
	SITE_USE_ID, 
	ACCOUNT_NUMBER, 
	LOCATION, 
	SITE_USE_CODE, 
	ORIG_SYSTEM_REFERENCE, 
	CUST_ACCT_SITE_ID,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	PARTY_NAME, 
	PARTY_NUMBER, 
	PARTY_SITE_NUMBER, 
	PARTY_SITE_ID, 
	ADDRESS1, 
	ADDRESS2, 
	ADDRESS3, 
	ADDRESS4, 
	ADDRESS5, 
	COUNTY, 
	CITY, 
	COUNTRY, 
	STATE, 
	POSTAL_CODE, 
	CUST_ACCOUNT_ID, 
	SITE_USE_ID, 
	ACCOUNT_NUMBER, 
	LOCATION, 
	SITE_USE_CODE, 
	ORIG_SYSTEM_REFERENCE, 
	CUST_ACCT_SITE_ID,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.BEC_CUSTOMER_DETAILS_VIEW;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bec_customer_details_view';
	
commit;