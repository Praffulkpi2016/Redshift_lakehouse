/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/


begin;

DROP TABLE if exists bec_ods.BEC_CUSTOMER_DETAILS_VIEW;

CREATE TABLE IF NOT EXISTS bec_ods.BEC_CUSTOMER_DETAILS_VIEW
(
	PARTY_NAME VARCHAR(360)   ENCODE lzo
	,PARTY_NUMBER VARCHAR(30)   ENCODE lzo
	,PARTY_SITE_NUMBER VARCHAR(30)   ENCODE lzo
	,PARTY_SITE_ID NUMERIC(15,0)  ENCODE az64
	,ADDRESS1 VARCHAR(240)   ENCODE lzo
	,ADDRESS2 VARCHAR(240)   ENCODE lzo
	,ADDRESS3 VARCHAR(240)   ENCODE lzo
	,ADDRESS4 VARCHAR(240)   ENCODE lzo
	,ADDRESS5 VARCHAR(240)   ENCODE lzo
	,COUNTY VARCHAR(60)   ENCODE lzo
	,CITY VARCHAR(60)   ENCODE lzo
	,COUNTRY VARCHAR(60)   ENCODE lzo
	,STATE VARCHAR(60)   ENCODE lzo
	,POSTAL_CODE VARCHAR(60)   ENCODE lzo
	,CUST_ACCOUNT_ID NUMERIC(15,0)  ENCODE az64
	,SITE_USE_ID NUMERIC(15,0)  ENCODE az64
	,ACCOUNT_NUMBER NUMERIC(15,0)  ENCODE az64
	,LOCATION VARCHAR(40)   ENCODE lzo
	,SITE_USE_CODE VARCHAR(30)   ENCODE lzo
	,ORIG_SYSTEM_REFERENCE VARCHAR(240)   ENCODE lzo
	,CUST_ACCT_SITE_ID NUMERIC(15,0)  ENCODE az64	
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

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
	is_deleted_flg,
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