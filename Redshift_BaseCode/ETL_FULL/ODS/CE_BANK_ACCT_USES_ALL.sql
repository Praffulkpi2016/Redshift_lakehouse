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
drop table if exists bec_ods.CE_BANK_ACCT_USES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.CE_BANK_ACCT_USES_ALL
(BANK_ACCT_USE_ID NUMERIC(15,0)   ENCODE az64 , 
BANK_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64 , 
LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64 , 
CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
CREATED_BY NUMERIC(15,0)   ENCODE az64 , 
LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64, 
PRIMARY_FLAG VARCHAR(1)   ENCODE lzo, 
ATTRIBUTE_CATEGORY VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE1 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE2 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE3 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE4 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE5 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE6 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE7 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE8 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE9 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE10 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE11 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE12 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE13 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE14 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE15 VARCHAR(150)   ENCODE lzo, 
REQUEST_ID NUMERIC(15,0)   ENCODE az64, 
PROGRAM_APPLICATION_ID NUMERIC(15,0)   ENCODE az64, 
PROGRAM_ID NUMERIC(15,0)   ENCODE az64, 
PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
ORG_ID NUMERIC(15,0)   ENCODE az64 , 
ORG_PARTY_ID NUMERIC(15,0)   ENCODE az64, 
AP_USE_ENABLE_FLAG VARCHAR(1)   ENCODE lzo, 
AR_USE_ENABLE_FLAG VARCHAR(1)   ENCODE lzo, 
XTR_USE_ENABLE_FLAG VARCHAR(1)   ENCODE lzo, 
PAY_USE_ENABLE_FLAG VARCHAR(1)   ENCODE lzo, 
EDISC_RECEIVABLES_TRX_ID NUMERIC(15,0)   ENCODE az64, 
UNEDISC_RECEIVABLES_TRX_ID NUMERIC(15,0)   ENCODE az64, 
POOLED_FLAG VARCHAR(1)   ENCODE lzo, 
END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
BR_STD_RECEIVABLES_TRX_ID NUMERIC(15,0)   ENCODE az64, 
PAYMENT_DOC_CATEGORY VARCHAR(30)   ENCODE lzo, 
LEGAL_ENTITY_ID NUMERIC(15,0)   ENCODE az64, 
INVESTMENT_LIMIT_CODE VARCHAR(7)   ENCODE lzo, 
FUNDING_LIMIT_CODE VARCHAR(7)   ENCODE lzo, 
AP_DEFAULT_SETTLEMENT_FLAG VARCHAR(1)   ENCODE lzo, 
XTR_DEFAULT_SETTLEMENT_FLAG VARCHAR(1)   ENCODE lzo, 
PAYROLL_BANK_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64, 
PRICING_MODEL VARCHAR(30)   ENCODE lzo, 
AUTHORIZED_FLAG VARCHAR(1)   ENCODE lzo , 
EFT_SCRIPT_NAME VARCHAR(50)   ENCODE lzo, 
PORTFOLIO_CODE VARCHAR(7)   ENCODE lzo, 
DEFAULT_ACCOUNT_FLAG VARCHAR(1)   ENCODE lzo , 
OBJECT_VERSION_NUMBER NUMERIC(15,0)   ENCODE az64 , 
AR_CLAIM_INV_ACT_ID NUMERIC(15,0)   ENCODE az64, 
NEW_AR_RCPTS_RECEIVABLE_TRX_ID NUMERIC(15,0)   ENCODE az64
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;
insert into bec_ods.CE_BANK_ACCT_USES_ALL
(BANK_ACCT_USE_ID,
	BANK_ACCOUNT_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	PRIMARY_FLAG,
	ATTRIBUTE_CATEGORY,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	ORG_ID,
	ORG_PARTY_ID,
	AP_USE_ENABLE_FLAG,
	AR_USE_ENABLE_FLAG,
	XTR_USE_ENABLE_FLAG,
	PAY_USE_ENABLE_FLAG,
	EDISC_RECEIVABLES_TRX_ID,
	UNEDISC_RECEIVABLES_TRX_ID,
	POOLED_FLAG,
	END_DATE,
	BR_STD_RECEIVABLES_TRX_ID,
	PAYMENT_DOC_CATEGORY,
	LEGAL_ENTITY_ID,
	INVESTMENT_LIMIT_CODE,
	FUNDING_LIMIT_CODE,
	AP_DEFAULT_SETTLEMENT_FLAG,
	XTR_DEFAULT_SETTLEMENT_FLAG,
	PAYROLL_BANK_ACCOUNT_ID,
	PRICING_MODEL,
	AUTHORIZED_FLAG,
	EFT_SCRIPT_NAME,
	PORTFOLIO_CODE,
	DEFAULT_ACCOUNT_FLAG,
	OBJECT_VERSION_NUMBER,
	AR_CLAIM_INV_ACT_ID,
	NEW_AR_RCPTS_RECEIVABLE_TRX_ID
	,KCA_OPERATION
	,IS_DELETED_FLG
	,kca_seq_id,
	kca_seq_date)
SELECT
	BANK_ACCT_USE_ID,
	BANK_ACCOUNT_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	PRIMARY_FLAG,
	ATTRIBUTE_CATEGORY,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	ORG_ID,
	ORG_PARTY_ID,
	AP_USE_ENABLE_FLAG,
	AR_USE_ENABLE_FLAG,
	XTR_USE_ENABLE_FLAG,
	PAY_USE_ENABLE_FLAG,
	EDISC_RECEIVABLES_TRX_ID,
	UNEDISC_RECEIVABLES_TRX_ID,
	POOLED_FLAG,
	END_DATE,
	BR_STD_RECEIVABLES_TRX_ID,
	PAYMENT_DOC_CATEGORY,
	LEGAL_ENTITY_ID,
	INVESTMENT_LIMIT_CODE,
	FUNDING_LIMIT_CODE,
	AP_DEFAULT_SETTLEMENT_FLAG,
	XTR_DEFAULT_SETTLEMENT_FLAG,
	PAYROLL_BANK_ACCOUNT_ID,
	PRICING_MODEL,
	AUTHORIZED_FLAG,
	EFT_SCRIPT_NAME,
	PORTFOLIO_CODE,
	DEFAULT_ACCOUNT_FLAG,
	OBJECT_VERSION_NUMBER,
	AR_CLAIM_INV_ACT_ID,
	NEW_AR_RCPTS_RECEIVABLE_TRX_ID
	,KCA_OPERATION
	,'N' as IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
FROM
	bec_ods_stg.CE_BANK_ACCT_USES_ALL;
end;	
 
update bec_etl_ctrl.batch_ods_info set load_type = 'I', last_refresh_date = getdate() where ods_table_name='ce_bank_acct_uses_all'; 

commit;