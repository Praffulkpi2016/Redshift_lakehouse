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

drop table if exists bec_ods.CE_BANK_ACCOUNTS;

CREATE TABLE IF NOT EXISTS bec_ods.CE_BANK_ACCOUNTS
(
BANK_ACCOUNT_ID	NUMERIC(15,0)   ENCODE az64
,BANK_ACCOUNT_NAME	VARCHAR(100)   ENCODE lzo
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,BANK_ACCOUNT_NUM	VARCHAR(30)   ENCODE lzo
,BANK_BRANCH_ID	NUMERIC(15,0)   ENCODE az64
,BANK_ID	NUMERIC(15,0)   ENCODE az64
,CURRENCY_CODE	VARCHAR(15)   ENCODE lzo
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,ATTRIBUTE_CATEGORY	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CHECK_DIGITS	VARCHAR(30)   ENCODE lzo
,BANK_ACCOUNT_NAME_ALT	VARCHAR(320)   ENCODE lzo
,ACCOUNT_HOLDER_ID	NUMERIC(15,0)   ENCODE az64
,EFT_REQUESTER_IDENTIFIER	VARCHAR(25)   ENCODE lzo
,SECONDARY_ACCOUNT_REFERENCE	VARCHAR(30)   ENCODE lzo
,ACCOUNT_SUFFIX	VARCHAR(30)   ENCODE lzo
,DESCRIPTION_CODE1	VARCHAR(60)   ENCODE lzo
,DESCRIPTION_CODE2	VARCHAR(60)   ENCODE lzo
,IBAN_NUMBER	VARCHAR(50)   ENCODE lzo
,SHORT_ACCOUNT_NAME	VARCHAR(240)   ENCODE lzo
,ACCOUNT_OWNER_PARTY_ID	NUMERIC(15,0)   ENCODE az64
,ACCOUNT_OWNER_ORG_ID	NUMERIC(15,0)   ENCODE az64
,ACCOUNT_CLASSIFICATION	VARCHAR(30)   ENCODE lzo
,AP_USE_ALLOWED_FLAG	VARCHAR(1)   ENCODE lzo
,AR_USE_ALLOWED_FLAG	VARCHAR(1)   ENCODE lzo
,XTR_USE_ALLOWED_FLAG	VARCHAR(1)   ENCODE lzo
,PAY_USE_ALLOWED_FLAG	VARCHAR(1)   ENCODE lzo
,MULTI_CURRENCY_ALLOWED_FLAG	VARCHAR(1)   ENCODE lzo
,PAYMENT_MULTI_CURRENCY_FLAG	VARCHAR(1)   ENCODE lzo
,RECEIPT_MULTI_CURRENCY_FLAG	VARCHAR(1)   ENCODE lzo
,ZERO_AMOUNT_ALLOWED	VARCHAR(1)   ENCODE lzo
,MAX_OUTLAY	NUMERIC(15,0)   ENCODE az64
,MAX_CHECK_AMOUNT	NUMERIC(28,10)   ENCODE az64
,MIN_CHECK_AMOUNT	NUMERIC(28,10)   ENCODE az64
,AP_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,AR_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,XTR_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,PAY_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,AP_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,AR_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,XTR_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,PAY_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,BANK_ACCOUNT_TYPE	VARCHAR(25)   ENCODE lzo
,AGENCY_LOCATION_CODE	VARCHAR(30)   ENCODE lzo
,START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ACCOUNT_HOLDER_NAME_ALT	VARCHAR(150)   ENCODE lzo
,ACCOUNT_HOLDER_NAME	VARCHAR(240)   ENCODE lzo
,CASHFLOW_DISPLAY_ORDER	NUMERIC(15,0)   ENCODE az64
,POOLED_FLAG	VARCHAR(1)   ENCODE lzo
,MIN_TARGET_BALANCE	NUMERIC(28,10)   ENCODE az64
,MAX_TARGET_BALANCE	NUMERIC(28,10)   ENCODE az64
,EFT_USER_NUM	VARCHAR(30)   ENCODE lzo
,MASKED_ACCOUNT_NUM	VARCHAR(100)   ENCODE lzo
,MASKED_IBAN	VARCHAR(50)   ENCODE lzo
,INTEREST_SCHEDULE_ID	NUMERIC(15,0)   ENCODE az64
,CASHPOOL_MIN_PAYMENT_AMT	NUMERIC(28,10)   ENCODE az64
,CASHPOOL_MIN_RECEIPT_AMT	NUMERIC(28,10)   ENCODE az64
,CASHPOOL_ROUND_FACTOR	NUMERIC(28,10)   ENCODE az64
,CASHPOOL_ROUND_RULE	VARCHAR(4)   ENCODE lzo
,CE_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,CE_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,ASSET_CODE_COMBINATION_ID	NUMERIC(15,0)   ENCODE az64
,CASH_CLEARING_CCID	NUMERIC(15,0)   ENCODE az64
,BANK_CHARGES_CCID	NUMERIC(15,0)   ENCODE az64
,BANK_ERRORS_CCID	NUMERIC(15,0)   ENCODE az64
,OBJECT_VERSION_NUMBER	NUMERIC(15,0)   ENCODE az64
,NETTING_ACCT_FLAG	VARCHAR(1)   ENCODE lzo
,POOL_PAYMENT_METHOD_CODE	VARCHAR(30)   ENCODE lzo
,POOL_BANK_CHARGE_BEARER_CODE	VARCHAR(30)   ENCODE lzo
,POOL_PAYMENT_REASON_CODE	VARCHAR(30)   ENCODE lzo
,POOL_PAYMENT_REASON_COMMENTS	VARCHAR(255)   ENCODE lzo
,POOL_REMITTANCE_MESSAGE1	VARCHAR(255)   ENCODE lzo
,POOL_REMITTANCE_MESSAGE2	VARCHAR(255)   ENCODE lzo
,POOL_REMITTANCE_MESSAGE3	VARCHAR(255)   ENCODE lzo
,FX_CHARGE_CCID	NUMERIC(15,0)   ENCODE az64
,BANK_ACCOUNT_NUM_ELECTRONIC	VARCHAR(100)   ENCODE lzo
,STMT_LINE_FLOAT_HANDLING_FLAG	VARCHAR(1)   ENCODE lzo
,AUTORECON_AP_MATCHING_ORDER	VARCHAR(10)   ENCODE lzo
,AUTORECON_AR_MATCHING_ORDER	VARCHAR(10)   ENCODE lzo
,RECON_FOREIGN_BANK_XRATE_TYPE	VARCHAR(30)   ENCODE lzo
,RECON_FOR_BANK_XRATE_DATE_TYPE	VARCHAR(10)   ENCODE lzo
,RECON_ENABLE_OI_FLAG	VARCHAR(1)   ENCODE lzo
,RECON_OI_FLOAT_STATUS	VARCHAR(30)   ENCODE lzo
,RECON_OI_CLEARED_STATUS	VARCHAR(30)   ENCODE lzo
,RECON_OI_MATCHING_CODE	VARCHAR(10)   ENCODE lzo
,RECON_OI_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,RECON_OI_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,MANUAL_RECON_AMOUNT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,MANUAL_RECON_PERCENT_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,RECON_AP_FOREIGN_DIFF_HANDLING	VARCHAR(30)   ENCODE lzo
,RECON_AR_FOREIGN_DIFF_HANDLING	VARCHAR(30)   ENCODE lzo
,RECON_CE_FOREIGN_DIFF_HANDLING	VARCHAR(30)   ENCODE lzo
,RECON_AP_TOLERANCE_DIFF_ACCT	VARCHAR(30)   ENCODE lzo
,RECON_CE_TOLERANCE_DIFF_ACCT	VARCHAR(30)   ENCODE lzo
,XTR_BANK_ACCOUNT_REFERENCE	VARCHAR(20)   ENCODE lzo
,AUTORECON_AP_MATCHING_ORDER2	VARCHAR(10)   ENCODE lzo
,AUTORECON_AR_MATCHING_ORDER2 VARCHAR(10)   ENCODE lzo
,AUTORECON_AR_MATCHING_ORDER3 VARCHAR(10)   ENCODE lzo
,AUTORECON_AR_MATCHING_ORDER4 VARCHAR(10)   ENCODE lzo
,AUTORECON_AR_MATCHING_ORDER5 VARCHAR(10)   ENCODE lzo
,FED_DO_SYMBOL VARCHAR(5)   ENCODE lzo
,FED_DUNS_NUM VARCHAR(13)   ENCODE lzo
,GAIN_CODE_COMBINATION_ID NUMERIC(15,0)   ENCODE az64
,LOSS_CODE_COMBINATION_ID NUMERIC(15,0)    ENCODE az64
,SEARCH_BY_PSON VARCHAR(1)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.CE_BANK_ACCOUNTS
(
BANK_ACCOUNT_ID
,BANK_ACCOUNT_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,BANK_ACCOUNT_NUM
,BANK_BRANCH_ID
,BANK_ID
,CURRENCY_CODE
,DESCRIPTION
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,CHECK_DIGITS
,BANK_ACCOUNT_NAME_ALT
,ACCOUNT_HOLDER_ID
,EFT_REQUESTER_IDENTIFIER
,SECONDARY_ACCOUNT_REFERENCE
,ACCOUNT_SUFFIX
,DESCRIPTION_CODE1
,DESCRIPTION_CODE2
,IBAN_NUMBER
,SHORT_ACCOUNT_NAME
,ACCOUNT_OWNER_PARTY_ID
,ACCOUNT_OWNER_ORG_ID
,ACCOUNT_CLASSIFICATION
,AP_USE_ALLOWED_FLAG
,AR_USE_ALLOWED_FLAG
,XTR_USE_ALLOWED_FLAG
,PAY_USE_ALLOWED_FLAG
,MULTI_CURRENCY_ALLOWED_FLAG
,PAYMENT_MULTI_CURRENCY_FLAG
,RECEIPT_MULTI_CURRENCY_FLAG
,ZERO_AMOUNT_ALLOWED
,MAX_OUTLAY
,MAX_CHECK_AMOUNT
,MIN_CHECK_AMOUNT
,AP_AMOUNT_TOLERANCE
,AR_AMOUNT_TOLERANCE
,XTR_AMOUNT_TOLERANCE
,PAY_AMOUNT_TOLERANCE
,AP_PERCENT_TOLERANCE
,AR_PERCENT_TOLERANCE
,XTR_PERCENT_TOLERANCE
,PAY_PERCENT_TOLERANCE
,BANK_ACCOUNT_TYPE
,AGENCY_LOCATION_CODE
,START_DATE
,END_DATE
,ACCOUNT_HOLDER_NAME_ALT
,ACCOUNT_HOLDER_NAME
,CASHFLOW_DISPLAY_ORDER
,POOLED_FLAG
,MIN_TARGET_BALANCE
,MAX_TARGET_BALANCE
,EFT_USER_NUM
,MASKED_ACCOUNT_NUM
,MASKED_IBAN
,INTEREST_SCHEDULE_ID
,CASHPOOL_MIN_PAYMENT_AMT
,CASHPOOL_MIN_RECEIPT_AMT
,CASHPOOL_ROUND_FACTOR
,CASHPOOL_ROUND_RULE
,CE_AMOUNT_TOLERANCE
,CE_PERCENT_TOLERANCE
,ASSET_CODE_COMBINATION_ID
,CASH_CLEARING_CCID
,BANK_CHARGES_CCID
,BANK_ERRORS_CCID
,OBJECT_VERSION_NUMBER
,NETTING_ACCT_FLAG
,POOL_PAYMENT_METHOD_CODE
,POOL_BANK_CHARGE_BEARER_CODE
,POOL_PAYMENT_REASON_CODE
,POOL_PAYMENT_REASON_COMMENTS
,POOL_REMITTANCE_MESSAGE1
,POOL_REMITTANCE_MESSAGE2
,POOL_REMITTANCE_MESSAGE3
,FX_CHARGE_CCID
,BANK_ACCOUNT_NUM_ELECTRONIC
,STMT_LINE_FLOAT_HANDLING_FLAG
,AUTORECON_AP_MATCHING_ORDER
,AUTORECON_AR_MATCHING_ORDER
,RECON_FOREIGN_BANK_XRATE_TYPE
,RECON_FOR_BANK_XRATE_DATE_TYPE
,RECON_ENABLE_OI_FLAG
,RECON_OI_FLOAT_STATUS
,RECON_OI_CLEARED_STATUS
,RECON_OI_MATCHING_CODE
,RECON_OI_AMOUNT_TOLERANCE
,RECON_OI_PERCENT_TOLERANCE
,MANUAL_RECON_AMOUNT_TOLERANCE
,MANUAL_RECON_PERCENT_TOLERANCE
,RECON_AP_FOREIGN_DIFF_HANDLING
,RECON_AR_FOREIGN_DIFF_HANDLING
,RECON_CE_FOREIGN_DIFF_HANDLING
,RECON_AP_TOLERANCE_DIFF_ACCT
,RECON_CE_TOLERANCE_DIFF_ACCT
,XTR_BANK_ACCOUNT_REFERENCE
,AUTORECON_AP_MATCHING_ORDER2
,AUTORECON_AR_MATCHING_ORDER2
,AUTORECON_AR_MATCHING_ORDER3
,AUTORECON_AR_MATCHING_ORDER4
,AUTORECON_AR_MATCHING_ORDER5
,FED_DO_SYMBOL
,FED_DUNS_NUM
,GAIN_CODE_COMBINATION_ID
,LOSS_CODE_COMBINATION_ID
,SEARCH_BY_PSON
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date
)
(
select

BANK_ACCOUNT_ID
,BANK_ACCOUNT_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,BANK_ACCOUNT_NUM
,BANK_BRANCH_ID
,BANK_ID
,CURRENCY_CODE
,DESCRIPTION
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,CHECK_DIGITS
,BANK_ACCOUNT_NAME_ALT
,ACCOUNT_HOLDER_ID
,EFT_REQUESTER_IDENTIFIER
,SECONDARY_ACCOUNT_REFERENCE
,ACCOUNT_SUFFIX
,DESCRIPTION_CODE1
,DESCRIPTION_CODE2
,IBAN_NUMBER
,SHORT_ACCOUNT_NAME
,ACCOUNT_OWNER_PARTY_ID
,ACCOUNT_OWNER_ORG_ID
,ACCOUNT_CLASSIFICATION
,AP_USE_ALLOWED_FLAG
,AR_USE_ALLOWED_FLAG
,XTR_USE_ALLOWED_FLAG
,PAY_USE_ALLOWED_FLAG
,MULTI_CURRENCY_ALLOWED_FLAG
,PAYMENT_MULTI_CURRENCY_FLAG
,RECEIPT_MULTI_CURRENCY_FLAG
,ZERO_AMOUNT_ALLOWED
,MAX_OUTLAY
,MAX_CHECK_AMOUNT
,MIN_CHECK_AMOUNT
,AP_AMOUNT_TOLERANCE
,AR_AMOUNT_TOLERANCE
,XTR_AMOUNT_TOLERANCE
,PAY_AMOUNT_TOLERANCE
,AP_PERCENT_TOLERANCE
,AR_PERCENT_TOLERANCE
,XTR_PERCENT_TOLERANCE
,PAY_PERCENT_TOLERANCE
,BANK_ACCOUNT_TYPE
,AGENCY_LOCATION_CODE
,START_DATE
,END_DATE
,ACCOUNT_HOLDER_NAME_ALT
,ACCOUNT_HOLDER_NAME
,CASHFLOW_DISPLAY_ORDER
,POOLED_FLAG
,MIN_TARGET_BALANCE
,MAX_TARGET_BALANCE
,EFT_USER_NUM
,MASKED_ACCOUNT_NUM
,MASKED_IBAN
,INTEREST_SCHEDULE_ID
,CASHPOOL_MIN_PAYMENT_AMT
,CASHPOOL_MIN_RECEIPT_AMT
,CASHPOOL_ROUND_FACTOR
,CASHPOOL_ROUND_RULE
,CE_AMOUNT_TOLERANCE
,CE_PERCENT_TOLERANCE
,ASSET_CODE_COMBINATION_ID
,CASH_CLEARING_CCID
,BANK_CHARGES_CCID
,BANK_ERRORS_CCID
,OBJECT_VERSION_NUMBER
,NETTING_ACCT_FLAG
,POOL_PAYMENT_METHOD_CODE
,POOL_BANK_CHARGE_BEARER_CODE
,POOL_PAYMENT_REASON_CODE
,POOL_PAYMENT_REASON_COMMENTS
,POOL_REMITTANCE_MESSAGE1
,POOL_REMITTANCE_MESSAGE2
,POOL_REMITTANCE_MESSAGE3
,FX_CHARGE_CCID
,BANK_ACCOUNT_NUM_ELECTRONIC
,STMT_LINE_FLOAT_HANDLING_FLAG
,AUTORECON_AP_MATCHING_ORDER
,AUTORECON_AR_MATCHING_ORDER
,RECON_FOREIGN_BANK_XRATE_TYPE
,RECON_FOR_BANK_XRATE_DATE_TYPE
,RECON_ENABLE_OI_FLAG
,RECON_OI_FLOAT_STATUS
,RECON_OI_CLEARED_STATUS
,RECON_OI_MATCHING_CODE
,RECON_OI_AMOUNT_TOLERANCE
,RECON_OI_PERCENT_TOLERANCE
,MANUAL_RECON_AMOUNT_TOLERANCE
,MANUAL_RECON_PERCENT_TOLERANCE
,RECON_AP_FOREIGN_DIFF_HANDLING
,RECON_AR_FOREIGN_DIFF_HANDLING
,RECON_CE_FOREIGN_DIFF_HANDLING
,RECON_AP_TOLERANCE_DIFF_ACCT
,RECON_CE_TOLERANCE_DIFF_ACCT
,XTR_BANK_ACCOUNT_REFERENCE
,AUTORECON_AP_MATCHING_ORDER2
,AUTORECON_AR_MATCHING_ORDER2
,AUTORECON_AR_MATCHING_ORDER3
,AUTORECON_AR_MATCHING_ORDER4
,AUTORECON_AR_MATCHING_ORDER5
,FED_DO_SYMBOL
,FED_DUNS_NUM
,GAIN_CODE_COMBINATION_ID
,LOSS_CODE_COMBINATION_ID
,SEARCH_BY_PSON
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from 
bec_ods_stg.CE_BANK_ACCOUNTS);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='ce_bank_accounts'; 

commit;