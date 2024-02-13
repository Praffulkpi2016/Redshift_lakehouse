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

DROP TABLE if exists bec_ods.IBY_EXT_BANK_ACCOUNTS;

CREATE TABLE IF NOT EXISTS bec_ods.IBY_EXT_BANK_ACCOUNTS
(
    ext_bank_account_id NUMERIC(15,0)   ENCODE az64
	,country_code VARCHAR(2)   ENCODE lzo
	,branch_id NUMERIC(15,0)   ENCODE az64
	,bank_id NUMERIC(15,0)   ENCODE az64
	,bank_account_num VARCHAR(100)   ENCODE lzo
	,masked_bank_account_num VARCHAR(100)   ENCODE lzo
	,ba_mask_setting VARCHAR(30)   ENCODE lzo
	,ba_unmask_length NUMERIC(2)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,iban VARCHAR(50)   ENCODE lzo
	,iban_hash1 VARCHAR(64)   ENCODE lzo
	,iban_hash2 VARCHAR(64)   ENCODE lzo
	,masked_iban VARCHAR(50)   ENCODE lzo
	,check_digits VARCHAR(30)   ENCODE lzo
	,bank_account_type VARCHAR(25)   ENCODE lzo
	,account_classification VARCHAR(30)   ENCODE lzo
	,account_suffix VARCHAR(30)   ENCODE lzo
	,agency_location_code VARCHAR(30)   ENCODE lzo
	,payment_factor_flag VARCHAR(1)   ENCODE lzo
	,foreign_payment_use_flag VARCHAR(1)   ENCODE lzo
	,exchange_rate_agreement_num VARCHAR(80)   ENCODE lzo
	,exchange_rate_agreement_type VARCHAR(80)   ENCODE lzo
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,hedging_contract_reference VARCHAR(20)   ENCODE lzo
	,secondary_account_reference VARCHAR(30)   ENCODE lzo
	,ba_num_sec_segment_id NUMERIC(15,0)   ENCODE az64
	,"encrypted" VARCHAR(1)   ENCODE lzo
	,iban_sec_segment_id NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,object_version_number NUMERIC(28,10)   ENCODE az64
	,bank_account_name VARCHAR(80)   ENCODE lzo
	,bank_account_name_alt VARCHAR(320)   ENCODE lzo
	,short_acct_name VARCHAR(30)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,bank_account_num_electronic VARCHAR(100)   ENCODE lzo
	,ba_num_elec_sec_segment_id NUMERIC(15,0)   ENCODE az64
	,salt_version NUMERIC(2)   ENCODE az64
	,contact_name VARCHAR(60)   ENCODE lzo
	,contact_phone VARCHAR(30)   ENCODE lzo
	,contact_fax VARCHAR(30)   ENCODE lzo
	,contact_email VARCHAR(80)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.IBY_EXT_BANK_ACCOUNTS (
 EXT_BANK_ACCOUNT_ID
,      COUNTRY_CODE
,      BRANCH_ID
,      BANK_ID
,      BANK_ACCOUNT_NUM
--,      BANK_ACCOUNT_NUM_HASH1
--,      BANK_ACCOUNT_NUM_HASH2
,      MASKED_BANK_ACCOUNT_NUM
,      BA_MASK_SETTING
,      BA_UNMASK_LENGTH
,      CURRENCY_CODE
,      IBAN
,      IBAN_HASH1
,      IBAN_HASH2
,      MASKED_IBAN
,      CHECK_DIGITS
,      BANK_ACCOUNT_TYPE
,      ACCOUNT_CLASSIFICATION
,      ACCOUNT_SUFFIX
,      AGENCY_LOCATION_CODE
,      PAYMENT_FACTOR_FLAG
,      FOREIGN_PAYMENT_USE_FLAG
,      EXCHANGE_RATE_AGREEMENT_NUM
,      EXCHANGE_RATE_AGREEMENT_TYPE
,      EXCHANGE_RATE
,      HEDGING_CONTRACT_REFERENCE
,      SECONDARY_ACCOUNT_REFERENCE
,      BA_NUM_SEC_SEGMENT_ID
,      ENCRYPTED
,      IBAN_SEC_SEGMENT_ID
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      START_DATE
,      END_DATE
,      CREATED_BY
,      CREATION_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      LAST_UPDATE_LOGIN
,      OBJECT_VERSION_NUMBER
,      BANK_ACCOUNT_NAME
,      BANK_ACCOUNT_NAME_ALT
,      SHORT_ACCT_NAME
,      DESCRIPTION
,      BANK_ACCOUNT_NUM_ELECTRONIC
,      BA_NUM_ELEC_SEC_SEGMENT_ID
,      SALT_VERSION
,      CONTACT_NAME
,      CONTACT_PHONE
,      CONTACT_FAX
,      CONTACT_EMAIL
,kca_operation
,	IS_DELETED_FLG
,	kca_seq_id
,	kca_seq_date
)
    SELECT
  EXT_BANK_ACCOUNT_ID
,      COUNTRY_CODE
,      BRANCH_ID
,      BANK_ID
,      BANK_ACCOUNT_NUM
--,      BANK_ACCOUNT_NUM_HASH1
--,      BANK_ACCOUNT_NUM_HASH2
,      MASKED_BANK_ACCOUNT_NUM
,      BA_MASK_SETTING
,      BA_UNMASK_LENGTH
,      CURRENCY_CODE
,      IBAN
,      IBAN_HASH1
,      IBAN_HASH2
,      MASKED_IBAN
,      CHECK_DIGITS
,      BANK_ACCOUNT_TYPE
,      ACCOUNT_CLASSIFICATION
,      ACCOUNT_SUFFIX
,      AGENCY_LOCATION_CODE
,      PAYMENT_FACTOR_FLAG
,      FOREIGN_PAYMENT_USE_FLAG
,      EXCHANGE_RATE_AGREEMENT_NUM
,      EXCHANGE_RATE_AGREEMENT_TYPE
,      EXCHANGE_RATE
,      HEDGING_CONTRACT_REFERENCE
,      SECONDARY_ACCOUNT_REFERENCE
,      BA_NUM_SEC_SEGMENT_ID
,      ENCRYPTED
,      IBAN_SEC_SEGMENT_ID
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      START_DATE
,      END_DATE
,      CREATED_BY
,      CREATION_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      LAST_UPDATE_LOGIN
,      OBJECT_VERSION_NUMBER
,      BANK_ACCOUNT_NAME
,      BANK_ACCOUNT_NAME_ALT
,      SHORT_ACCT_NAME
,      DESCRIPTION
,      BANK_ACCOUNT_NUM_ELECTRONIC
,      BA_NUM_ELEC_SEC_SEGMENT_ID
,      SALT_VERSION
,      CONTACT_NAME
,      CONTACT_PHONE
,      CONTACT_FAX
,      CONTACT_EMAIL
   , KCA_OPERATION
    ,'N' as IS_DELETED_FLG
    ,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.IBY_EXT_BANK_ACCOUNTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'iby_ext_bank_accounts';
	
COMMIT;