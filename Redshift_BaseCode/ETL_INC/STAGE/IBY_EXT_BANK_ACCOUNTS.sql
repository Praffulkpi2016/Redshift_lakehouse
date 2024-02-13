/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.IBY_EXT_BANK_ACCOUNTS;

insert
	into
	bec_ods_stg.IBY_EXT_BANK_ACCOUNTS
    (
	EXT_BANK_ACCOUNT_ID
,      COUNTRY_CODE
,      BRANCH_ID
,      BANK_ID
,      BANK_ACCOUNT_NUM
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
	,KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date)
(
	select
	EXT_BANK_ACCOUNT_ID
,      COUNTRY_CODE
,      BRANCH_ID
,      BANK_ID
,      BANK_ACCOUNT_NUM
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
		,KCA_OPERATION,
		KCA_SEQ_ID,
	kca_seq_date
	from
		bec_raw_dl_ext.IBY_EXT_BANK_ACCOUNTS
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and ( nvl(EXT_BANK_ACCOUNT_ID, 0) ,
		KCA_SEQ_ID) in 
	(
		select
			nvl(EXT_BANK_ACCOUNT_ID, 0) as EXT_BANK_ACCOUNT_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.IBY_EXT_BANK_ACCOUNTS
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(EXT_BANK_ACCOUNT_ID, 0) 
	)
		and  kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'iby_ext_bank_accounts'));
end;