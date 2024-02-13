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
	table bec_ods_stg.CE_BANK_ACCT_USES_ALL;

insert into bec_ods_stg.CE_BANK_ACCT_USES_ALL
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
	,kca_seq_id
	,kca_seq_date)
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
	,kca_seq_id
	,kca_seq_date
FROM
	bec_raw_dl_ext.CE_BANK_ACCT_USES_ALL
where  kca_operation != 'DELETE' and (BANK_ACCT_USE_ID,kca_seq_id) in (select BANK_ACCT_USE_ID,max(kca_seq_id) from bec_raw_dl_ext.CE_BANK_ACCT_USES_ALL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by BANK_ACCT_USE_ID) and
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ce_bank_acct_uses_all')
			;
end;