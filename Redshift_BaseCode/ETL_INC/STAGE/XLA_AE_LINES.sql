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

truncate table bec_ods_stg.XLA_AE_LINES;

insert into	bec_ods_stg.XLA_AE_LINES
   (
   AE_HEADER_ID
,AE_LINE_NUM
,APPLICATION_ID
,CODE_COMBINATION_ID
,GL_TRANSFER_MODE_CODE
,GL_SL_LINK_ID
,ACCOUNTING_CLASS_CODE
,PARTY_ID
,PARTY_SITE_ID
,PARTY_TYPE_CODE
,ENTERED_DR
,ENTERED_CR
,ACCOUNTED_DR
,ACCOUNTED_CR
,DESCRIPTION
,STATISTICAL_AMOUNT
,CURRENCY_CODE
,CURRENCY_CONVERSION_DATE
,CURRENCY_CONVERSION_RATE
,CURRENCY_CONVERSION_TYPE
,USSGL_TRANSACTION_CODE
,JGZZ_RECON_REF
,CONTROL_BALANCE_FLAG
,ANALYTICAL_BALANCE_FLAG
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
,GL_SL_LINK_TABLE
,DISPLAYED_LINE_NUMBER
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,PROGRAM_UPDATE_DATE
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,REQUEST_ID
,UPG_BATCH_ID
,UPG_TAX_REFERENCE_ID1
,UPG_TAX_REFERENCE_ID2
,UPG_TAX_REFERENCE_ID3
,UNROUNDED_ACCOUNTED_DR
,UNROUNDED_ACCOUNTED_CR
,GAIN_OR_LOSS_FLAG
,UNROUNDED_ENTERED_DR
,UNROUNDED_ENTERED_CR
,SUBSTITUTED_CCID
,BUSINESS_CLASS_CODE
,MPA_ACCRUAL_ENTRY_FLAG
,ENCUMBRANCE_TYPE_ID
,FUNDS_STATUS_CODE
,MERGE_CODE_COMBINATION_ID
,MERGE_PARTY_ID
,MERGE_PARTY_SITE_ID
,ACCOUNTING_DATE
,LEDGER_ID
,SOURCE_TABLE
,SOURCE_ID
,ACCOUNT_OVERLAY_SOURCE_ID
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	)
(
	select
AE_HEADER_ID
,AE_LINE_NUM
,APPLICATION_ID
,CODE_COMBINATION_ID
,GL_TRANSFER_MODE_CODE
,GL_SL_LINK_ID
,ACCOUNTING_CLASS_CODE
,PARTY_ID
,PARTY_SITE_ID
,PARTY_TYPE_CODE
,ENTERED_DR
,ENTERED_CR
,ACCOUNTED_DR
,ACCOUNTED_CR
,DESCRIPTION
,STATISTICAL_AMOUNT
,CURRENCY_CODE
,CURRENCY_CONVERSION_DATE
,CURRENCY_CONVERSION_RATE
,CURRENCY_CONVERSION_TYPE
,USSGL_TRANSACTION_CODE
,JGZZ_RECON_REF
,CONTROL_BALANCE_FLAG
,ANALYTICAL_BALANCE_FLAG
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
,GL_SL_LINK_TABLE
,DISPLAYED_LINE_NUMBER
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,PROGRAM_UPDATE_DATE
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,REQUEST_ID
,UPG_BATCH_ID
,UPG_TAX_REFERENCE_ID1
,UPG_TAX_REFERENCE_ID2
,UPG_TAX_REFERENCE_ID3
,UNROUNDED_ACCOUNTED_DR
,UNROUNDED_ACCOUNTED_CR
,GAIN_OR_LOSS_FLAG
,UNROUNDED_ENTERED_DR
,UNROUNDED_ENTERED_CR
,SUBSTITUTED_CCID
,BUSINESS_CLASS_CODE
,MPA_ACCRUAL_ENTRY_FLAG
,ENCUMBRANCE_TYPE_ID
,FUNDS_STATUS_CODE
,MERGE_CODE_COMBINATION_ID
,MERGE_PARTY_ID
,MERGE_PARTY_SITE_ID
,ACCOUNTING_DATE
,LEDGER_ID
,SOURCE_TABLE
,SOURCE_ID
,ACCOUNT_OVERLAY_SOURCE_ID
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	from bec_raw_dl_ext.XLA_AE_LINES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (AE_HEADER_ID, AE_LINE_NUM,kca_seq_id) in 
	(select AE_HEADER_ID, AE_LINE_NUM,max(kca_seq_id) from bec_raw_dl_ext.XLA_AE_LINES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by AE_HEADER_ID, AE_LINE_NUM)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'xla_ae_lines')

            )
);
end;
