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

truncate table bec_ods_stg.XLA_AE_HEADERS;

insert into	bec_ods_stg.XLA_AE_HEADERS
   (
   AE_HEADER_ID
,APPLICATION_ID
,LEDGER_ID
,ENTITY_ID
,EVENT_ID
,EVENT_TYPE_CODE
,ACCOUNTING_DATE
,GL_TRANSFER_STATUS_CODE
,GL_TRANSFER_DATE
,JE_CATEGORY_NAME
,ACCOUNTING_ENTRY_STATUS_CODE
,ACCOUNTING_ENTRY_TYPE_CODE
,AMB_CONTEXT_CODE
,PRODUCT_RULE_TYPE_CODE
,PRODUCT_RULE_CODE
,PRODUCT_RULE_VERSION
,DESCRIPTION
,DOC_SEQUENCE_ID
,DOC_SEQUENCE_VALUE
,ACCOUNTING_BATCH_ID
,COMPLETION_ACCT_SEQ_VERSION_ID
,CLOSE_ACCT_SEQ_VERSION_ID
,COMPLETION_ACCT_SEQ_VALUE
,CLOSE_ACCT_SEQ_VALUE
,BUDGET_VERSION_ID
,FUNDS_STATUS_CODE
,ENCUMBRANCE_TYPE_ID
,BALANCE_TYPE_CODE
,REFERENCE_DATE
,COMPLETED_DATE
,PERIOD_NAME
,PACKET_ID
,COMPLETION_ACCT_SEQ_ASSIGN_ID
,CLOSE_ACCT_SEQ_ASSIGN_ID
,DOC_CATEGORY_CODE
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
,GROUP_ID
,DOC_SEQUENCE_VERSION_ID
,DOC_SEQUENCE_ASSIGN_ID
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
,UPG_SOURCE_APPLICATION_ID
,UPG_VALID_FLAG
,ZERO_AMOUNT_FLAG
,PARENT_AE_HEADER_ID
,PARENT_AE_LINE_NUM
,ACCRUAL_REVERSAL_FLAG
,MERGE_EVENT_ID
,NEED_BAL_FLAG
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	)
(
	select
AE_HEADER_ID
,APPLICATION_ID
,LEDGER_ID
,ENTITY_ID
,EVENT_ID
,EVENT_TYPE_CODE
,ACCOUNTING_DATE
,GL_TRANSFER_STATUS_CODE
,GL_TRANSFER_DATE
,JE_CATEGORY_NAME
,ACCOUNTING_ENTRY_STATUS_CODE
,ACCOUNTING_ENTRY_TYPE_CODE
,AMB_CONTEXT_CODE
,PRODUCT_RULE_TYPE_CODE
,PRODUCT_RULE_CODE
,PRODUCT_RULE_VERSION
,DESCRIPTION
,DOC_SEQUENCE_ID
,DOC_SEQUENCE_VALUE
,ACCOUNTING_BATCH_ID
,COMPLETION_ACCT_SEQ_VERSION_ID
,CLOSE_ACCT_SEQ_VERSION_ID
,COMPLETION_ACCT_SEQ_VALUE
,CLOSE_ACCT_SEQ_VALUE
,BUDGET_VERSION_ID
,FUNDS_STATUS_CODE
,ENCUMBRANCE_TYPE_ID
,BALANCE_TYPE_CODE
,REFERENCE_DATE
,COMPLETED_DATE
,PERIOD_NAME
,PACKET_ID
,COMPLETION_ACCT_SEQ_ASSIGN_ID
,CLOSE_ACCT_SEQ_ASSIGN_ID
,DOC_CATEGORY_CODE
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
,GROUP_ID
,DOC_SEQUENCE_VERSION_ID
,DOC_SEQUENCE_ASSIGN_ID
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
,UPG_SOURCE_APPLICATION_ID
,UPG_VALID_FLAG
,ZERO_AMOUNT_FLAG
,PARENT_AE_HEADER_ID
,PARENT_AE_LINE_NUM
,ACCRUAL_REVERSAL_FLAG
,MERGE_EVENT_ID
,NEED_BAL_FLAG
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	from bec_raw_dl_ext.XLA_AE_HEADERS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (AE_HEADER_ID,kca_seq_id) in 
	(select AE_HEADER_ID,max(kca_seq_id) from bec_raw_dl_ext.XLA_AE_HEADERS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by AE_HEADER_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'xla_ae_headers')

            )
);
end;

