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

truncate table bec_ods_stg.WIP_TRANSACTION_ACCOUNTS;

insert into	bec_ods_stg.WIP_TRANSACTION_ACCOUNTS
   (	
	TRANSACTION_ID, 
	REFERENCE_ACCOUNT, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	ORGANIZATION_ID, 
	TRANSACTION_DATE, 
	WIP_ENTITY_ID, 
	REPETITIVE_SCHEDULE_ID, 
	ACCOUNTING_LINE_TYPE, 
	TRANSACTION_VALUE, 
	BASE_TRANSACTION_VALUE, 
	CONTRA_SET_ID, 
	PRIMARY_QUANTITY, 
	RATE_OR_AMOUNT, 
	BASIS_TYPE, 
	RESOURCE_ID, 
	COST_ELEMENT_ID, 
	ACTIVITY_ID, 
	CURRENCY_CODE, 
	CURRENCY_CONVERSION_DATE, 
	CURRENCY_CONVERSION_TYPE, 
	CURRENCY_CONVERSION_RATE, 
	OVERHEAD_BASIS_FACTOR, 
	BASIS_RESOURCE_ID, 
	GL_BATCH_ID, 
	REQUEST_ID, 
	PROGRAM_APPLICATION_ID, 
	PROGRAM_ID, 
	PROGRAM_UPDATE_DATE, 
	GL_SL_LINK_ID, 
	WIP_SUB_LEDGER_ID, 
	ENCUMBRANCE_TYPE_ID,
    KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		TRANSACTION_ID, 
		REFERENCE_ACCOUNT, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		ORGANIZATION_ID, 
		TRANSACTION_DATE, 
		WIP_ENTITY_ID, 
		REPETITIVE_SCHEDULE_ID, 
		ACCOUNTING_LINE_TYPE, 
		TRANSACTION_VALUE, 
		BASE_TRANSACTION_VALUE, 
		CONTRA_SET_ID, 
		PRIMARY_QUANTITY, 
		RATE_OR_AMOUNT, 
		BASIS_TYPE, 
		RESOURCE_ID, 
		COST_ELEMENT_ID, 
		ACTIVITY_ID, 
		CURRENCY_CODE, 
		CURRENCY_CONVERSION_DATE, 
		CURRENCY_CONVERSION_TYPE, 
		CURRENCY_CONVERSION_RATE, 
		OVERHEAD_BASIS_FACTOR, 
		BASIS_RESOURCE_ID, 
		GL_BATCH_ID, 
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		GL_SL_LINK_ID, 
		WIP_SUB_LEDGER_ID, 
		ENCUMBRANCE_TYPE_ID,
        KCA_OPERATION,
		kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.WIP_TRANSACTION_ACCOUNTS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(TRANSACTION_ID,0),nvl(REFERENCE_ACCOUNT,0),nvl(WIP_SUB_LEDGER_ID,0),kca_seq_id) in 
	(select nvl(TRANSACTION_ID,0),nvl(REFERENCE_ACCOUNT,0),nvl(WIP_SUB_LEDGER_ID,0),max(kca_seq_id) from bec_raw_dl_ext.WIP_TRANSACTION_ACCOUNTS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(TRANSACTION_ID,0),nvl(REFERENCE_ACCOUNT,0),nvl(WIP_SUB_LEDGER_ID,0))
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wip_transaction_accounts')

            )
);
end;