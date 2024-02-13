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

truncate table bec_ods_stg.mtl_consumption_transactions;

insert into	bec_ods_stg.mtl_consumption_transactions
   (TRANSACTION_ID,
	CONSUMPTION_RELEASE_ID,
	CONSUMPTION_PO_HEADER_ID,
	CONSUMPTION_PROCESSED_FLAG,
	REQUEST_ID,
	CREATED_BY,
	PROGRAM_APPLICATION_ID,
	CREATION_DATE,
	PROGRAM_ID,
	LAST_UPDATED_BY,
	PROGRAM_UPDATE_DATE,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	NET_QTY,
	BATCH_ID,
	RATE,
	RATE_TYPE,
	TAX_CODE_ID,
	TAX_RATE,
	ERROR_CODE,
	RECOVERABLE_TAX,
	NON_RECOVERABLE_TAX,
	TAX_RECOVERY_RATE,
	PARENT_TRANSACTION_ID,
	CHARGE_ACCOUNT_ID,
	VARIANCE_ACCOUNT_ID,
	GLOBAL_AGREEMENT_FLAG,
	NEED_BY_DATE,
	ERROR_EXPLANATION,
	SECONDARY_NET_QTY,
	BLANKET_PRICE,
	PO_DISTRIBUTION_ID,
	INTERFACE_DISTRIBUTION_REF,
	TRANSACTION_SOURCE_ID,
	INVENTORY_ITEM_ID,
	ACCRUAL_ACCOUNT_ID,
	ORGANIZATION_ID,
	OWNING_ORGANIZATION_ID,
	TRANSACTION_DATE,
	PO_LINE_ID,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		TRANSACTION_ID,
		CONSUMPTION_RELEASE_ID,
		CONSUMPTION_PO_HEADER_ID,
		CONSUMPTION_PROCESSED_FLAG,
		REQUEST_ID,
		CREATED_BY,
		PROGRAM_APPLICATION_ID,
		CREATION_DATE,
		PROGRAM_ID,
		LAST_UPDATED_BY,
		PROGRAM_UPDATE_DATE,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		NET_QTY,
		BATCH_ID,
		RATE,
		RATE_TYPE,
		TAX_CODE_ID,
		TAX_RATE,
		ERROR_CODE,
		RECOVERABLE_TAX,
		NON_RECOVERABLE_TAX,
		TAX_RECOVERY_RATE,
		PARENT_TRANSACTION_ID,
		CHARGE_ACCOUNT_ID,
		VARIANCE_ACCOUNT_ID,
		GLOBAL_AGREEMENT_FLAG,
		NEED_BY_DATE,
		ERROR_EXPLANATION,
		SECONDARY_NET_QTY,
		BLANKET_PRICE,
		PO_DISTRIBUTION_ID,
		INTERFACE_DISTRIBUTION_REF,
		TRANSACTION_SOURCE_ID,
		INVENTORY_ITEM_ID,
		ACCRUAL_ACCOUNT_ID,
		ORGANIZATION_ID,
		OWNING_ORGANIZATION_ID,
		TRANSACTION_DATE,
		PO_LINE_ID,
        KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.mtl_consumption_transactions
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (TRANSACTION_ID,kca_seq_id) in 
	(select TRANSACTION_ID,max(kca_seq_id) from bec_raw_dl_ext.mtl_consumption_transactions 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by TRANSACTION_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_consumption_transactions')
		 
            )
);
end;