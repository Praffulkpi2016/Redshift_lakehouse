/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.MTL_CONSUMPTION_TRANSACTIONS
where TRANSACTION_ID in (
select stg.TRANSACTION_ID from bec_ods.MTL_CONSUMPTION_TRANSACTIONS ods, bec_ods_stg.MTL_CONSUMPTION_TRANSACTIONS stg
where ods.TRANSACTION_ID = stg.TRANSACTION_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.mtl_consumption_transactions
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.mtl_consumption_transactions
	where kca_operation IN ('INSERT','UPDATE') 
	and (TRANSACTION_ID,kca_seq_id) in 
	(select TRANSACTION_ID,max(kca_seq_id) from bec_ods_stg.mtl_consumption_transactions 
     where kca_operation IN ('INSERT','UPDATE')
     group by TRANSACTION_ID)
);

commit;

 

-- Soft delete
update bec_ods.mtl_consumption_transactions set IS_DELETED_FLG = 'N';
commit;
update bec_ods.mtl_consumption_transactions set IS_DELETED_FLG = 'Y'
where (TRANSACTION_ID)  in
(
select TRANSACTION_ID from bec_raw_dl_ext.mtl_consumption_transactions
where (TRANSACTION_ID,KCA_SEQ_ID)
in 
(
select TRANSACTION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.mtl_consumption_transactions
group by TRANSACTION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_consumption_transactions';

commit;