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

truncate table bec_ods_stg.mtl_interorg_parameters;

insert into	bec_ods_stg.mtl_interorg_parameters
   (FROM_ORGANIZATION_ID,
	TO_ORGANIZATION_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	INTRANSIT_TYPE,
	DISTANCE_UOM_CODE,
	TO_ORGANIZATION_DISTANCE,
	FOB_POINT,
	MATL_INTERORG_TRANSFER_CODE,
	ROUTING_HEADER_ID,
	INTERNAL_ORDER_REQUIRED_FLAG,
	INTRANSIT_INV_ACCOUNT,
	INTERORG_TRNSFR_CHARGE_PERCENT,
	INTERORG_TRANSFER_CR_ACCOUNT,
	INTERORG_RECEIVABLES_ACCOUNT,
	INTERORG_PAYABLES_ACCOUNT,
	INTERORG_PRICE_VAR_ACCOUNT,
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
	GLOBAL_ATTRIBUTE_CATEGORY,
	GLOBAL_ATTRIBUTE1,
	GLOBAL_ATTRIBUTE2,
	GLOBAL_ATTRIBUTE3,
	GLOBAL_ATTRIBUTE4,
	GLOBAL_ATTRIBUTE5,
	GLOBAL_ATTRIBUTE6,
	GLOBAL_ATTRIBUTE7,
	GLOBAL_ATTRIBUTE8,
	GLOBAL_ATTRIBUTE9,
	GLOBAL_ATTRIBUTE10,
	GLOBAL_ATTRIBUTE11,
	GLOBAL_ATTRIBUTE12,
	GLOBAL_ATTRIBUTE13,
	GLOBAL_ATTRIBUTE14,
	GLOBAL_ATTRIBUTE15,
	GLOBAL_ATTRIBUTE16,
	GLOBAL_ATTRIBUTE17,
	GLOBAL_ATTRIBUTE18,
	GLOBAL_ATTRIBUTE19,
	GLOBAL_ATTRIBUTE20,
	ELEMENTAL_VISIBILITY_ENABLED,
	MANUAL_RECEIPT_EXPENSE,
	PROFIT_IN_INV_ACCOUNT,
	SHIKYU_ENABLED_FLAG,
	SHIKYU_DEFAULT_ORDER_TYPE_ID,
	SHIKYU_OEM_VAR_ACCOUNT_ID,
	SHIKYU_TP_OFFSET_ACCOUNT_ID,
	INTERORG_PROFIT_ACCOUNT,
	PRICELIST_ID,
	SUBCONTRACTING_TYPE,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		FROM_ORGANIZATION_ID,
		TO_ORGANIZATION_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		INTRANSIT_TYPE,
		DISTANCE_UOM_CODE,
		TO_ORGANIZATION_DISTANCE,
		FOB_POINT,
		MATL_INTERORG_TRANSFER_CODE,
		ROUTING_HEADER_ID,
		INTERNAL_ORDER_REQUIRED_FLAG,
		INTRANSIT_INV_ACCOUNT,
		INTERORG_TRNSFR_CHARGE_PERCENT,
		INTERORG_TRANSFER_CR_ACCOUNT,
		INTERORG_RECEIVABLES_ACCOUNT,
		INTERORG_PAYABLES_ACCOUNT,
		INTERORG_PRICE_VAR_ACCOUNT,
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
		GLOBAL_ATTRIBUTE_CATEGORY,
		GLOBAL_ATTRIBUTE1,
		GLOBAL_ATTRIBUTE2,
		GLOBAL_ATTRIBUTE3,
		GLOBAL_ATTRIBUTE4,
		GLOBAL_ATTRIBUTE5,
		GLOBAL_ATTRIBUTE6,
		GLOBAL_ATTRIBUTE7,
		GLOBAL_ATTRIBUTE8,
		GLOBAL_ATTRIBUTE9,
		GLOBAL_ATTRIBUTE10,
		GLOBAL_ATTRIBUTE11,
		GLOBAL_ATTRIBUTE12,
		GLOBAL_ATTRIBUTE13,
		GLOBAL_ATTRIBUTE14,
		GLOBAL_ATTRIBUTE15,
		GLOBAL_ATTRIBUTE16,
		GLOBAL_ATTRIBUTE17,
		GLOBAL_ATTRIBUTE18,
		GLOBAL_ATTRIBUTE19,
		GLOBAL_ATTRIBUTE20,
		ELEMENTAL_VISIBILITY_ENABLED,
		MANUAL_RECEIPT_EXPENSE,
		PROFIT_IN_INV_ACCOUNT,
		SHIKYU_ENABLED_FLAG,
		SHIKYU_DEFAULT_ORDER_TYPE_ID,
		SHIKYU_OEM_VAR_ACCOUNT_ID,
		SHIKYU_TP_OFFSET_ACCOUNT_ID,
		INTERORG_PROFIT_ACCOUNT,
		PRICELIST_ID,
		SUBCONTRACTING_TYPE,
        KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.mtl_interorg_parameters
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,kca_seq_id) in 
	(select FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,max(kca_seq_id) from bec_raw_dl_ext.mtl_interorg_parameters 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID)
        and	(kca_seq_date  > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_interorg_parameters')
		 
            )	
);
end;