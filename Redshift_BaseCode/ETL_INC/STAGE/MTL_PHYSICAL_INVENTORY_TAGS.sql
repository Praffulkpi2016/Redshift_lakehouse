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
	table bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS;

COMMIT;

insert
	into
	bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS
(	TAG_ID, 
	PHYSICAL_INVENTORY_ID, 
	ORGANIZATION_ID, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	VOID_FLAG, 
	TAG_NUMBER, 
	ADJUSTMENT_ID, 
	INVENTORY_ITEM_ID, 
	TAG_QUANTITY, 
	TAG_UOM, 
	TAG_QUANTITY_AT_STANDARD_UOM, 
	STANDARD_UOM, 
	SUBINVENTORY, 
	LOCATOR_ID, 
	LOT_NUMBER, 
	LOT_EXPIRATION_DATE, 
	REVISION, 
	SERIAL_NUM, 
	COUNTED_BY_EMPLOYEE_ID, 
	LOT_SERIAL_CONTROLS, 
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
	PARENT_LPN_ID, 
	OUTERMOST_LPN_ID, 
	COST_GROUP_ID, 
	TAG_SECONDARY_QUANTITY, 
	TAG_SECONDARY_UOM, 
	TAG_QTY_AT_STD_SECONDARY_UOM, 
	STANDARD_SECONDARY_UOM,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		TAG_ID, 
		PHYSICAL_INVENTORY_ID, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		VOID_FLAG, 
		TAG_NUMBER, 
		ADJUSTMENT_ID, 
		INVENTORY_ITEM_ID, 
		TAG_QUANTITY, 
		TAG_UOM, 
		TAG_QUANTITY_AT_STANDARD_UOM, 
		STANDARD_UOM, 
		SUBINVENTORY, 
		LOCATOR_ID, 
		LOT_NUMBER, 
		LOT_EXPIRATION_DATE, 
		REVISION, 
		SERIAL_NUM, 
		COUNTED_BY_EMPLOYEE_ID, 
		LOT_SERIAL_CONTROLS, 
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
		PARENT_LPN_ID, 
		OUTERMOST_LPN_ID, 
		COST_GROUP_ID, 
		TAG_SECONDARY_QUANTITY, 
		TAG_SECONDARY_UOM, 
		TAG_QTY_AT_STD_SECONDARY_UOM, 
		STANDARD_SECONDARY_UOM,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.MTL_PHYSICAL_INVENTORY_TAGS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(TAG_ID, 0),
		kca_seq_id) in 
(
		select
			nvl(TAG_ID,0) as TAG_ID,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.MTL_PHYSICAL_INVENTORY_TAGS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(TAG_ID, 0))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_physical_inventory_tags')
);
end;