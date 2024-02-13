/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents incremental approach for ODS.
	# File Version: KPI v1.0
*/
begin;
TRUNCATE TABLE bec_ods.XXBEC_INVENTORY_VALUE_RPT;
	
	insert into bec_ods.XXBEC_INVENTORY_VALUE_RPT
	(
		AS_OF_DATE, 
		INV_CATEGORY, 
		INV_SUBCATEGORY, 
		SUBINVENTORY, 
		LOCATOR, 
		PART_NUMBER, 
		DESCRIPTION, 
		INVENTORY_ITEM_STATUS_CODE, 
		CATEGORY_SEGMENT1, 
		CATEGORY_SEGMENT2, 
		CATEGORY, 
		PRIMARY_UOM_CODE, 
		QUANTITY, 
		ITEM_COST, 
		EXTENDED_COST, 
		ORGANIZATION_ID, 
		ORGANIZATION_NAME, 
		ORGANIZATION_CODE, 
		VMI_FLAG, 
		OWNING_ORGANIZATION_ID, 
		SUBINVENTORY_TYPE, 
		MATERIAL_COST, 
		MATERIAL_OVERHEAD_COST, 
		RESOURCE_COST, 
		OUTSIDE_PROCESSING_COST, 
		OVERHEAD_COST, 
		EXT_MATERIAL_COST, 
		EXT_MATERIAL_OVERHEAD_COST, 
		EXT_RESOURCE_COST, 
		EXT_OUTSIDE_PROCESSING_COST, 
		EXT_OVERHEAD_COST, 
		LOCATOR_ID, 
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		AS_OF_DATE, 
		INV_CATEGORY, 
		INV_SUBCATEGORY, 
		SUBINVENTORY, 
		LOCATOR, 
		PART_NUMBER, 
		DESCRIPTION, 
		INVENTORY_ITEM_STATUS_CODE, 
		CATEGORY_SEGMENT1, 
		CATEGORY_SEGMENT2, 
		CATEGORY, 
		PRIMARY_UOM_CODE, 
		QUANTITY, 
		ITEM_COST, 
		EXTENDED_COST, 
		ORGANIZATION_ID, 
		ORGANIZATION_NAME, 
		ORGANIZATION_CODE, 
		VMI_FLAG, 
		OWNING_ORGANIZATION_ID, 
		SUBINVENTORY_TYPE, 
		MATERIAL_COST, 
		MATERIAL_OVERHEAD_COST, 
		RESOURCE_COST, 
		OUTSIDE_PROCESSING_COST, 
		OVERHEAD_COST, 
		EXT_MATERIAL_COST, 
		EXT_MATERIAL_OVERHEAD_COST, 
		EXT_RESOURCE_COST, 
		EXT_OUTSIDE_PROCESSING_COST, 
		EXT_OVERHEAD_COST, 
		LOCATOR_ID,  
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.XXBEC_INVENTORY_VALUE_RPT
	);
	
end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='xxbec_inventory_value_rpt'; 

commit;