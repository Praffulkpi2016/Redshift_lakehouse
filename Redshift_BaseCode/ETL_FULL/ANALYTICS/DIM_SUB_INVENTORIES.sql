/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.DIM_SUB_INVENTORIES;

create table bec_dwh.DIM_SUB_INVENTORIES 
diststyle all 
sortkey(ORGANIZATION_ID,SECONDARY_INVENTORY_NAME) as
(select
	SECONDARY_INVENTORY_NAME
,ORGANIZATION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,DISABLE_DATE
,INVENTORY_ATP_CODE
,AVAILABILITY_TYPE
,RESERVABLE_TYPE
,LOCATOR_TYPE
,PICKING_ORDER
,MATERIAL_ACCOUNT
,MATERIAL_OVERHEAD_ACCOUNT
,RESOURCE_ACCOUNT
,OVERHEAD_ACCOUNT
,OUTSIDE_PROCESSING_ACCOUNT
,QUANTITY_TRACKED
,ASSET_INVENTORY
,SOURCE_TYPE
,SOURCE_SUBINVENTORY
,SOURCE_ORGANIZATION_ID
,REQUISITION_APPROVAL_TYPE
,EXPENSE_ACCOUNT
,ENCUMBRANCE_ACCOUNT
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,PREPROCESSING_LEAD_TIME
,PROCESSING_LEAD_TIME
,POSTPROCESSING_LEAD_TIME
,DEMAND_CLASS
,PROJECT_ID
,TASK_ID
,SUBINVENTORY_USAGE
,NOTIFY_LIST_ID
,PICK_UOM_CODE
,DEPRECIABLE_FLAG
,LOCATION_ID
,DEFAULT_COST_GROUP_ID
,STATUS_ID
,DEFAULT_LOC_STATUS_ID
,LPN_CONTROLLED_FLAG
,PICK_METHODOLOGY
,CARTONIZATION_FLAG
,DROPPING_ORDER
,SUBINVENTORY_TYPE
,PLANNING_LEVEL
,DEFAULT_COUNT_TYPE_CODE
,ENABLE_BULK_PICK
,ENABLE_LOCATOR_ALIAS
,ENFORCE_ALIAS_UNIQUENESS
,ENABLE_OPP_CYC_COUNT
,OPP_CYC_COUNT_DAYS
,OPP_CYC_COUNT_HEADER_ID
,OPP_CYC_COUNT_QUANTITY
	-- audit columns
	,'N' as is_deleted_flg
	,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id
	,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')
	|| '-' || nvl(ORGANIZATION_ID, 0) || '-' || nvl(SECONDARY_INVENTORY_NAME, 'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	from bec_ods.MTL_SECONDARY_INVENTORIES);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_sub_inventories'
	and batch_name = 'inv';

commit;