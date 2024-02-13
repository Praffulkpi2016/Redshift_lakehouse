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

drop table if exists bec_dwh.DIM_ITEM_LOCATIONS;

create table bec_dwh.DIM_ITEM_LOCATIONS diststyle all sortkey(INVENTORY_LOCATION_ID,ORGANIZATION_ID)
as
(
SELECT 
 INVENTORY_LOCATION_ID
,ORGANIZATION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,DESCRIPTIVE_TEXT
,DISABLE_DATE
,INVENTORY_LOCATION_TYPE
,PICKING_ORDER
,PHYSICAL_LOCATION_CODE
,LOCATION_MAXIMUM_UNITS
,SUBINVENTORY_CODE
,LOCATION_WEIGHT_UOM_CODE
,MAX_WEIGHT
,VOLUME_UOM_CODE
,MAX_CUBIC_AREA
,X_COORDINATE
,Y_COORDINATE
,Z_COORDINATE
,INVENTORY_ACCOUNT_ID
,SEGMENT1
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,SEGMENT6
,SEGMENT7
,SEGMENT8
,SEGMENT9
,SEGMENT10
,SUMMARY_FLAG
,ENABLED_FLAG
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,PROJECT_ID
,TASK_ID
,PHYSICAL_LOCATION_ID
,PICK_UOM_CODE
,DIMENSION_UOM_CODE
,LENGTH
,WIDTH
,HEIGHT
,LOCATOR_STATUS
,STATUS_ID
,CURRENT_CUBIC_AREA
,AVAILABLE_CUBIC_AREA
,CURRENT_WEIGHT
,AVAILABLE_WEIGHT
,LOCATION_CURRENT_UNITS
,LOCATION_AVAILABLE_UNITS
,INVENTORY_ITEM_ID
,SUGGESTED_CUBIC_AREA
,SUGGESTED_WEIGHT
,LOCATION_SUGGESTED_UNITS
,EMPTY_FLAG
,MIXED_ITEMS_FLAG
,DROPPING_ORDER
,AVAILABILITY_TYPE
,INVENTORY_ATP_CODE
,RESERVABLE_TYPE
,ALIAS
,INV_DOCK_DOOR_ID
,INV_ORG_ID
	,'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-' || nvl(INVENTORY_LOCATION_ID, 0)
	|| '-' || nvl(ORGANIZATION_ID, 0)	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.mtl_item_locations
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_item_locations'
	and batch_name = 'inv';

commit;
