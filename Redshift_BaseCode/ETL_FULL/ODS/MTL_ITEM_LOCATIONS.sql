/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.MTL_ITEM_LOCATIONS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_ITEM_LOCATIONS
(
    inventory_location_id NUMERIC(15,0)   ENCODE az64
	,INV_DOCK_DOOR_ID NUMERIC(15,0)   ENCODE az64
	,INV_ORG_ID NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(50)   ENCODE lzo
	,descriptive_text VARCHAR(240)   ENCODE lzo
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inventory_location_type NUMERIC(15,0)   ENCODE az64
	,picking_order NUMERIC(15,0)   ENCODE az64
	,physical_location_code VARCHAR(10)   ENCODE lzo
	,location_maximum_units NUMERIC(15,0)   ENCODE az64
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,location_weight_uom_code VARCHAR(3)   ENCODE lzo
	,max_weight NUMERIC(28,10)   ENCODE az64
	,volume_uom_code VARCHAR(3)   ENCODE lzo
	,max_cubic_area NUMERIC(28,10)   ENCODE az64
	,x_coordinate NUMERIC(15,0)   ENCODE az64
	,y_coordinate NUMERIC(15,0)   ENCODE az64
	,z_coordinate NUMERIC(15,0)   ENCODE az64
	,inventory_account_id NUMERIC(15,0)   ENCODE az64
	,segment1 VARCHAR(40)   ENCODE lzo
	,segment2 VARCHAR(40)   ENCODE lzo
	,segment3 VARCHAR(40)   ENCODE lzo
	,segment4 VARCHAR(40)   ENCODE lzo
	,segment5 VARCHAR(40)   ENCODE lzo
	,segment6 VARCHAR(40)   ENCODE lzo
	,segment7 VARCHAR(40)   ENCODE lzo
	,segment8 VARCHAR(40)   ENCODE lzo
	,segment9 VARCHAR(40)   ENCODE lzo
	,segment10 VARCHAR(40)   ENCODE lzo
	,segment11 VARCHAR(40)   ENCODE lzo
	,segment12 VARCHAR(40)   ENCODE lzo
	,segment13 VARCHAR(40)   ENCODE lzo
	,segment14 VARCHAR(40)   ENCODE lzo
	,segment15 VARCHAR(40)   ENCODE lzo
	,segment16 VARCHAR(40)   ENCODE lzo
	,segment17 VARCHAR(40)   ENCODE lzo
	,segment18 VARCHAR(40)   ENCODE lzo
	,segment19 VARCHAR(40)   ENCODE lzo
	,segment20 VARCHAR(40)   ENCODE lzo
	,summary_flag VARCHAR(1)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,physical_location_id NUMERIC(15,0)   ENCODE az64
	,pick_uom_code VARCHAR(3)   ENCODE lzo
	,dimension_uom_code VARCHAR(3)   ENCODE lzo
	,length NUMERIC(28,10)   ENCODE az64
	,width NUMERIC(28,10)   ENCODE az64
	,height NUMERIC(28,10)   ENCODE az64
	,locator_status NUMERIC(15,0)   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,current_cubic_area NUMERIC(28,10)   ENCODE az64
	,available_cubic_area NUMERIC(28,10)   ENCODE az64
	,current_weight NUMERIC(28,10)   ENCODE az64
	,available_weight NUMERIC(28,10)   ENCODE az64
	,location_current_units NUMERIC(28,10)   ENCODE az64
	,location_available_units NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,suggested_cubic_area NUMERIC(28,10)   ENCODE az64
	,suggested_weight NUMERIC(28,10)   ENCODE az64
	,location_suggested_units NUMERIC(15,0)   ENCODE az64
	,empty_flag VARCHAR(1)   ENCODE lzo
	,mixed_items_flag VARCHAR(1)   ENCODE lzo
	,dropping_order NUMERIC(15,0)   ENCODE az64
	,availability_type NUMERIC(15,0)   ENCODE az64
	,inventory_atp_code NUMERIC(15,0)   ENCODE az64
	,reservable_type NUMERIC(15,0)   ENCODE az64
	,alias VARCHAR(30)   ENCODE lzo 
	,AREA_ROW_ID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_ITEM_LOCATIONS (
    inventory_location_id,
	inv_dock_door_id, 
	inv_org_id  ,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	descriptive_text,
	disable_date,
	inventory_location_type,
	picking_order,
	physical_location_code,
	location_maximum_units,
	subinventory_code,
	location_weight_uom_code,
	max_weight,
	volume_uom_code,
	max_cubic_area,
	x_coordinate,
	y_coordinate,
	z_coordinate,
	inventory_account_id,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	segment11,
	segment12,
	segment13,
	segment14,
	segment15,
	segment16,
	segment17,
	segment18,
	segment19,
	segment20,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_id,
	task_id,
	physical_location_id,
	pick_uom_code,
	dimension_uom_code,
	length,
	width,
	height,
	locator_status,
	status_id,
	current_cubic_area,
	available_cubic_area,
	current_weight,
	available_weight,
	location_current_units,
	location_available_units,
	inventory_item_id,
	suggested_cubic_area,
	suggested_weight,
	location_suggested_units,
	empty_flag,
	mixed_items_flag,
	dropping_order,
	availability_type,
	inventory_atp_code,
	reservable_type,
	alias,
	AREA_ROW_ID,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
    inventory_location_id,
	inv_dock_door_id, 
	inv_org_id  ,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	descriptive_text,
	disable_date,
	inventory_location_type,
	picking_order,
	physical_location_code,
	location_maximum_units,
	subinventory_code,
	location_weight_uom_code,
	max_weight,
	volume_uom_code,
	max_cubic_area,
	x_coordinate,
	y_coordinate,
	z_coordinate,
	inventory_account_id,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	segment11,
	segment12,
	segment13,
	segment14,
	segment15,
	segment16,
	segment17,
	segment18,
	segment19,
	segment20,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_id,
	task_id,
	physical_location_id,
	pick_uom_code,
	dimension_uom_code,
	length,
	width,
	height,
	locator_status,
	status_id,
	current_cubic_area,
	available_cubic_area,
	current_weight,
	available_weight,
	location_current_units,
	location_available_units,
	inventory_item_id,
	suggested_cubic_area,
	suggested_weight,
	location_suggested_units,
	empty_flag,
	mixed_items_flag,
	dropping_order,
	availability_type,
	inventory_atp_code,
	reservable_type,
	alias,
	AREA_ROW_ID,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_ITEM_LOCATIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_item_locations';
	
commit;