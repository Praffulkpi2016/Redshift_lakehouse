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
truncate table bec_ods.MRP_FORECAST_ITEMS_V;

insert into bec_ods.MRP_FORECAST_ITEMS_V
(
	inventory_item_id,
	concatenated_segments,
	item_description,
	primary_uom_code,
	bom_item_type,
	pick_components_flag,
	bom_item_type_desc,
	ato_forecast_control_desc,
	mrp_planning_code_desc,
	alternate_bom_designator,
	organization_id,
	forecast_designator,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
select 
	inventory_item_id,
	concatenated_segments,
	item_description,
	primary_uom_code,
	bom_item_type,
	pick_components_flag,
	bom_item_type_desc,
	ato_forecast_control_desc,
	mrp_planning_code_desc,
	alternate_bom_designator,
	organization_id,
	forecast_designator,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.MRP_FORECAST_ITEMS_V
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mrp_forecast_items_v'; 

commit;