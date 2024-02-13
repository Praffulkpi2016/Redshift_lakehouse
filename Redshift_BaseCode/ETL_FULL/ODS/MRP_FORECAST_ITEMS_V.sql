/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full incremental approach for ODS.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_ods.MRP_FORECAST_ITEMS_V;

CREATE TABLE IF NOT EXISTS bec_ods.MRP_FORECAST_ITEMS_V
(
	inventory_item_id NUMERIC(15,0)   ENCODE az64
	,concatenated_segments VARCHAR(100)   ENCODE lzo
	,item_description VARCHAR(2000)   ENCODE lzo
	,primary_uom_code VARCHAR(10)   ENCODE lzo
	,bom_item_type NUMERIC(15,0)   ENCODE az64
	,pick_components_flag VARCHAR(3)   ENCODE lzo
	,bom_item_type_desc VARCHAR(2000)   ENCODE lzo
	,ato_forecast_control_desc VARCHAR(2000)   ENCODE lzo
	,mrp_planning_code_desc VARCHAR(2000)   ENCODE lzo
	,alternate_bom_designator VARCHAR(100)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,forecast_designator VARCHAR(100)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(100)   ENCODE lzo
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
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

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