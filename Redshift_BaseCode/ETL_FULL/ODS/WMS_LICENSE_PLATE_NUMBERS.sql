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

DROP TABLE if exists bec_ods.WMS_LICENSE_PLATE_NUMBERS;

CREATE TABLE IF NOT EXISTS bec_ods.WMS_LICENSE_PLATE_NUMBERS
(

     lpn_id NUMERIC(15,0)   ENCODE az64
	,license_plate_number VARCHAR(30)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,revision VARCHAR(3)   ENCODE lzo
	,lot_number VARCHAR(80)   ENCODE lzo
	,serial_number VARCHAR(30)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,parent_lpn_id NUMERIC(15,0)   ENCODE az64
	,gross_weight_uom_code VARCHAR(3)   ENCODE lzo
	,gross_weight NUMERIC(28,10)   ENCODE az64
	,content_volume_uom_code VARCHAR(3)   ENCODE lzo
	,content_volume NUMERIC(28,10)   ENCODE az64
	,tare_weight_uom_code VARCHAR(3)   ENCODE lzo
	,tare_weight NUMERIC(28,10)   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,lpn_state NUMERIC(28,10)   ENCODE az64
	,sealed_status NUMERIC(15,0)   ENCODE az64
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
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,lpn_context NUMERIC(15,0)   ENCODE az64
	,lpn_reusability NUMERIC(15,0)   ENCODE az64
	,outermost_lpn_id NUMERIC(15,0)   ENCODE az64
	,homogeneous_container NUMERIC(28,10)   ENCODE az64
	,source_type_id NUMERIC(15,0)   ENCODE az64
	,source_header_id NUMERIC(15,0)   ENCODE az64
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,source_line_detail_id NUMERIC(15,0)   ENCODE az64
	,source_name VARCHAR(30)   ENCODE lzo
	,container_volume NUMERIC(28,0)   ENCODE az64
	,container_volume_uom VARCHAR(3)   ENCODE lzo
	,catch_weight_flag VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.WMS_LICENSE_PLATE_NUMBERS (
    lpn_id,
	license_plate_number,
	inventory_item_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	revision,
	lot_number,
	serial_number,
	organization_id,
	subinventory_code,
	locator_id,
	parent_lpn_id,
	gross_weight_uom_code,
	gross_weight,
	content_volume_uom_code,
	content_volume,
	tare_weight_uom_code,
	tare_weight,
	status_id,
	lpn_state,
	sealed_status,
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
	cost_group_id,
	lpn_context,
	lpn_reusability,
	outermost_lpn_id,
	homogeneous_container,
	source_type_id,
	source_header_id,
	source_line_id,
	source_line_detail_id,
	source_name,
	container_volume,
	container_volume_uom,
	catch_weight_flag,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    select
	lpn_id,
	license_plate_number,
	inventory_item_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	revision,
	lot_number,
	serial_number,
	organization_id,
	subinventory_code,
	locator_id,
	parent_lpn_id,
	gross_weight_uom_code,
	gross_weight,
	content_volume_uom_code,
	content_volume,
	tare_weight_uom_code,
	tare_weight,
	status_id,
	lpn_state,
	sealed_status,
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
	cost_group_id,
	lpn_context,
	lpn_reusability,
	outermost_lpn_id,
	homogeneous_container,
	source_type_id,
	source_header_id,
	source_line_id,
	source_line_detail_id,
	source_name,
	container_volume,
	container_volume_uom,
	catch_weight_flag,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'wms_license_plate_numbers';

commit;