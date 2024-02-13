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

DROP TABLE if exists bec_ods.HR_LOCATIONS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.HR_LOCATIONS_ALL
(
	location_id NUMERIC(15,0)   ENCODE az64
	,entered_by NUMERIC(15,0)   ENCODE az64
	,location_code VARCHAR(60)   ENCODE lzo
	,address_line_1 VARCHAR(240)   ENCODE lzo
	,address_line_2 VARCHAR(240)   ENCODE lzo
	,address_line_3 VARCHAR(240)   ENCODE lzo
	,bill_to_site_flag VARCHAR(30)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,designated_receiver_id NUMERIC(15,0)   ENCODE az64
	,in_organization_flag VARCHAR(30)   ENCODE lzo
	,inactive_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inventory_organization_id NUMERIC(15,0)   ENCODE az64
	,office_site_flag VARCHAR(30)   ENCODE lzo
	,postal_code VARCHAR(30)   ENCODE lzo
	,receiving_site_flag VARCHAR(30)   ENCODE lzo
	,region_1 VARCHAR(120)   ENCODE lzo
	,region_2 VARCHAR(120)   ENCODE lzo
	,region_3 VARCHAR(120)   ENCODE lzo
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,ship_to_site_flag VARCHAR(30)   ENCODE lzo
	,style VARCHAR(7)   ENCODE lzo
	,tax_name VARCHAR(15)   ENCODE lzo
	,telephone_number_1 VARCHAR(60)   ENCODE lzo
	,telephone_number_2 VARCHAR(60)   ENCODE lzo
	,telephone_number_3 VARCHAR(60)   ENCODE lzo
	,town_or_city VARCHAR(30)   ENCODE lzo
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
	,attribute16 VARCHAR(150)   ENCODE lzo
	,attribute17 VARCHAR(150)   ENCODE lzo
	,attribute18 VARCHAR(150)   ENCODE lzo
	,attribute19 VARCHAR(150)   ENCODE lzo
	,attribute20 VARCHAR(150)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,object_version_number NUMERIC(9,0)    ENCODE az64
	,tp_header_id NUMERIC(15,0)   ENCODE az64
	,ece_tp_location_code VARCHAR(35)   ENCODE lzo
	,global_attribute_category VARCHAR(150)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,business_group_id NUMERIC(15,0)   ENCODE az64
	,loc_information13 VARCHAR(150)   ENCODE lzo
	,loc_information14 VARCHAR(150)   ENCODE lzo
	,loc_information15 VARCHAR(150)   ENCODE lzo
	,loc_information16 VARCHAR(150)   ENCODE lzo
	,loc_information17 VARCHAR(150)   ENCODE lzo
	,loc_information18 VARCHAR(150)   ENCODE lzo
	,loc_information19 VARCHAR(150)   ENCODE lzo
	,loc_information20 VARCHAR(150)   ENCODE lzo
	,derived_locale VARCHAR(240)   ENCODE lzo
	,legal_address_flag VARCHAR(30)   ENCODE lzo
	,timezone_code VARCHAR(50)   ENCODE lzo
	,"TAX_NAME#1" VARCHAR(50) ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.HR_LOCATIONS_ALL (
    location_id,
	entered_by,
	location_code,
	address_line_1,
	address_line_2,
	address_line_3,
	bill_to_site_flag,
	country,
	description,
	designated_receiver_id,
	in_organization_flag,
	inactive_date,
	inventory_organization_id,
	office_site_flag,
	postal_code,
	receiving_site_flag,
	region_1,
	region_2,
	region_3,
	ship_to_location_id,
	ship_to_site_flag,
	"style",
	tax_name,
	telephone_number_1,
	telephone_number_2,
	telephone_number_3,
	town_or_city,
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
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
	last_update_date,
	last_updated_by,
	last_update_login,
	created_by,
	creation_date,
	object_version_number,
	tp_header_id,
	ece_tp_location_code,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	business_group_id,
	loc_information13,
	loc_information14,
	loc_information15,
	loc_information16,
	loc_information17,
	loc_information18,
	loc_information19,
	loc_information20,
	derived_locale,
	legal_address_flag,
	timezone_code,
	"TAX_NAME#1",
	kca_operation,
	IS_DELETED_FLG,
	 kca_seq_id,
	kca_seq_date)
	SELECT
	location_id,
	entered_by,
	location_code,
	address_line_1,
	address_line_2,
	address_line_3,
	bill_to_site_flag,
	country,
	description,
	designated_receiver_id,
	in_organization_flag,
	inactive_date,
	inventory_organization_id,
	office_site_flag,
	postal_code,
	receiving_site_flag,
	region_1,
	region_2,
	region_3,
	ship_to_location_id,
	ship_to_site_flag,
	"style",
	tax_name,
	telephone_number_1,
	telephone_number_2,
	telephone_number_3,
	town_or_city,
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
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
	last_update_date,
	last_updated_by,
	last_update_login,
	created_by,
	creation_date,
	object_version_number,
	tp_header_id,
	ece_tp_location_code,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	business_group_id,
	loc_information13,
	loc_information14,
	loc_information15,
	loc_information16,
	loc_information17,
	loc_information18,
	loc_information19,
	loc_information20,
	derived_locale,
	legal_address_flag,
	timezone_code,
	"TAX_NAME#1",
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.HR_LOCATIONS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hr_locations_all';
	
commit;