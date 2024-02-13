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

DROP TABLE if exists bec_ods.HZ_RELATIONSHIPS;

CREATE TABLE IF NOT EXISTS bec_ods.hz_relationships
(
	relationship_id NUMERIC(15,0)   ENCODE az64
	,subject_id NUMERIC(15,0)   ENCODE az64
	,subject_type VARCHAR(30)   ENCODE lzo
	,subject_table_name VARCHAR(30)   ENCODE lzo
	,object_id NUMERIC(15,0)   ENCODE az64
	,object_type VARCHAR(30)   ENCODE lzo
	,object_table_name VARCHAR(30)   ENCODE lzo
	,party_id NUMERIC(15,0)   ENCODE az64
	,relationship_code VARCHAR(30)   ENCODE lzo
	,directional_flag VARCHAR(1)   ENCODE lzo
	,comments VARCHAR(240)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status VARCHAR(1)   ENCODE lzo
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,global_attribute_category VARCHAR(30)   ENCODE lzo
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
	,content_source_type VARCHAR(30)   ENCODE lzo
	,relationship_type VARCHAR(30)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by_module VARCHAR(150)   ENCODE lzo
	,application_id NUMERIC(15,0)   ENCODE az64
	,additional_information1 VARCHAR(150)   ENCODE lzo
	,additional_information2 VARCHAR(150)   ENCODE lzo
	,additional_information3 VARCHAR(150)   ENCODE lzo
	,additional_information4 VARCHAR(150)   ENCODE lzo
	,additional_information5 VARCHAR(150)   ENCODE lzo
	,additional_information6 VARCHAR(150)   ENCODE lzo
	,additional_information7 VARCHAR(150)   ENCODE lzo
	,additional_information8 VARCHAR(150)   ENCODE lzo
	,additional_information9 VARCHAR(150)   ENCODE lzo
	,additional_information10 VARCHAR(150)   ENCODE lzo
	,additional_information11 VARCHAR(150)   ENCODE lzo
	,additional_information12 VARCHAR(150)   ENCODE lzo
	,additional_information13 VARCHAR(150)   ENCODE lzo
	,additional_information14 VARCHAR(150)   ENCODE lzo
	,additional_information15 VARCHAR(150)   ENCODE lzo
	,additional_information16 VARCHAR(150)   ENCODE lzo
	,additional_information17 VARCHAR(150)   ENCODE lzo
	,additional_information18 VARCHAR(150)   ENCODE lzo
	,additional_information19 VARCHAR(150)   ENCODE lzo
	,additional_information20 VARCHAR(150)   ENCODE lzo
	,additional_information21 VARCHAR(150)   ENCODE lzo
	,additional_information22 VARCHAR(150)   ENCODE lzo
	,additional_information23 VARCHAR(150)   ENCODE lzo
	,additional_information24 VARCHAR(150)   ENCODE lzo
	,additional_information25 VARCHAR(150)   ENCODE lzo
	,additional_information26 VARCHAR(150)   ENCODE lzo
	,additional_information27 VARCHAR(150)   ENCODE lzo
	,additional_information28 VARCHAR(150)   ENCODE lzo
	,additional_information29 VARCHAR(150)   ENCODE lzo
	,additional_information30 VARCHAR(150)   ENCODE lzo
	,direction_code VARCHAR(30)   ENCODE lzo
	,percentage_ownership NUMERIC(28,10)   ENCODE az64
	,actual_content_source VARCHAR(30)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
	,is_deleted_flg VARCHAR(2)   ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
insert
	into
	bec_ods.hz_relationships
(relationship_id,
	subject_id,
	subject_type,
	subject_table_name,
	object_id,
	object_type,
	object_table_name,
	party_id,
	relationship_code,
	directional_flag,
	comments,
	start_date,
	end_date,
	status,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	wh_update_date,
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
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
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
	content_source_type,
	relationship_type,
	object_version_number,
	created_by_module,
	application_id,
	additional_information1,
	additional_information2,
	additional_information3,
	additional_information4,
	additional_information5,
	additional_information6,
	additional_information7,
	additional_information8,
	additional_information9,
	additional_information10,
	additional_information11,
	additional_information12,
	additional_information13,
	additional_information14,
	additional_information15,
	additional_information16,
	additional_information17,
	additional_information18,
	additional_information19,
	additional_information20,
	additional_information21,
	additional_information22,
	additional_information23,
	additional_information24,
	additional_information25,
	additional_information26,
	additional_information27,
	additional_information28,
	additional_information29,
	additional_information30,
	direction_code,
	percentage_ownership,
	actual_content_source,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	relationship_id,
	subject_id,
	subject_type,
	subject_table_name,
	object_id,
	object_type,
	object_table_name,
	party_id,
	relationship_code,
	directional_flag,
	comments,
	start_date,
	end_date,
	status,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	wh_update_date,
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
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
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
	content_source_type,
	relationship_type,
	object_version_number,
	created_by_module,
	application_id,
	additional_information1,
	additional_information2,
	additional_information3,
	additional_information4,
	additional_information5,
	additional_information6,
	additional_information7,
	additional_information8,
	additional_information9,
	additional_information10,
	additional_information11,
	additional_information12,
	additional_information13,
	additional_information14,
	additional_information15,
	additional_information16,
	additional_information17,
	additional_information18,
	additional_information19,
	additional_information20,
	additional_information21,
	additional_information22,
	additional_information23,
	additional_information24,
	additional_information25,
	additional_information26,
	additional_information27,
	additional_information28,
	additional_information29,
	additional_information30,
	direction_code,
	percentage_ownership,
	actual_content_source,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.hz_relationships;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_relationships';
	
commit;
	
