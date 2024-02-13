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

drop table if exists bec_ods.MTL_UNITS_OF_MEASURE_TL;

create table if not exists bec_ods.MTL_UNITS_OF_MEASURE_TL
(
	unit_of_measure VARCHAR(25) ENCODE lzo ,
	uom_code VARCHAR(3) ENCODE lzo ,
	uom_class VARCHAR(10) ENCODE lzo ,
	base_uom_flag VARCHAR(1) ENCODE lzo ,
	unit_of_measure_tl VARCHAR(25) ENCODE lzo ,
	last_update_date TIMESTAMP without TIME zone ENCODE az64 ,
	last_updated_by numeric(15,0) ENCODE az64 ,
	created_by numeric(15,0) ENCODE az64 ,
	creation_date TIMESTAMP without TIME zone ENCODE az64 ,
	last_update_login numeric(15,0) ENCODE az64 ,
	disable_date TIMESTAMP without TIME zone ENCODE az64 ,
	description VARCHAR(50) ENCODE lzo ,
	"language" VARCHAR(4) ENCODE lzo ,
	source_lang VARCHAR(4) ENCODE lzo ,
	attribute_category VARCHAR(30) ENCODE lzo ,
	attribute1 VARCHAR(150) ENCODE lzo ,
	attribute2 VARCHAR(150) ENCODE lzo ,
	attribute3 VARCHAR(150) ENCODE lzo ,
	attribute4 VARCHAR(150) ENCODE lzo ,
	attribute5 VARCHAR(150) ENCODE lzo ,
	attribute6 VARCHAR(150) ENCODE lzo ,
	attribute7 VARCHAR(150) ENCODE lzo ,
	attribute8 VARCHAR(150) ENCODE lzo ,
	attribute9 VARCHAR(150) ENCODE lzo ,
	attribute10 VARCHAR(150) ENCODE lzo ,
	attribute11 VARCHAR(150) ENCODE lzo ,
	attribute12 VARCHAR(150) ENCODE lzo ,
	attribute13 VARCHAR(150) ENCODE lzo ,
	attribute14 VARCHAR(150) ENCODE lzo ,
	attribute15 VARCHAR(150) ENCODE lzo ,
	request_id numeric(15,0) ENCODE az64 ,
	program_application_id numeric(15,0) ENCODE az64 ,
	program_id numeric(15,0) ENCODE az64 ,
	program_update_date TIMESTAMP without TIME zone ENCODE az64 ,
	zd_edition_name VARCHAR(30) ENCODE lzo ,
	zd_sync VARCHAR(30) ENCODE lzo ,
	kca_operation VARCHAR(10) ENCODE lzo ,
	is_deleted_flg VARCHAR(2) ENCODE lzo ,
	kca_seq_id numeric(36,0) ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
diststyle
auto;

insert
	into
	bec_ods.MTL_UNITS_OF_MEASURE_TL (
	unit_of_measure,
	uom_code,
	uom_class,
	base_uom_flag,
	unit_of_measure_tl,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	last_update_login,
	disable_date,
	description,
	"language",
	source_lang,
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
	zd_edition_name,
	zd_sync,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date 
	)
select
	unit_of_measure,
	uom_code,
	uom_class,
	base_uom_flag,
	unit_of_measure_tl,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	last_update_login,
	disable_date,
	description,
	"language",
	source_lang,
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
	zd_edition_name,
	zd_sync,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date	
from
	bec_ods_stg.MTL_UNITS_OF_MEASURE_TL;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'mtl_units_of_measure_tl';