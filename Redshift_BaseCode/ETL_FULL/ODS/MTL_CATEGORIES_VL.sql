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

DROP TABLE if exists bec_ods.MTL_CATEGORIES_VL;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_CATEGORIES_VL
(
	category_id NUMERIC(15,0)   ENCODE az64
	,structure_id NUMERIC(15,0)   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,web_status VARCHAR(30)   ENCODE lzo
	,supplier_enabled_flag VARCHAR(1)   ENCODE lzo
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
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_CATEGORIES_VL (
    category_id,
	structure_id,
	disable_date,
	web_status,
	supplier_enabled_flag,
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
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	description,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
    SELECT
        category_id,
	structure_id,
	disable_date,
	web_status,
	supplier_enabled_flag,
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
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	description,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_CATEGORIES_VL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_categories_vl';
	
commit;