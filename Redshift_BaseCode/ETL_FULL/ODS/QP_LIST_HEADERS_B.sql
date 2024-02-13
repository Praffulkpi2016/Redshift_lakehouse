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

DROP TABLE if exists bec_ods.QP_LIST_HEADERS_B;

CREATE TABLE IF NOT EXISTS bec_ods.QP_LIST_HEADERS_B
(
	list_header_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,list_type_code VARCHAR(30)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,automatic_flag VARCHAR(1)   ENCODE lzo
	,currency_code VARCHAR(30)   ENCODE lzo
	,rounding_factor NUMERIC(15,0)   ENCODE az64
	,ship_method_code VARCHAR(30)   ENCODE lzo
	,freight_terms_code VARCHAR(30)   ENCODE lzo
	,terms_id NUMERIC(15,0)   ENCODE az64
	,comments VARCHAR(2000)   ENCODE lzo
	,discount_lines_flag VARCHAR(1)   ENCODE lzo
	,gsa_indicator VARCHAR(1)   ENCODE lzo
	,prorate_flag VARCHAR(30)   ENCODE lzo
	,source_system_code VARCHAR(30)   ENCODE lzo
	,ask_for_flag VARCHAR(1)   ENCODE lzo
	,active_flag VARCHAR(1)   ENCODE lzo
	,parent_list_header_id NUMERIC(15,0)   ENCODE az64
	,start_date_active_first TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active_first TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,active_date_first_type VARCHAR(30)   ENCODE lzo
	,start_date_active_second TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active_second TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,active_date_second_type VARCHAR(30)   ENCODE lzo
	,context VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(240)   ENCODE lzo
	,attribute2 VARCHAR(240)   ENCODE lzo
	,attribute3 VARCHAR(240)   ENCODE lzo
	,attribute4 VARCHAR(240)   ENCODE lzo
	,attribute5 VARCHAR(240)   ENCODE lzo
	,attribute6 VARCHAR(240)   ENCODE lzo
	,attribute7 VARCHAR(240)   ENCODE lzo
	,attribute8 VARCHAR(240)   ENCODE lzo
	,attribute9 VARCHAR(240)   ENCODE lzo
	,attribute10 VARCHAR(240)   ENCODE lzo
	,attribute11 VARCHAR(240)   ENCODE lzo
	,attribute12 VARCHAR(240)   ENCODE lzo
	,attribute13 VARCHAR(240)   ENCODE lzo
	,attribute14 VARCHAR(240)   ENCODE lzo
	,attribute15 VARCHAR(240)   ENCODE lzo
	,limit_exists_flag VARCHAR(1)   ENCODE lzo
	,mobile_download VARCHAR(1)   ENCODE lzo
	,currency_header_id NUMERIC(15,0)   ENCODE az64
	,pte_code VARCHAR(30)   ENCODE lzo
	,list_source_code VARCHAR(30)   ENCODE lzo
	,orig_system_header_ref VARCHAR(50)   ENCODE lzo
	,orig_org_id NUMERIC(15,0)   ENCODE az64
	,global_flag VARCHAR(1)   ENCODE lzo
	,shareable_flag VARCHAR(1)   ENCODE lzo
	,sold_to_org_id NUMERIC(15,0)   ENCODE az64
	,locked_from_list_header_id NUMERIC(15,0)   ENCODE az64	
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.QP_LIST_HEADERS_B (		
	list_header_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_type_code,
	start_date_active,
	end_date_active,
	automatic_flag,
	currency_code,
	rounding_factor,
	ship_method_code,
	freight_terms_code,
	terms_id,
	comments,
	discount_lines_flag,
	gsa_indicator,
	prorate_flag,
	source_system_code,
	ask_for_flag,
	active_flag,
	parent_list_header_id,
	start_date_active_first,
	end_date_active_first,
	active_date_first_type,
	start_date_active_second,
	end_date_active_second,
	active_date_second_type,
	context,
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
	limit_exists_flag,
	mobile_download,
	currency_header_id,
	pte_code,
	list_source_code,
	orig_system_header_ref,
	orig_org_id,
	global_flag,
	shareable_flag,
	sold_to_org_id,
	locked_from_list_header_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date

)
 SELECT
	list_header_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_type_code,
	start_date_active,
	end_date_active,
	automatic_flag,
	currency_code,
	rounding_factor,
	ship_method_code,
	freight_terms_code,
	terms_id,
	comments,
	discount_lines_flag,
	gsa_indicator,
	prorate_flag,
	source_system_code,
	ask_for_flag,
	active_flag,
	parent_list_header_id,
	start_date_active_first,
	end_date_active_first,
	active_date_first_type,
	start_date_active_second,
	end_date_active_second,
	active_date_second_type,
	context,
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
	limit_exists_flag,
	mobile_download,
	currency_header_id,
	pte_code,
	list_source_code,
	orig_system_header_ref,
	orig_org_id,
	global_flag,
	shareable_flag,
	sold_to_org_id,
	locked_from_list_header_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.QP_LIST_HEADERS_B;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'qp_list_headers_b';

COMMIT;