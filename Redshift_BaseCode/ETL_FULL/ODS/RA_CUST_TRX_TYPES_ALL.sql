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

DROP TABLE if exists bec_ods.ra_cust_trx_types_all;

CREATE TABLE IF NOT EXISTS bec_ods.ra_cust_trx_types_all
(
	cust_trx_type_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,post_to_gl VARCHAR(1)   ENCODE lzo
	,accounting_affect_flag VARCHAR(1)   ENCODE lzo
	,credit_memo_type_id NUMERIC(15,0)   ENCODE az64
	,status VARCHAR(30)   ENCODE lzo
	,name VARCHAR(20)   ENCODE lzo
	,description VARCHAR(80)   ENCODE lzo
	,"type" VARCHAR(20)   ENCODE lzo
	,default_term NUMERIC(15,0)   ENCODE az64
	,default_printing_option VARCHAR(20)   ENCODE lzo
	,default_status VARCHAR(30)   ENCODE lzo
	,gl_id_rev NUMERIC(15,0)   ENCODE az64
	,gl_id_freight NUMERIC(15,0)   ENCODE az64
	,gl_id_rec NUMERIC(15,0)   ENCODE az64
	,subsequent_trx_type_id NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
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
	,allow_freight_flag VARCHAR(1)   ENCODE lzo
	,allow_overapplication_flag VARCHAR(1)   ENCODE lzo
	,creation_sign VARCHAR(30)   ENCODE lzo
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,gl_id_clearing NUMERIC(15,0)   ENCODE az64
	,gl_id_tax NUMERIC(15,0)   ENCODE az64
	,gl_id_unbilled NUMERIC(15,0)   ENCODE az64
	,gl_id_unearned NUMERIC(15,0)   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tax_calculation_flag VARCHAR(1)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,natural_application_only_flag VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
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
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,rule_set_id NUMERIC(15,0)   ENCODE az64
	,signed_flag VARCHAR(1)   ENCODE lzo
	,drawee_issued_flag VARCHAR(1)   ENCODE lzo
	,magnetic_format_code VARCHAR(30)   ENCODE lzo
	,format_program_id NUMERIC(15,0)   ENCODE az64
	,gl_id_unpaid_rec NUMERIC(15,0)   ENCODE az64
	,gl_id_remittance NUMERIC(15,0)   ENCODE az64
	,gl_id_factor NUMERIC(15,0)   ENCODE az64
	,allocate_tax_freight VARCHAR(1)   ENCODE lzo
	,legal_entity_id NUMERIC(15,0)   ENCODE az64
	,exclude_from_late_charges VARCHAR(30)   ENCODE lzo
	,adj_post_to_gl VARCHAR(1)   ENCODE lzo
	,ZD_EDITION_NAME VARCHAR(30)   ENCODE lzo 
	,ZD_SYNC VARCHAR(30)   ENCODE lzo	
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
insert
	into
	bec_ods.ra_cust_trx_types_all
(	cust_trx_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	post_to_gl,
	accounting_affect_flag,
	credit_memo_type_id,
	status,
	"name",
	description,
	"type",
	default_term,
	default_printing_option,
	default_status,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	subsequent_trx_type_id,
	set_of_books_id,
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
	allow_freight_flag,
	allow_overapplication_flag,
	creation_sign,
	end_date,
	gl_id_clearing,
	gl_id_tax,
	gl_id_unbilled,
	gl_id_unearned,
	start_date,
	tax_calculation_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	natural_application_only_flag,
	org_id,
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
	global_attribute_category,
	rule_set_id,
	signed_flag,
	drawee_issued_flag,
	magnetic_format_code,
	format_program_id,
	gl_id_unpaid_rec,
	gl_id_remittance,
	gl_id_factor,
	allocate_tax_freight,
	legal_entity_id,
	exclude_from_late_charges,
	adj_post_to_gl,
	ZD_EDITION_NAME, 
	ZD_SYNC,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)
	SELECT
	cust_trx_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	post_to_gl,
	accounting_affect_flag,
	credit_memo_type_id,
	status,
	"name",
	description,
	"type",
	default_term,
	default_printing_option,
	default_status,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	subsequent_trx_type_id,
	set_of_books_id,
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
	allow_freight_flag,
	allow_overapplication_flag,
	creation_sign,
	end_date,
	gl_id_clearing,
	gl_id_tax,
	gl_id_unbilled,
	gl_id_unearned,
	start_date,
	tax_calculation_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	natural_application_only_flag,
	org_id,
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
	global_attribute_category,
	rule_set_id,
	signed_flag,
	drawee_issued_flag,
	magnetic_format_code,
	format_program_id,
	gl_id_unpaid_rec,
	gl_id_remittance,
	gl_id_factor,
	allocate_tax_freight,
	legal_entity_id,
	exclude_from_late_charges,
	adj_post_to_gl,
	ZD_EDITION_NAME, 
	ZD_SYNC,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.ra_cust_trx_types_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_cust_trx_types_all';
	
commit;
	
