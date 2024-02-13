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

DROP TABLE if exists bec_ods.AR_CUSTOMERS;

CREATE TABLE IF NOT EXISTS bec_ods.AR_CUSTOMERS
(
	 customer_id NUMERIC(15,0)   ENCODE az64
	,customer_name VARCHAR(50)   ENCODE lzo
	,customer_number VARCHAR(30)   ENCODE lzo
	,customer_key VARCHAR(500)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,orig_system_reference VARCHAR(240)   ENCODE lzo
	,customer_prospect_code VARCHAR(8)   ENCODE lzo
	,customer_category_code VARCHAR(30)   ENCODE lzo
	,customer_class_code VARCHAR(30)   ENCODE lzo
	,customer_type VARCHAR(30)   ENCODE lzo
	,primary_salesrep_id NUMERIC(15,0)   ENCODE az64
	,sic_code VARCHAR(30)   ENCODE lzo
	,tax_reference VARCHAR(50)   ENCODE lzo
	,tax_code VARCHAR(50)   ENCODE lzo
	,fob_point VARCHAR(30)   ENCODE lzo
	,ship_via VARCHAR(30)   ENCODE lzo
	,gsa_indicator VARCHAR(1)   ENCODE lzo
	,ship_partial VARCHAR(1)   ENCODE lzo
	,taxpayer_id VARCHAR(20)   ENCODE lzo
	,price_list_id NUMERIC(15,0)   ENCODE az64
	,freight_term VARCHAR(30)   ENCODE lzo
	,order_type_id NUMERIC(15,0)   ENCODE az64
	,sales_channel_code VARCHAR(30)   ENCODE lzo
	,warehouse_id NUMERIC(15,0)   ENCODE az64
	,mission_statement VARCHAR(2000)   ENCODE lzo
	,num_of_employees NUMERIC(15,0)   ENCODE az64
	,potential_revenue_curr_fy NUMERIC(28,10)   ENCODE az64
	,potential_revenue_next_fy NUMERIC(28,10)   ENCODE az64
	,fiscal_yearend_month VARCHAR(30)   ENCODE lzo
	,year_established NUMERIC(15,0)   ENCODE az64
	,analysis_fy VARCHAR(5)   ENCODE lzo
	,competitor_flag VARCHAR(1)   ENCODE lzo
	,reference_use_flag VARCHAR(1)   ENCODE lzo
	,third_party_flag VARCHAR(1)   ENCODE lzo
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
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,customer_name_phonetic VARCHAR(320)   ENCODE lzo
	,tax_header_level_flag VARCHAR(1)   ENCODE lzo
	,tax_rounding_rule VARCHAR(30)   ENCODE lzo
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
	,global_attribute20 VARCHAR(150)   ENCODE
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.AR_CUSTOMERS (
   customer_id,
	customer_name,
	customer_number,
	customer_key,
	status,
	orig_system_reference,
	customer_prospect_code,
	customer_category_code,
	customer_class_code,
	customer_type,
	primary_salesrep_id,
	sic_code,
	tax_reference,
	tax_code,
	fob_point,
	ship_via,
	gsa_indicator,
	ship_partial,
	taxpayer_id,
	price_list_id,
	freight_term,
	order_type_id,
	sales_channel_code,
	warehouse_id,
	mission_statement,
	num_of_employees,
	potential_revenue_curr_fy,
	potential_revenue_next_fy,
	fiscal_yearend_month,
	year_established,
	analysis_fy,
	competitor_flag,
	reference_use_flag,
	third_party_flag,
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
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	customer_name_phonetic,
	tax_header_level_flag,
	tax_rounding_rule,
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
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
      customer_id,
	customer_name,
	customer_number,
	customer_key,
	status,
	orig_system_reference,
	customer_prospect_code,
	customer_category_code,
	customer_class_code,
	customer_type,
	primary_salesrep_id,
	sic_code,
	tax_reference,
	tax_code,
	fob_point,
	ship_via,
	gsa_indicator,
	ship_partial,
	taxpayer_id,
	price_list_id,
	freight_term,
	order_type_id,
	sales_channel_code,
	warehouse_id,
	mission_statement,
	num_of_employees,
	potential_revenue_curr_fy,
	potential_revenue_next_fy,
	fiscal_yearend_month,
	year_established,
	analysis_fy,
	competitor_flag,
	reference_use_flag,
	third_party_flag,
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
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	customer_name_phonetic,
	tax_header_level_flag,
	tax_rounding_rule,
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
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.AR_CUSTOMERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_customers';
	
commit;