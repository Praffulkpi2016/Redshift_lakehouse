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

DROP TABLE if exists bec_ods.hz_cust_site_uses_all;

CREATE TABLE IF NOT EXISTS bec_ods.hz_cust_site_uses_all
(
	site_use_id NUMERIC(15,0)   ENCODE az64
	,cust_acct_site_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,site_use_code VARCHAR(30)   ENCODE lzo
	,primary_flag VARCHAR(1)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,"location" VARCHAR(40)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,contact_id NUMERIC(15,0)   ENCODE az64
	,bill_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,orig_system_reference VARCHAR(240)   ENCODE lzo
	,sic_code VARCHAR(30)   ENCODE lzo
	,payment_term_id NUMERIC(15,0)   ENCODE az64
	,gsa_indicator VARCHAR(1)   ENCODE lzo
	,ship_partial VARCHAR(1)   ENCODE lzo
	,ship_via VARCHAR(30)   ENCODE lzo
	,fob_point VARCHAR(30)   ENCODE lzo
	,order_type_id NUMERIC(15,0)   ENCODE az64
	,price_list_id NUMERIC(15,0)   ENCODE az64
	,freight_term VARCHAR(30)   ENCODE lzo
	,warehouse_id NUMERIC(15,0)   ENCODE az64
	,territory_id NUMERIC(15,0)   ENCODE az64
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
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tax_reference VARCHAR(50)   ENCODE lzo
	,sort_priority NUMERIC(5,0)   ENCODE az64
	,tax_code VARCHAR(50)   ENCODE lzo
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
	,attribute21 VARCHAR(150)   ENCODE lzo
	,attribute22 VARCHAR(150)   ENCODE lzo
	,attribute23 VARCHAR(150)   ENCODE lzo
	,attribute24 VARCHAR(150)   ENCODE lzo
	,attribute25 VARCHAR(150)   ENCODE lzo
	,last_accrue_charge_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,second_last_accrue_charge_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_unaccrue_charge_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,second_last_unaccrue_chrg_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,demand_class_code VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,tax_header_level_flag VARCHAR(1)   ENCODE lzo
	,tax_rounding_rule VARCHAR(30)   ENCODE lzo
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,primary_salesrep_id NUMERIC(15,0)   ENCODE az64
	,finchrg_receivables_trx_id NUMERIC(15,0)   ENCODE az64
	,dates_negative_tolerance NUMERIC(15,0)   ENCODE az64
	,dates_positive_tolerance NUMERIC(15,0)   ENCODE az64
	,date_type_preference VARCHAR(20)   ENCODE lzo
	,over_shipment_tolerance NUMERIC(15,0)   ENCODE az64
	,under_shipment_tolerance NUMERIC(15,0)   ENCODE az64
	,item_cross_ref_pref VARCHAR(30)   ENCODE lzo
	,over_return_tolerance NUMERIC(15,0)   ENCODE az64
	,under_return_tolerance NUMERIC(15,0)   ENCODE az64
	,ship_sets_include_lines_flag VARCHAR(1)   ENCODE lzo
	,arrivalsets_include_lines_flag VARCHAR(1)   ENCODE lzo
	,sched_date_push_flag VARCHAR(1)   ENCODE lzo
	,invoice_quantity_rule VARCHAR(30)   ENCODE lzo
	,pricing_event VARCHAR(30)   ENCODE lzo
	,gl_id_rec NUMERIC(15,0)   ENCODE az64
	,gl_id_rev NUMERIC(15,0)   ENCODE az64
	,gl_id_tax NUMERIC(15,0)   ENCODE az64
	,gl_id_freight NUMERIC(15,0)   ENCODE az64
	,gl_id_clearing NUMERIC(15,0)   ENCODE az64
	,gl_id_unbilled NUMERIC(15,0)   ENCODE az64
	,gl_id_unearned NUMERIC(15,0)   ENCODE az64
	,gl_id_unpaid_rec NUMERIC(15,0)   ENCODE az64
	,gl_id_remittance NUMERIC(15,0)   ENCODE az64
	,gl_id_factor NUMERIC(15,0)   ENCODE az64
	,tax_classification VARCHAR(30)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by_module VARCHAR(150)   ENCODE lzo
	,application_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert
	into
	bec_ods.hz_cust_site_uses_all
(	site_use_id,
	cust_acct_site_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	site_use_code,
	primary_flag,
	status,
	"location",
	last_update_login,
	contact_id,
	bill_to_site_use_id,
	orig_system_reference,
	sic_code,
	payment_term_id,
	gsa_indicator,
	ship_partial,
	ship_via,
	fob_point,
	order_type_id,
	price_list_id,
	freight_term,
	warehouse_id,
	territory_id,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	tax_reference,
	sort_priority,
	tax_code,
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
	attribute21,
	attribute22,
	attribute23,
	attribute24,
	attribute25,
	last_accrue_charge_date,
	second_last_accrue_charge_date,
	last_unaccrue_charge_date,
	second_last_unaccrue_chrg_date,
	demand_class_code,
	org_id,
	tax_header_level_flag,
	tax_rounding_rule,
	wh_update_date,
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
	primary_salesrep_id,
	finchrg_receivables_trx_id,
	dates_negative_tolerance,
	dates_positive_tolerance,
	date_type_preference,
	over_shipment_tolerance,
	under_shipment_tolerance,
	item_cross_ref_pref,
	over_return_tolerance,
	under_return_tolerance,
	ship_sets_include_lines_flag,
	arrivalsets_include_lines_flag,
	sched_date_push_flag,
	invoice_quantity_rule,
	pricing_event,
	gl_id_rec,
	gl_id_rev,
	gl_id_tax,
	gl_id_freight,
	gl_id_clearing,
	gl_id_unbilled,
	gl_id_unearned,
	gl_id_unpaid_rec,
	gl_id_remittance,
	gl_id_factor,
	tax_classification,
	object_version_number,
	created_by_module,
	application_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	site_use_id,
	cust_acct_site_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	site_use_code,
	primary_flag,
	status,
	"location",
	last_update_login,
	contact_id,
	bill_to_site_use_id,
	orig_system_reference,
	sic_code,
	payment_term_id,
	gsa_indicator,
	ship_partial,
	ship_via,
	fob_point,
	order_type_id,
	price_list_id,
	freight_term,
	warehouse_id,
	territory_id,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	tax_reference,
	sort_priority,
	tax_code,
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
	attribute21,
	attribute22,
	attribute23,
	attribute24,
	attribute25,
	last_accrue_charge_date,
	second_last_accrue_charge_date,
	last_unaccrue_charge_date,
	second_last_unaccrue_chrg_date,
	demand_class_code,
	org_id,
	tax_header_level_flag,
	tax_rounding_rule,
	wh_update_date,
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
	primary_salesrep_id,
	finchrg_receivables_trx_id,
	dates_negative_tolerance,
	dates_positive_tolerance,
	date_type_preference,
	over_shipment_tolerance,
	under_shipment_tolerance,
	item_cross_ref_pref,
	over_return_tolerance,
	under_return_tolerance,
	ship_sets_include_lines_flag,
	arrivalsets_include_lines_flag,
	sched_date_push_flag,
	invoice_quantity_rule,
	pricing_event,
	gl_id_rec,
	gl_id_rev,
	gl_id_tax,
	gl_id_freight,
	gl_id_clearing,
	gl_id_unbilled,
	gl_id_unearned,
	gl_id_unpaid_rec,
	gl_id_remittance,
	gl_id_factor,
	tax_classification,
	object_version_number,
	created_by_module,
	application_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.hz_cust_site_uses_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_cust_site_uses_all';
	
commit;
	
