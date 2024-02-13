/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.hz_cust_site_uses_all
where (SITE_USE_ID) in (
select stg.SITE_USE_ID from bec_ods.hz_cust_site_uses_all ods, bec_ods_stg.hz_cust_site_uses_all stg
where ods.SITE_USE_ID = stg.SITE_USE_ID  and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

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
        bec_ods_stg.hz_cust_site_uses_all
	where kca_operation IN ('INSERT','UPDATE') 
	and (SITE_USE_ID,kca_seq_id) in 
	(select SITE_USE_ID,max(kca_seq_id) from bec_ods_stg.hz_cust_site_uses_all 
     where kca_operation IN ('INSERT','UPDATE')
     group by SITE_USE_ID);

commit;
 

-- Soft delete
update bec_ods.hz_cust_site_uses_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.hz_cust_site_uses_all set IS_DELETED_FLG = 'Y'
where (SITE_USE_ID)  in
(
select SITE_USE_ID from bec_raw_dl_ext.hz_cust_site_uses_all
where (SITE_USE_ID,KCA_SEQ_ID)
in 
(
select SITE_USE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.hz_cust_site_uses_all
group by SITE_USE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hz_cust_site_uses_all';

commit;