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

delete from bec_ods.RA_CUST_TRX_TYPES_ALL
where (CUST_TRX_TYPE_ID,nvl(org_id, 0)) in 
(
select stg.CUST_TRX_TYPE_ID,nvl(stg.org_id,0) as org_id
from bec_ods.ra_cust_trx_types_all ods, bec_ods_stg.ra_cust_trx_types_all stg
where ods.CUST_TRX_TYPE_ID = stg.CUST_TRX_TYPE_ID
AND nvl(ods.org_id,0) = nvl(stg.org_id,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

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
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE
    FROM
        bec_ods_stg.ra_cust_trx_types_all
	where kca_operation in ('INSERT','UPDATE') 
	and (CUST_TRX_TYPE_ID,nvl(org_id, 0),kca_seq_id) in 
	(
	select CUST_TRX_TYPE_ID,nvl(org_id, 0),max(kca_seq_id) 
	from bec_ods_stg.ra_cust_trx_types_all 
     where kca_operation in ('INSERT','UPDATE')
     group by CUST_TRX_TYPE_ID,org_id
	 );

commit;



-- Soft delete
update bec_ods.ra_cust_trx_types_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ra_cust_trx_types_all set IS_DELETED_FLG = 'Y'
where (CUST_TRX_TYPE_ID,nvl(org_id, 0))  in
(
select CUST_TRX_TYPE_ID,nvl(org_id, 0) from bec_raw_dl_ext.ra_cust_trx_types_all
where (CUST_TRX_TYPE_ID,nvl(org_id, 0),KCA_SEQ_ID)
in 
(
select CUST_TRX_TYPE_ID,nvl(org_id, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ra_cust_trx_types_all
group by CUST_TRX_TYPE_ID,nvl(org_id, 0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ra_cust_trx_types_all';

commit;