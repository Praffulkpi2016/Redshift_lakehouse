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

delete from bec_ods.OKC_K_HEADERS_ALL_B
where ID in (
select stg.ID
from bec_ods.OKC_K_HEADERS_ALL_B ods, bec_ods_stg.OKC_K_HEADERS_ALL_B stg
where ods.ID = stg.ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OKC_K_HEADERS_ALL_B
       (id,
	contract_number,
	authoring_org_id,
	contract_number_modifier,
	chr_id_response,
	chr_id_award,
	chr_id_renewed,
	inv_organization_id,
	sts_code,
	qcl_id,
	scs_code,
	trn_code,
	currency_code,
	archived_yn,
	deleted_yn,
	template_yn,
	chr_type,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	cust_po_number_req_yn,
	pre_pay_req_yn,
	cust_po_number,
	dpas_rating,
	template_used,
	date_approved,
	datetime_cancelled,
	auto_renew_days,
	date_issued,
	datetime_responded,
	rfp_type,
	keep_on_mail_list,
	set_aside_percent,
	response_copies_req,
	date_close_projected,
	datetime_proposed,
	date_signed,
	date_terminated,
	date_renewed,
	start_date,
	end_date,
	buy_or_sell,
	issue_or_receive,
	last_update_login,
	estimated_amount,
	attribute_category,
	last_update_date,
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
	security_group_id,
	chr_id_renewed_to,
	estimated_amount_renewed,
	currency_code_renewed,
	upg_orig_system_ref,
	upg_orig_system_ref_id,
	application_id,
	resolved_until,
	orig_system_source_code,
	orig_system_id1,
	orig_system_reference1,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	price_list_id,
	pricing_date,
	total_line_list_price,
	sign_by_date,
	user_estimated_amount,
	governing_contract_yn,
	conversion_type,
	conversion_rate,
	conversion_rate_date,
	conversion_euro_rate,
	cust_acct_id,
	bill_to_site_use_id,
	inv_rule_id,
	renewal_type_code,
	renewal_notify_to,
	renewal_end_date,
	ship_to_site_use_id,
	payment_term_id,
	document_id,
	approval_type,
	term_cancel_source,
	payment_instruction_type,
	org_id,
	cancelled_amount,
	billed_at_source,
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
	attribute26,
	attribute27,
	attribute28,
	attribute29,
	attribute30,
	attribute31,
	attribute32,
	attribute33,
	attribute34,
	attribute35,
	attribute36,
	attribute37,
	attribute38,
	attribute39,
	attribute40,
	attribute41,
	attribute42,
	attribute43,
	attribute44,
	attribute45,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
	id,
	contract_number,
	authoring_org_id,
	contract_number_modifier,
	chr_id_response,
	chr_id_award,
	chr_id_renewed,
	inv_organization_id,
	sts_code,
	qcl_id,
	scs_code,
	trn_code,
	currency_code,
	archived_yn,
	deleted_yn,
	template_yn,
	chr_type,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	cust_po_number_req_yn,
	pre_pay_req_yn,
	cust_po_number,
	dpas_rating,
	template_used,
	date_approved,
	datetime_cancelled,
	auto_renew_days,
	date_issued,
	datetime_responded,
	rfp_type,
	keep_on_mail_list,
	set_aside_percent,
	response_copies_req,
	date_close_projected,
	datetime_proposed,
	date_signed,
	date_terminated,
	date_renewed,
	start_date,
	end_date,
	buy_or_sell,
	issue_or_receive,
	last_update_login,
	estimated_amount,
	attribute_category,
	last_update_date,
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
	security_group_id,
	chr_id_renewed_to,
	estimated_amount_renewed,
	currency_code_renewed,
	upg_orig_system_ref,
	upg_orig_system_ref_id,
	application_id,
	resolved_until,
	orig_system_source_code,
	orig_system_id1,
	orig_system_reference1,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	price_list_id,
	pricing_date,
	total_line_list_price,
	sign_by_date,
	user_estimated_amount,
	governing_contract_yn,
	conversion_type,
	conversion_rate,
	conversion_rate_date,
	conversion_euro_rate,
	cust_acct_id,
	bill_to_site_use_id,
	inv_rule_id,
	renewal_type_code,
	renewal_notify_to,
	renewal_end_date,
	ship_to_site_use_id,
	payment_term_id,
	document_id,
	approval_type,
	term_cancel_source,
	payment_instruction_type,
	org_id,
	cancelled_amount,
	billed_at_source,
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
	attribute26,
	attribute27,
	attribute28,
	attribute29,
	attribute30,
	attribute31,
	attribute32,
	attribute33,
	attribute34,
	attribute35,
	attribute36,
	attribute37,
	attribute38,
	attribute39,
	attribute40,
	attribute41,
	attribute42,
	attribute43,
	attribute44,
	attribute45,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.OKC_K_HEADERS_ALL_B
	where kca_operation in ('INSERT','UPDATE') 
	and (ID,kca_seq_id) in 
	(select ID, max(kca_seq_id) from bec_ods_stg.OKC_K_HEADERS_ALL_B 
     where kca_operation in ('INSERT','UPDATE')
     group by ID)
);

commit;



-- Soft delete
update bec_ods.OKC_K_HEADERS_ALL_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKC_K_HEADERS_ALL_B set IS_DELETED_FLG = 'Y'
where (nvl(ID,0))  in
(
select nvl(ID,0) from bec_raw_dl_ext.OKC_K_HEADERS_ALL_B
where (nvl(ID,0),KCA_SEQ_ID)
in 
(
select nvl(ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKC_K_HEADERS_ALL_B
group by nvl(ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'okc_k_headers_all_b';

commit;