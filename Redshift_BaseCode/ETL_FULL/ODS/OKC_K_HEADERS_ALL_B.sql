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

DROP TABLE if exists bec_ods.OKC_K_HEADERS_ALL_B;

CREATE TABLE IF NOT EXISTS bec_ods.OKC_K_HEADERS_ALL_B
(
	id NUMERIC(15,0)   ENCODE az64
	,contract_number VARCHAR(120)   ENCODE lzo
	,authoring_org_id NUMERIC(15,0)   ENCODE az64
	,contract_number_modifier VARCHAR(120)   ENCODE lzo
	,chr_id_response NUMERIC(15,0)   ENCODE az64
	,chr_id_award NUMERIC(15,0)   ENCODE az64
	,chr_id_renewed NUMERIC(15,0)   ENCODE az64
	,inv_organization_id NUMERIC(15,0)   ENCODE az64
	,sts_code VARCHAR(30)   ENCODE lzo
	,qcl_id NUMERIC(15,0)   ENCODE az64
	,scs_code VARCHAR(30)   ENCODE lzo
	,trn_code VARCHAR(30)   ENCODE lzo
	,currency_code VARCHAR(15)   ENCODE lzo
	,archived_yn VARCHAR(3)   ENCODE lzo
	,deleted_yn VARCHAR(3)   ENCODE lzo
	,template_yn VARCHAR(3)   ENCODE lzo
	,chr_type VARCHAR(30)   ENCODE lzo
	,object_version_number NUMERIC(9,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,cust_po_number_req_yn VARCHAR(3)   ENCODE lzo
	,pre_pay_req_yn VARCHAR(3)   ENCODE lzo
	,cust_po_number VARCHAR(150)   ENCODE lzo
	,dpas_rating VARCHAR(24)   ENCODE lzo
	,template_used VARCHAR(120)   ENCODE lzo
	,date_approved TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,datetime_cancelled TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,auto_renew_days NUMERIC(4,0)   ENCODE az64
	,date_issued TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,datetime_responded TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,rfp_type VARCHAR(30)   ENCODE lzo
	,keep_on_mail_list VARCHAR(3)   ENCODE lzo
	,set_aside_percent NUMERIC(5,2)   ENCODE az64
	,response_copies_req NUMERIC(2,0)   ENCODE az64
	,date_close_projected TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,datetime_proposed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_signed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_terminated TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_renewed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,buy_or_sell VARCHAR(3)   ENCODE lzo
	,issue_or_receive VARCHAR(3)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,estimated_amount NUMERIC(28,10)   ENCODE az64
	,attribute_category VARCHAR(90)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute1 VARCHAR(450)   ENCODE lzo
	,attribute2 VARCHAR(450)   ENCODE lzo
	,attribute3 VARCHAR(450)   ENCODE lzo
	,attribute4 VARCHAR(450)   ENCODE lzo
	,attribute5 VARCHAR(450)   ENCODE lzo
	,attribute6 VARCHAR(450)   ENCODE lzo
	,attribute7 VARCHAR(450)   ENCODE lzo
	,attribute8 VARCHAR(450)   ENCODE lzo
	,attribute9 VARCHAR(450)   ENCODE lzo
	,attribute10 VARCHAR(450)   ENCODE lzo
	,attribute11 VARCHAR(450)   ENCODE lzo
	,attribute12 VARCHAR(450)   ENCODE lzo
	,attribute13 VARCHAR(450)   ENCODE lzo
	,attribute14 VARCHAR(450)   ENCODE lzo
	,attribute15 VARCHAR(450)   ENCODE lzo
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,chr_id_renewed_to NUMERIC(15,0)   ENCODE az64
	,estimated_amount_renewed NUMERIC(28,10)   ENCODE az64
	,currency_code_renewed VARCHAR(15)   ENCODE lzo
	,upg_orig_system_ref VARCHAR(60)   ENCODE lzo
	,upg_orig_system_ref_id NUMERIC(15,0)   ENCODE az64
	,application_id NUMERIC(15,0)   ENCODE az64
	,resolved_until TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,orig_system_source_code VARCHAR(30)   ENCODE lzo
	,orig_system_id1 NUMERIC(15,0)   ENCODE az64
	,orig_system_reference1 VARCHAR(30)   ENCODE lzo
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,price_list_id NUMERIC(15,0)   ENCODE az64
	,pricing_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,total_line_list_price NUMERIC(28,10)   ENCODE az64
	,sign_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,user_estimated_amount NUMERIC(28,10)   ENCODE az64
	,governing_contract_yn VARCHAR(3)   ENCODE lzo
	,conversion_type VARCHAR(30)   ENCODE lzo
	,conversion_rate NUMERIC(28,10)   ENCODE az64
	,conversion_rate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,conversion_euro_rate NUMERIC(28,10)   ENCODE az64
	,cust_acct_id NUMERIC(15,0)   ENCODE az64
	,bill_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,inv_rule_id NUMERIC(15,0)   ENCODE az64
	,renewal_type_code VARCHAR(30)   ENCODE lzo
	,renewal_notify_to NUMERIC(28,10)   ENCODE az64
	,renewal_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ship_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,payment_term_id NUMERIC(15,0)   ENCODE az64
	,document_id NUMERIC(15,0)   ENCODE az64
	,approval_type VARCHAR(30)   ENCODE lzo
	,term_cancel_source VARCHAR(30)   ENCODE lzo
	,payment_instruction_type VARCHAR(3)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,cancelled_amount NUMERIC(28,10)   ENCODE az64
	,billed_at_source VARCHAR(1)   ENCODE lzo
	,attribute16 VARCHAR(450)   ENCODE lzo
	,attribute17 VARCHAR(450)   ENCODE lzo
	,attribute18 VARCHAR(450)   ENCODE lzo
	,attribute19 VARCHAR(450)   ENCODE lzo
	,attribute20 VARCHAR(450)   ENCODE lzo
	,attribute21 VARCHAR(450)   ENCODE lzo
	,attribute22 VARCHAR(450)   ENCODE lzo
	,attribute23 VARCHAR(450)   ENCODE lzo
	,attribute24 VARCHAR(450)   ENCODE lzo
	,attribute25 VARCHAR(450)   ENCODE lzo
	,attribute26 VARCHAR(450)   ENCODE lzo
	,attribute27 VARCHAR(450)   ENCODE lzo
	,attribute28 VARCHAR(450)   ENCODE lzo
	,attribute29 VARCHAR(450)   ENCODE lzo
	,attribute30 VARCHAR(450)   ENCODE lzo
	,attribute31 VARCHAR(450)   ENCODE lzo
	,attribute32 VARCHAR(450)   ENCODE lzo
	,attribute33 VARCHAR(450)   ENCODE lzo
	,attribute34 VARCHAR(450)   ENCODE lzo
	,attribute35 VARCHAR(450)   ENCODE lzo
	,attribute36 VARCHAR(450)   ENCODE lzo
	,attribute37 VARCHAR(450)   ENCODE lzo
	,attribute38 VARCHAR(450)   ENCODE lzo
	,attribute39 VARCHAR(450)   ENCODE lzo
	,attribute40 VARCHAR(450)   ENCODE lzo
	,attribute41 VARCHAR(450)   ENCODE lzo
	,attribute42 VARCHAR(450)   ENCODE lzo
	,attribute43 VARCHAR(450)   ENCODE lzo
	,attribute44 VARCHAR(450)   ENCODE lzo
	,attribute45 VARCHAR(450)   ENCODE lzo
	,kca_operation VARCHAR(150)   ENCODE lzo
	,is_deleted_flg VARCHAR(2)   ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE lzo
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OKC_K_HEADERS_ALL_B (
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
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
	)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.OKC_K_HEADERS_ALL_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'okc_k_headers_all_b';
	
commit;