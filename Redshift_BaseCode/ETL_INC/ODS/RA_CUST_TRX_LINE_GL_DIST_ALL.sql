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

delete from bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL
where CUST_TRX_LINE_GL_DIST_ID in (
select stg.CUST_TRX_LINE_GL_DIST_ID from bec_ods.ra_cust_trx_line_gl_dist_all ods, bec_ods_stg.ra_cust_trx_line_gl_dist_all stg
where ods.CUST_TRX_LINE_GL_DIST_ID = stg.CUST_TRX_LINE_GL_DIST_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.ra_cust_trx_line_gl_dist_all
(	cust_trx_line_gl_dist_id,
	customer_trx_line_id,
	code_combination_id,
	set_of_books_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	"percent",
	amount,
	gl_date,
	gl_posted_date,
	cust_trx_line_salesrep_id,
	comments,
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
	concatenated_segments,
	original_gl_date,
	post_request_id,
	posting_control_id,
	account_class,
	ra_post_loop_number,
	customer_trx_id,
	account_set_flag,
	acctd_amount,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	latest_rec_flag,
	org_id,
	mrc_account_class,
	mrc_customer_trx_id,
	mrc_amount,
	mrc_gl_posted_date,
	mrc_posting_control_id,
	mrc_acctd_amount,
	collected_tax_ccid,
	collected_tax_concat_seg,
	revenue_adjustment_id,
	rev_adj_class_temp,
	rec_offset_flag,
	event_id,
	user_generated_flag,
	rounding_correction_flag,
	cogs_request_id,
	ccid_change_flag,
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
	global_attribute21,
	global_attribute22,
	global_attribute23,
	global_attribute24,
	global_attribute25,
	global_attribute26,
	global_attribute27,
	global_attribute28,
	global_attribute29,
	global_attribute30,
	global_attribute_category,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)
	SELECT
	cust_trx_line_gl_dist_id,
	customer_trx_line_id,
	code_combination_id,
	set_of_books_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	"percent",
	amount,
	gl_date,
	gl_posted_date,
	cust_trx_line_salesrep_id,
	comments,
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
	concatenated_segments,
	original_gl_date,
	post_request_id,
	posting_control_id,
	account_class,
	ra_post_loop_number,
	customer_trx_id,
	account_set_flag,
	acctd_amount,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	latest_rec_flag,
	org_id,
	mrc_account_class,
	mrc_customer_trx_id,
	mrc_amount,
	mrc_gl_posted_date,
	mrc_posting_control_id,
	mrc_acctd_amount,
	collected_tax_ccid,
	collected_tax_concat_seg,
	revenue_adjustment_id,
	rev_adj_class_temp,
	rec_offset_flag,
	event_id,
	user_generated_flag,
	rounding_correction_flag,
	cogs_request_id,
	ccid_change_flag,
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
	global_attribute21,
	global_attribute22,
	global_attribute23,
	global_attribute24,
	global_attribute25,
	global_attribute26,
	global_attribute27,
	global_attribute28,
	global_attribute29,
	global_attribute30,
	global_attribute_category,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE
    FROM
        bec_ods_stg.ra_cust_trx_line_gl_dist_all
	where kca_operation in ('INSERT','UPDATE') 
	and (CUST_TRX_LINE_GL_DIST_ID,kca_seq_id) in 
	(select CUST_TRX_LINE_GL_DIST_ID,max(kca_seq_id) from bec_ods_stg.ra_cust_trx_line_gl_dist_all 
     where kca_operation in ('INSERT','UPDATE')
     group by CUST_TRX_LINE_GL_DIST_ID);

commit;



-- Soft delete
update bec_ods.ra_cust_trx_line_gl_dist_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ra_cust_trx_line_gl_dist_all set IS_DELETED_FLG = 'Y'
where (CUST_TRX_LINE_GL_DIST_ID)  in
(
select CUST_TRX_LINE_GL_DIST_ID from bec_raw_dl_ext.ra_cust_trx_line_gl_dist_all
where (CUST_TRX_LINE_GL_DIST_ID,KCA_SEQ_ID)
in 
(
select CUST_TRX_LINE_GL_DIST_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ra_cust_trx_line_gl_dist_all
group by CUST_TRX_LINE_GL_DIST_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ra_cust_trx_line_gl_dist_all';

commit;