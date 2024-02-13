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

DROP TABLE if exists bec_ods.ra_cust_trx_line_gl_dist_all;

CREATE TABLE IF NOT EXISTS bec_ods.ra_cust_trx_line_gl_dist_all
(
	cust_trx_line_gl_dist_id NUMERIC(15,0)   ENCODE az64
	,customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,code_combination_id NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,"percent" NUMERIC(28,10)   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,gl_posted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cust_trx_line_salesrep_id NUMERIC(15,0)   ENCODE az64
	,comments VARCHAR(240)   ENCODE lzo
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
	,concatenated_segments VARCHAR(240)   ENCODE lzo
	,original_gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,post_request_id NUMERIC(15,0)   ENCODE az64
	,posting_control_id NUMERIC(15,0)   ENCODE az64
	,account_class VARCHAR(20)   ENCODE lzo
	,ra_post_loop_number NUMERIC(15,0)   ENCODE az64
	,customer_trx_id NUMERIC(15,0)   ENCODE az64
	,account_set_flag VARCHAR(1)   ENCODE lzo
	,acctd_amount NUMERIC(28,10)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_transaction_code_context VARCHAR(30)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,latest_rec_flag VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,mrc_account_class VARCHAR(2000)   ENCODE lzo
	,mrc_customer_trx_id VARCHAR(2000)   ENCODE lzo
	,mrc_amount VARCHAR(2000)   ENCODE lzo
	,mrc_gl_posted_date VARCHAR(2000)   ENCODE lzo
	,mrc_posting_control_id VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_amount VARCHAR(2000)   ENCODE lzo
	,collected_tax_ccid NUMERIC(15,0)   ENCODE az64
	,collected_tax_concat_seg VARCHAR(240)   ENCODE lzo
	,revenue_adjustment_id NUMERIC(15,0)   ENCODE az64
	,rev_adj_class_temp VARCHAR(30)   ENCODE lzo
	,rec_offset_flag VARCHAR(1)   ENCODE lzo
	,event_id NUMERIC(15,0)   ENCODE az64
	,user_generated_flag VARCHAR(1)   ENCODE lzo
	,rounding_correction_flag VARCHAR(1)   ENCODE lzo
	,cogs_request_id NUMERIC(15,0)   ENCODE az64
	,ccid_change_flag VARCHAR(1)   ENCODE lzo
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
	,global_attribute21 VARCHAR(150)   ENCODE lzo
	,global_attribute22 VARCHAR(150)   ENCODE lzo
	,global_attribute23 VARCHAR(150)   ENCODE lzo
	,global_attribute24 VARCHAR(150)   ENCODE lzo
	,global_attribute25 VARCHAR(150)   ENCODE lzo
	,global_attribute26 VARCHAR(150)   ENCODE lzo
	,global_attribute27 VARCHAR(150)   ENCODE lzo
	,global_attribute28 VARCHAR(150)   ENCODE lzo
	,global_attribute29 VARCHAR(150)   ENCODE lzo
	,global_attribute30 VARCHAR(150)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
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
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.ra_cust_trx_line_gl_dist_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_cust_trx_line_gl_dist_all';
	
commit;
	
