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

DROP TABLE if exists bec_ods.AR_ADJUSTMENTS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.AR_ADJUSTMENTS_ALL
(
	adjustment_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,apply_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,code_combination_id NUMERIC(15,0)   ENCODE az64
	,"type" VARCHAR(15)   ENCODE lzo
	,adjustment_type VARCHAR(3)   ENCODE lzo
	,status VARCHAR(30)   ENCODE lzo
	,line_adjusted NUMERIC(28,10)   ENCODE az64
	,freight_adjusted NUMERIC(15,0)   ENCODE az64
	,tax_adjusted NUMERIC(28,10)   ENCODE az64
	,receivables_charges_adjusted NUMERIC(28,10)   ENCODE az64
	,associated_cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,chargeback_customer_trx_id NUMERIC(15,0)   ENCODE az64
	,batch_id NUMERIC(15,0)   ENCODE az64
	,customer_trx_id NUMERIC(15,0)   ENCODE az64
	,customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,subsequent_trx_id NUMERIC(15,0)   ENCODE az64
	,payment_schedule_id NUMERIC(15,0)   ENCODE az64
	,receivables_trx_id NUMERIC(15,0)   ENCODE az64
	,distribution_set_id NUMERIC(15,0)   ENCODE az64
	,gl_posted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,comments VARCHAR(2000)   ENCODE lzo
	,automatically_generated VARCHAR(1)   ENCODE lzo
	,created_from VARCHAR(30)   ENCODE lzo
	,reason_code VARCHAR(30)   ENCODE lzo
	,postable VARCHAR(1)   ENCODE lzo
	,approved_by NUMERIC(15,0)   ENCODE az64
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
	,posting_control_id NUMERIC(15,0)   ENCODE az64
	,acctd_amount NUMERIC(28,10)   ENCODE az64
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,adjustment_number VARCHAR(20)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_transaction_code_context VARCHAR(30)   ENCODE lzo
	,doc_sequence_value NUMERIC(15,0)   ENCODE az64
	,doc_sequence_id NUMERIC(15,0)   ENCODE az64
	,associated_application_id NUMERIC(15,0)   ENCODE az64
	,cons_inv_id NUMERIC(15,0)   ENCODE az64
	,mrc_gl_posted_date VARCHAR(2000)   ENCODE lzo
	,mrc_posting_control_id VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_amount VARCHAR(2000)   ENCODE lzo
	,adj_tax_acct_rule VARCHAR(3)   ENCODE lzo
	,global_attribute_category VARCHAR(150)   ENCODE lzo
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
	,link_to_trx_hist_id NUMERIC(15,0)   ENCODE az64
	,event_id NUMERIC(15,0)   ENCODE az64
	,upgrade_method VARCHAR(30)   ENCODE lzo
	,ax_accounted_flag VARCHAR(1)   ENCODE lzo
	,interest_header_id NUMERIC(15,0)   ENCODE az64
	,interest_line_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ar_adjustments_all (
    adjustment_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	amount,
	apply_date,
	gl_date,
	set_of_books_id,
	code_combination_id,
	"type",
	adjustment_type,
	status,
	line_adjusted,
	freight_adjusted,
	tax_adjusted,
	receivables_charges_adjusted,
	associated_cash_receipt_id,
	chargeback_customer_trx_id,
	batch_id,
	customer_trx_id,
	customer_trx_line_id,
	subsequent_trx_id,
	payment_schedule_id,
	receivables_trx_id,
	distribution_set_id,
	gl_posted_date,
	comments,
	automatically_generated,
	created_from,
	reason_code,
	postable,
	approved_by,
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
	posting_control_id,
	acctd_amount,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	adjustment_number,
	org_id,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	doc_sequence_value,
	doc_sequence_id,
	associated_application_id,
	cons_inv_id,
	mrc_gl_posted_date,
	mrc_posting_control_id,
	mrc_acctd_amount,
	adj_tax_acct_rule,
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
	link_to_trx_hist_id,
	event_id,
	upgrade_method,
	ax_accounted_flag,
	interest_header_id,
	interest_line_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        adjustment_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	amount,
	apply_date,
	gl_date,
	set_of_books_id,
	code_combination_id,
	"type",
	adjustment_type,
	status,
	line_adjusted,
	freight_adjusted,
	tax_adjusted,
	receivables_charges_adjusted,
	associated_cash_receipt_id,
	chargeback_customer_trx_id,
	batch_id,
	customer_trx_id,
	customer_trx_line_id,
	subsequent_trx_id,
	payment_schedule_id,
	receivables_trx_id,
	distribution_set_id,
	gl_posted_date,
	comments,
	automatically_generated,
	created_from,
	reason_code,
	postable,
	approved_by,
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
	posting_control_id,
	acctd_amount,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	adjustment_number,
	org_id,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	doc_sequence_value,
	doc_sequence_id,
	associated_application_id,
	cons_inv_id,
	mrc_gl_posted_date,
	mrc_posting_control_id,
	mrc_acctd_amount,
	adj_tax_acct_rule,
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
	link_to_trx_hist_id,
	event_id,
	upgrade_method,
	ax_accounted_flag,
	interest_header_id,
	interest_line_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.ar_adjustments_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_adjustments_all';
	
commit;