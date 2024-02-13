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

DROP TABLE if exists bec_ods.AR_DISTRIBUTIONS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.ar_distributions_all
(
	line_id NUMERIC(15,0)   ENCODE az64
	,source_id NUMERIC(15,0)   ENCODE az64
	,source_table VARCHAR(10)   ENCODE lzo
	,source_type VARCHAR(30)   ENCODE lzo
	,code_combination_id NUMERIC(15,0)   ENCODE az64
	,amount_dr NUMERIC(28,10)   ENCODE az64
	,amount_cr NUMERIC(28,10)   ENCODE az64
	,acctd_amount_dr NUMERIC(28,10)   ENCODE az64
	,acctd_amount_cr NUMERIC(28,10)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,source_table_secondary VARCHAR(10)   ENCODE lzo
	,source_id_secondary NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,currency_conversion_rate NUMERIC(28,10)   ENCODE az64
	,currency_conversion_type VARCHAR(30)   ENCODE lzo
	,currency_conversion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,taxable_entered_dr NUMERIC(28,10)   ENCODE az64
	,taxable_entered_cr NUMERIC(28,10)   ENCODE az64
	,taxable_accounted_dr NUMERIC(28,10)   ENCODE az64
	,taxable_accounted_cr NUMERIC(28,10)   ENCODE az64
	,tax_link_id NUMERIC(15,0)   ENCODE az64
	,third_party_id NUMERIC(15,0)   ENCODE az64
	,third_party_sub_id NUMERIC(15,0)   ENCODE az64
	,reversed_source_id NUMERIC(15,0)   ENCODE az64
	,tax_code_id NUMERIC(15,0)   ENCODE az64
	,location_segment_id NUMERIC(15,0)   ENCODE az64
	,source_type_secondary VARCHAR(30)   ENCODE lzo
	,tax_group_code_id NUMERIC(15,0)   ENCODE az64
	,ref_customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,ref_cust_trx_line_gl_dist_id NUMERIC(15,0)   ENCODE az64
	,ref_account_class VARCHAR(30)   ENCODE lzo
	,activity_bucket VARCHAR(30)   ENCODE lzo
	,ref_line_id NUMERIC(15,0)   ENCODE az64
	,from_amount_dr NUMERIC(28,10)   ENCODE az64
	,from_amount_cr NUMERIC(28,10)   ENCODE az64
	,from_acctd_amount_dr NUMERIC(28,10)   ENCODE az64
	,from_acctd_amount_cr NUMERIC(28,10)   ENCODE az64
	,ref_mf_dist_flag VARCHAR(1)   ENCODE lzo
	,ref_dist_ccid NUMERIC(15,0)   ENCODE az64 
	,REF_PREV_CUST_TRX_LINE_ID NUMERIC(15,0)   ENCODE az64 
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.AR_DISTRIBUTIONS_ALL (
    line_id,
	source_id,
	source_table,
	source_type,
	code_combination_id,
	amount_dr,
	amount_cr,
	acctd_amount_dr,
	acctd_amount_cr,
	creation_date,
	created_by,
	last_updated_by,
	last_update_date,
	last_update_login,
	org_id,
	source_table_secondary,
	source_id_secondary,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	taxable_entered_dr,
	taxable_entered_cr,
	taxable_accounted_dr,
	taxable_accounted_cr,
	tax_link_id,
	third_party_id,
	third_party_sub_id,
	reversed_source_id,
	tax_code_id,
	location_segment_id,
	source_type_secondary,
	tax_group_code_id,
	ref_customer_trx_line_id,
	ref_cust_trx_line_gl_dist_id,
	ref_account_class,
	activity_bucket,
	ref_line_id,
	from_amount_dr,
	from_amount_cr,
	from_acctd_amount_dr,
	from_acctd_amount_cr,
	ref_mf_dist_flag,
	ref_dist_ccid, 
	REF_PREV_CUST_TRX_LINE_ID,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        line_id,
	source_id,
	source_table,
	source_type,
	code_combination_id,
	amount_dr,
	amount_cr,
	acctd_amount_dr,
	acctd_amount_cr,
	creation_date,
	created_by,
	last_updated_by,
	last_update_date,
	last_update_login,
	org_id,
	source_table_secondary,
	source_id_secondary,
	currency_code,
	currency_conversion_rate,
	currency_conversion_type,
	currency_conversion_date,
	taxable_entered_dr,
	taxable_entered_cr,
	taxable_accounted_dr,
	taxable_accounted_cr,
	tax_link_id,
	third_party_id,
	third_party_sub_id,
	reversed_source_id,
	tax_code_id,
	location_segment_id,
	source_type_secondary,
	tax_group_code_id,
	ref_customer_trx_line_id,
	ref_cust_trx_line_gl_dist_id,
	ref_account_class,
	activity_bucket,
	ref_line_id,
	from_amount_dr,
	from_amount_cr,
	from_acctd_amount_dr,
	from_acctd_amount_cr,
	ref_mf_dist_flag,
	ref_dist_ccid, 
	REF_PREV_CUST_TRX_LINE_ID,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.AR_DISTRIBUTIONS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_distributions_all';
	
commit;