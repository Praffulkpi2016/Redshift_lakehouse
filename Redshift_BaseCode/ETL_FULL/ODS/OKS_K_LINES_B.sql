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
	
	DROP TABLE if exists bec_ods.OKS_K_LINES_B;
	
	CREATE TABLE IF NOT EXISTS bec_ods.OKS_K_LINES_B
	(
		id VARCHAR(100)   ENCODE lzo
		,cle_id VARCHAR(50)   ENCODE lzo
		,dnz_chr_id VARCHAR(50)   ENCODE lzo
		,discount_list NUMERIC(15,0) ENCODE az64
		,acct_rule_id NUMERIC(15,0) ENCODE az64
		,payment_type VARCHAR(30)   ENCODE lzo
		,cc_no VARCHAR(80)   ENCODE lzo
		,cc_expiry_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,cc_bank_acct_id NUMERIC(15,0) ENCODE az64
		,cc_auth_code VARCHAR(150)   ENCODE lzo
		,commitment_id NUMERIC(15,0) ENCODE az64
		,locked_price_list_id NUMERIC(15,0) ENCODE az64
		,usage_est_yn VARCHAR(1)   ENCODE lzo
		,usage_est_method VARCHAR(30)   ENCODE lzo
		,usage_est_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,termn_method VARCHAR(30)   ENCODE lzo
		,ubt_amount NUMERIC(28,10) ENCODE az64
		,credit_amount NUMERIC(28,10) ENCODE az64
		,suppressed_credit NUMERIC(28,10) ENCODE az64
		,override_amount NUMERIC(28,10) ENCODE az64
		,cust_po_number_req_yn VARCHAR(1)   ENCODE lzo
		,cust_po_number VARCHAR(150)   ENCODE lzo
		,grace_duration NUMERIC(15,0) ENCODE az64
		,grace_period VARCHAR(30)   ENCODE lzo
		,inv_print_flag VARCHAR(1)   ENCODE lzo
		,price_uom VARCHAR(30)   ENCODE lzo
		,tax_amount NUMERIC(28,10) ENCODE az64
		,tax_inclusive_yn VARCHAR(1)   ENCODE lzo
		,tax_status VARCHAR(30)   ENCODE lzo
		,tax_code NUMERIC(28,10) ENCODE az64
		,tax_exemption_id NUMERIC(15,0) ENCODE az64
		,ib_trans_type VARCHAR(10)   ENCODE lzo
		,ib_trans_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,prod_price NUMERIC(28,10) ENCODE az64
		,service_price NUMERIC(28,10) ENCODE az64
		,clvl_list_price NUMERIC(28,10) ENCODE az64
		,clvl_quantity NUMERIC(28,10) ENCODE az64
		,clvl_extended_amt NUMERIC(28,10) ENCODE az64
		,clvl_uom_code VARCHAR(3)   ENCODE lzo
		,toplvl_operand_code VARCHAR(30)   ENCODE lzo
		,toplvl_operand_val NUMERIC(28,10) ENCODE az64
		,toplvl_quantity NUMERIC(28,10) ENCODE az64
		,toplvl_uom_code VARCHAR(3)   ENCODE lzo
		,toplvl_adj_price NUMERIC(28,10) ENCODE az64
		,toplvl_price_qty NUMERIC(28,10) ENCODE az64
		,averaging_interval NUMERIC(28,10) ENCODE az64
		,settlement_interval VARCHAR(30)   ENCODE lzo
		,minimum_quantity NUMERIC(28,10) ENCODE az64
		,default_quantity NUMERIC(28,10) ENCODE az64
		,amcv_flag VARCHAR(1)   ENCODE lzo
		,fixed_quantity NUMERIC(28,10) ENCODE az64
		,usage_duration NUMERIC(28,10) ENCODE az64
		,usage_period VARCHAR(3)   ENCODE lzo
		,level_yn VARCHAR(1)   ENCODE lzo
		,usage_type VARCHAR(10)   ENCODE lzo
		,uom_quantified VARCHAR(3)   ENCODE lzo
		,base_reading  NUMERIC(15,0) ENCODE az64
		,billing_schedule_type VARCHAR(10)   ENCODE lzo
		,full_credit VARCHAR(1)   ENCODE lzo
		,locked_price_list_line_id  NUMERIC(15,0) ENCODE az64
		,break_uom VARCHAR(3)   ENCODE lzo
		,prorate VARCHAR(5)   ENCODE lzo
		,coverage_type VARCHAR(30)   ENCODE lzo
		,exception_cov_id NUMERIC(15,0) ENCODE az64
		,limit_uom_quantified VARCHAR(3)   ENCODE lzo
		,discount_amount NUMERIC(28,10) ENCODE az64
		,discount_percent NUMERIC(28,10) ENCODE az64
		,offset_duration NUMERIC(28,10) ENCODE az64
		,offset_period VARCHAR(3)   ENCODE lzo
		,incident_severity_id NUMERIC(15,0) ENCODE az64
		,pdf_id NUMERIC(15,0) ENCODE az64
		,work_thru_yn VARCHAR(1)   ENCODE lzo
		,react_active_yn VARCHAR(1)   ENCODE lzo
		,transfer_option VARCHAR(30)   ENCODE lzo
		,prod_upgrade_yn VARCHAR(1)   ENCODE lzo
		,inheritance_type VARCHAR(30)   ENCODE lzo
		,pm_program_id NUMERIC(15,0) ENCODE az64
		,pm_conf_req_yn VARCHAR(1)   ENCODE lzo
		,pm_sch_exists_yn VARCHAR(1)   ENCODE lzo
		,allow_bt_discount VARCHAR(1)   ENCODE lzo
		,apply_default_timezone VARCHAR(1)   ENCODE lzo
		,sync_date_install VARCHAR(1)   ENCODE lzo
		,object_version_number NUMERIC(15,0) ENCODE az64
		,security_group_id NUMERIC(15,0) ENCODE az64
		,request_id NUMERIC(15,0) ENCODE az64
		,created_by NUMERIC(15,0) ENCODE az64
		,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,last_updated_by NUMERIC(15,0)   ENCODE az64
		,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,last_update_login NUMERIC(15,0)   ENCODE az64
		,trxn_extension_id NUMERIC(15,0) ENCODE az64
		,tax_classification_code VARCHAR(50)   ENCODE lzo
		,exempt_certificate_number VARCHAR(80)   ENCODE lzo
		,exempt_reason_code VARCHAR(30)   ENCODE lzo
		,coverage_id VARCHAR(50)   ENCODE lzo
		,standard_cov_yn VARCHAR(1)   ENCODE lzo
		,orig_system_id1 VARCHAR(50)   ENCODE lzo
		,orig_system_reference1 VARCHAR(30)   ENCODE lzo
		,orig_system_source_code VARCHAR(30)   ENCODE lzo
		,revenue_impact_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,counter_value_id NUMERIC(15,0) ENCODE az64
		,from_day NUMERIC(15,0)   ENCODE az64
		,to_day NUMERIC(15,0)   ENCODE az64
		,inv_organization_id NUMERIC(15,0)   ENCODE az64
		,consolidation_yn VARCHAR(1)   ENCODE lzo
		,billing_option VARCHAR(30)   ENCODE lzo
		,usage_limit_definition VARCHAR(30)   ENCODE lzo
		,rollover_type VARCHAR(30)   ENCODE lzo
		,usage_limit_value NUMERIC(28,10) ENCODE az64
		,usage_grace_period VARCHAR(30)   ENCODE lzo
		,usage_grace_duration NUMERIC(28,10) ENCODE az64
		,KCA_OPERATION VARCHAR(10)   ENCODE lzo
		,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
		,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE
	auto;
	
	INSERT INTO bec_ods.OKS_K_LINES_B (
		id,
		cle_id,
		dnz_chr_id,
		discount_list,
		acct_rule_id,
		payment_type,
		cc_no,
		cc_expiry_date,
		cc_bank_acct_id,
		cc_auth_code,
		commitment_id,
		locked_price_list_id,
		usage_est_yn,
		usage_est_method,
		usage_est_start_date,
		termn_method,
		ubt_amount,
		credit_amount,
		suppressed_credit,
		override_amount,
		cust_po_number_req_yn,
		cust_po_number,
		grace_duration,
		grace_period,
		inv_print_flag,
		price_uom,
		tax_amount,
		tax_inclusive_yn,
		tax_status,
		tax_code,
		tax_exemption_id,
		ib_trans_type,
		ib_trans_date,
		prod_price,
		service_price,
		clvl_list_price,
		clvl_quantity,
		clvl_extended_amt,
		clvl_uom_code,
		toplvl_operand_code,
		toplvl_operand_val,
		toplvl_quantity,
		toplvl_uom_code,
		toplvl_adj_price,
		toplvl_price_qty,
		averaging_interval,
		settlement_interval,
		minimum_quantity,
		default_quantity,
		amcv_flag,
		fixed_quantity,
		usage_duration,
		usage_period,
		level_yn,
		usage_type,
		uom_quantified,
		base_reading,
		billing_schedule_type,
		full_credit,
		locked_price_list_line_id,
		break_uom,
		prorate,
		coverage_type,
		exception_cov_id,
		limit_uom_quantified,
		discount_amount,
		discount_percent,
		offset_duration,
		offset_period,
		incident_severity_id,
		pdf_id,
		work_thru_yn,
		react_active_yn,
		transfer_option,
		prod_upgrade_yn,
		inheritance_type,
		pm_program_id,
		pm_conf_req_yn,
		pm_sch_exists_yn,
		allow_bt_discount,
		apply_default_timezone,
		sync_date_install,
		object_version_number,
		security_group_id,
		request_id,
		created_by,
		creation_date,
		last_updated_by,
		last_update_date,
		last_update_login,
		trxn_extension_id,
		tax_classification_code,
		exempt_certificate_number,
		exempt_reason_code,
		coverage_id,
		standard_cov_yn,
		orig_system_id1,
		orig_system_reference1,
		orig_system_source_code,
		revenue_impact_date,
		counter_value_id,
		from_day,
		to_day,
		inv_organization_id,
		consolidation_yn,
		billing_option,
		usage_limit_definition,
		rollover_type,
		usage_limit_value,
		usage_grace_period,
		usage_grace_duration,
		KCA_OPERATION,
		IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
	)
    SELECT
	id,
	cle_id,
	dnz_chr_id,
	discount_list,
	acct_rule_id,
	payment_type,
	cc_no,
	cc_expiry_date,
	cc_bank_acct_id,
	cc_auth_code,
	commitment_id,
	locked_price_list_id,
	usage_est_yn,
	usage_est_method,
	usage_est_start_date,
	termn_method,
	ubt_amount,
	credit_amount,
	suppressed_credit,
	override_amount,
	cust_po_number_req_yn,
	cust_po_number,
	grace_duration,
	grace_period,
	inv_print_flag,
	price_uom,
	tax_amount,
	tax_inclusive_yn,
	tax_status,
	tax_code,
	tax_exemption_id,
	ib_trans_type,
	ib_trans_date,
	prod_price,
	service_price,
	clvl_list_price,
	clvl_quantity,
	clvl_extended_amt,
	clvl_uom_code,
	toplvl_operand_code,
	toplvl_operand_val,
	toplvl_quantity,
	toplvl_uom_code,
	toplvl_adj_price,
	toplvl_price_qty,
	averaging_interval,
	settlement_interval,
	minimum_quantity,
	default_quantity,
	amcv_flag,
	fixed_quantity,
	usage_duration,
	usage_period,
	level_yn,
	usage_type,
	uom_quantified,
	base_reading,
	billing_schedule_type,
	full_credit,
	locked_price_list_line_id,
	break_uom,
	prorate,
	coverage_type,
	exception_cov_id,
	limit_uom_quantified,
	discount_amount,
	discount_percent,
	offset_duration,
	offset_period,
	incident_severity_id,
	pdf_id,
	work_thru_yn,
	react_active_yn,
	transfer_option,
	prod_upgrade_yn,
	inheritance_type,
	pm_program_id,
	pm_conf_req_yn,
	pm_sch_exists_yn,
	allow_bt_discount,
	apply_default_timezone,
	sync_date_install,
	object_version_number,
	security_group_id,
	request_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	trxn_extension_id,
	tax_classification_code,
	exempt_certificate_number,
	exempt_reason_code,
	coverage_id,
	standard_cov_yn,
	orig_system_id1,
	orig_system_reference1,
	orig_system_source_code,
	revenue_impact_date,
	counter_value_id,
	from_day,
	to_day,
	inv_organization_id,
	consolidation_yn,
	billing_option,
	usage_limit_definition,
	rollover_type,
	usage_limit_value,
	usage_grace_period,
	usage_grace_duration,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
	bec_ods_stg.OKS_K_LINES_B;
	
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
ods_table_name = 'oks_k_lines_b';

Commit;