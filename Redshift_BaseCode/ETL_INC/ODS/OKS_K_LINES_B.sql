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
	
	delete from bec_ods.OKS_K_LINES_B
	where (nvl(ID,'NA') )  in (
		select nvl(stg.ID,'NA') as ID from bec_ods.OKS_K_LINES_B ods, bec_ods_stg.OKS_K_LINES_B stg
		where nvl(ods.ID,'NA') = nvl(stg.ID,'NA')
 		and stg.kca_operation IN ('INSERT','UPDATE')
	);
	
	commit;
	
	-- Insert records
	
	insert into	bec_ods.OKS_K_LINES_B
	(
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
	kca_seq_date)	
	(
		select
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
		'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.OKS_K_LINES_B
		where kca_operation IN ('INSERT','UPDATE') 
		and (nvl(ID,'NA') ,kca_seq_id) in 
		(select nvl(ID,'NA') as ID ,max(kca_seq_id) from bec_ods_stg.OKS_K_LINES_B 
			where kca_operation IN ('INSERT','UPDATE')
		group by nvl(ID,'NA') )
	);
	
	commit;
	
	-- Soft delete
update bec_ods.OKS_K_LINES_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKS_K_LINES_B set IS_DELETED_FLG = 'Y'
where (NVL(ID,'NA'))  in
(
select NVL(ID,'NA') from bec_raw_dl_ext.OKS_K_LINES_B
where (NVL(ID,'NA'),KCA_SEQ_ID)
in 
(
select NVL(ID,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKS_K_LINES_B
group by NVL(ID,'NA')
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oks_k_lines_b';

commit;