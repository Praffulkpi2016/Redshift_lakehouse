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

delete
from
	bec_ods.FA_BOOK_CONTROLS
where
	nvl(BOOK_TYPE_CODE,'NA') in (
	select
		nvl(stg.BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE
	from
		bec_ods.FA_BOOK_CONTROLS ods,
		bec_ods_stg.FA_BOOK_CONTROLS stg
	where
		nvl(ods.BOOK_TYPE_CODE,'NA') = nvl(stg.BOOK_TYPE_CODE,'NA')
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FA_BOOK_CONTROLS
       (
    book_type_code,
	book_type_name,
	set_of_books_id,
	initial_date,
	last_deprn_run_date,
	amortize_flag,
	fully_reserved_flag,
	deprn_calendar,
	book_class,
	gl_posting_allowed_flag,
	current_fiscal_year,
	allow_mass_changes,
	allow_deprn_adjustments,
	accounting_flex_structure,
	last_update_date,
	last_updated_by,
	prorate_calendar,
	date_ineffective,
	je_retirement_category,
	je_depreciation_category,
	je_reclass_category,
	gl_je_source,
	je_addition_category,
	je_adjustment_category,
	distribution_source_book,
	je_transfer_category,
	copy_retirements_flag,
	copy_adjustments_flag,
	deprn_request_id,
	allow_cost_ceiling,
	allow_deprn_exp_ceiling,
	calculate_nbv,
	run_year_end_program,
	je_deferred_deprn_category,
	allow_cip_assets_flag,
	itc_allowed_flag,
	created_by,
	creation_date,
	last_update_login,
	allow_mass_copy,
	allow_purge_flag,
	allow_reval_flag,
	amortize_reval_reserve_flag,
	ap_intercompany_acct,
	ar_intercompany_acct,
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
	attribute_category_code,
	capital_gain_threshold,
	copy_salvage_value_flag,
	cost_of_removal_clearing_acct,
	cost_of_removal_gain_acct,
	cost_of_removal_loss_acct,
	default_life_extension_ceiling,
	default_life_extension_factor,
	default_max_fully_rsvd_revals,
	default_reval_fully_rsvd_flag,
	deferred_deprn_expense_acct,
	deferred_deprn_reserve_acct,
	deprn_allocation_code,
	deprn_status,
	fiscal_year_name,
	initial_period_counter,
	je_cip_adjustment_category,
	je_cip_addition_category,
	je_cip_reclass_category,
	je_cip_retirement_category,
	je_cip_reval_category,
	je_cip_transfer_category,
	je_reval_category,
	last_mass_copy_period_counter,
	last_period_counter,
	last_purge_period_counter,
	mass_copy_source_book,
	mass_request_id,
	nbv_amount_threshold,
	nbv_fraction_threshold,
	nbv_retired_gain_acct,
	nbv_retired_loss_acct,
	proceeds_of_sale_clearing_acct,
	proceeds_of_sale_gain_acct,
	proceeds_of_sale_loss_acct,
	revalue_on_retirement_flag,
	reval_deprn_reserve_flag,
	reval_posting_flag,
	reval_rsv_retired_gain_acct,
	reval_rsv_retired_loss_acct,
	deprn_adjustment_acct,
	immediate_copy_flag,
	je_deprn_adjustment_category,
	depr_first_year_ret_flag,
	flexbuilder_defaults_ccid,
	retire_reval_reserve_flag,
	use_current_nbv_for_deprn,
	copy_additions_flag,
	use_percent_salvage_value_flag,
	mc_source_flag,
	reval_ytd_deprn_flag,
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
	org_id,
	allow_group_deprn_flag,
	allow_cip_dep_group_flag,
	allow_interco_group_flag,
	copy_group_assignment_flag,
	copy_group_addition_flag,
	allow_cip_member_flag,
	allow_member_tracking_flag,
	intercompany_posting_flag,
	allow_backdated_transfers_flag,
	allow_cost_sign_change_flag,
	create_accounting_request_id,
	allow_impairment_flag,
	allow_bonus_deprn_flag,
	sorp_enabled_flag,
	copy_amort_adaj_exp_flag,
	copy_group_change_flag,
	prevent_prior_period_txns_flag,
	allow_unallocated_lines_flag,
	default_period_end_reval_flag,
	import_alloc_inv_lines_as_new,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
	select
		book_type_code,
	book_type_name,
	set_of_books_id,
	initial_date,
	last_deprn_run_date,
	amortize_flag,
	fully_reserved_flag,
	deprn_calendar,
	book_class,
	gl_posting_allowed_flag,
	current_fiscal_year,
	allow_mass_changes,
	allow_deprn_adjustments,
	accounting_flex_structure,
	last_update_date,
	last_updated_by,
	prorate_calendar,
	date_ineffective,
	je_retirement_category,
	je_depreciation_category,
	je_reclass_category,
	gl_je_source,
	je_addition_category,
	je_adjustment_category,
	distribution_source_book,
	je_transfer_category,
	copy_retirements_flag,
	copy_adjustments_flag,
	deprn_request_id,
	allow_cost_ceiling,
	allow_deprn_exp_ceiling,
	calculate_nbv,
	run_year_end_program,
	je_deferred_deprn_category,
	allow_cip_assets_flag,
	itc_allowed_flag,
	created_by,
	creation_date,
	last_update_login,
	allow_mass_copy,
	allow_purge_flag,
	allow_reval_flag,
	amortize_reval_reserve_flag,
	ap_intercompany_acct,
	ar_intercompany_acct,
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
	attribute_category_code,
	capital_gain_threshold,
	copy_salvage_value_flag,
	cost_of_removal_clearing_acct,
	cost_of_removal_gain_acct,
	cost_of_removal_loss_acct,
	default_life_extension_ceiling,
	default_life_extension_factor,
	default_max_fully_rsvd_revals,
	default_reval_fully_rsvd_flag,
	deferred_deprn_expense_acct,
	deferred_deprn_reserve_acct,
	deprn_allocation_code,
	deprn_status,
	fiscal_year_name,
	initial_period_counter,
	je_cip_adjustment_category,
	je_cip_addition_category,
	je_cip_reclass_category,
	je_cip_retirement_category,
	je_cip_reval_category,
	je_cip_transfer_category,
	je_reval_category,
	last_mass_copy_period_counter,
	last_period_counter,
	last_purge_period_counter,
	mass_copy_source_book,
	mass_request_id,
	nbv_amount_threshold,
	nbv_fraction_threshold,
	nbv_retired_gain_acct,
	nbv_retired_loss_acct,
	proceeds_of_sale_clearing_acct,
	proceeds_of_sale_gain_acct,
	proceeds_of_sale_loss_acct,
	revalue_on_retirement_flag,
	reval_deprn_reserve_flag,
	reval_posting_flag,
	reval_rsv_retired_gain_acct,
	reval_rsv_retired_loss_acct,
	deprn_adjustment_acct,
	immediate_copy_flag,
	je_deprn_adjustment_category,
	depr_first_year_ret_flag,
	flexbuilder_defaults_ccid,
	retire_reval_reserve_flag,
	use_current_nbv_for_deprn,
	copy_additions_flag,
	use_percent_salvage_value_flag,
	mc_source_flag,
	reval_ytd_deprn_flag,
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
	org_id,
	allow_group_deprn_flag,
	allow_cip_dep_group_flag,
	allow_interco_group_flag,
	copy_group_assignment_flag,
	copy_group_addition_flag,
	allow_cip_member_flag,
	allow_member_tracking_flag,
	intercompany_posting_flag,
	allow_backdated_transfers_flag,
	allow_cost_sign_change_flag,
	create_accounting_request_id,
	allow_impairment_flag,
	allow_bonus_deprn_flag,
	sorp_enabled_flag,
	copy_amort_adaj_exp_flag,
	copy_group_change_flag,
	prevent_prior_period_txns_flag,
	allow_unallocated_lines_flag,
	default_period_end_reval_flag,
	import_alloc_inv_lines_as_new,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_BOOK_CONTROLS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (nvl(BOOK_TYPE_CODE,'NA'),
		kca_seq_id) in 
	(
		select
			nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,
			max(kca_seq_id)
		from
			bec_ods_stg.FA_BOOK_CONTROLS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(BOOK_TYPE_CODE,'NA'))
);

commit;

-- Soft delete
update bec_ods.FA_BOOK_CONTROLS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_BOOK_CONTROLS set IS_DELETED_FLG = 'Y'
where (BOOK_TYPE_CODE)  in
(
select BOOK_TYPE_CODE from bec_raw_dl_ext.FA_BOOK_CONTROLS
where (BOOK_TYPE_CODE,KCA_SEQ_ID)
in 
(
select BOOK_TYPE_CODE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_BOOK_CONTROLS
group by BOOK_TYPE_CODE
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'fa_book_controls';

commit;