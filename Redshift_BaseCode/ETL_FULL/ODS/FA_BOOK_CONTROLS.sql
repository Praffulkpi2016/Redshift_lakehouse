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

DROP TABLE if exists bec_ods.FA_BOOK_CONTROLS;

CREATE TABLE IF NOT EXISTS bec_ods.FA_BOOK_CONTROLS
(
	book_type_code VARCHAR(15)   ENCODE lzo
	,book_type_name VARCHAR(30)   ENCODE lzo
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,initial_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_deprn_run_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amortize_flag VARCHAR(3)   ENCODE lzo
	,fully_reserved_flag VARCHAR(3)   ENCODE lzo
	,deprn_calendar VARCHAR(15)   ENCODE lzo
	,book_class VARCHAR(15)   ENCODE lzo
	,gl_posting_allowed_flag VARCHAR(3)   ENCODE lzo
	,current_fiscal_year SMALLINT   ENCODE az64
	,allow_mass_changes VARCHAR(3)   ENCODE lzo
	,allow_deprn_adjustments VARCHAR(3)   ENCODE lzo
	,accounting_flex_structure NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,prorate_calendar VARCHAR(15)   ENCODE lzo
	,date_ineffective TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,je_retirement_category VARCHAR(30)   ENCODE lzo
	,je_depreciation_category VARCHAR(30)   ENCODE lzo
	,je_reclass_category VARCHAR(30)   ENCODE lzo
	,gl_je_source VARCHAR(30)   ENCODE lzo
	,je_addition_category VARCHAR(30)   ENCODE lzo
	,je_adjustment_category VARCHAR(30)   ENCODE lzo
	,distribution_source_book VARCHAR(15)   ENCODE lzo
	,je_transfer_category VARCHAR(30)   ENCODE lzo
	,copy_retirements_flag VARCHAR(3)   ENCODE lzo
	,copy_adjustments_flag VARCHAR(3)   ENCODE lzo
	,deprn_request_id NUMERIC(15,0)   ENCODE az64
	,allow_cost_ceiling VARCHAR(3)   ENCODE lzo
	,allow_deprn_exp_ceiling VARCHAR(3)   ENCODE lzo
	,calculate_nbv VARCHAR(3)   ENCODE lzo
	,run_year_end_program VARCHAR(3)   ENCODE lzo
	,je_deferred_deprn_category VARCHAR(30)   ENCODE lzo
	,allow_cip_assets_flag VARCHAR(3)   ENCODE lzo
	,itc_allowed_flag VARCHAR(3)   ENCODE lzo
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,allow_mass_copy VARCHAR(3)   ENCODE lzo
	,allow_purge_flag VARCHAR(3)   ENCODE lzo
	,allow_reval_flag VARCHAR(3)   ENCODE lzo
	,amortize_reval_reserve_flag VARCHAR(3)   ENCODE lzo
	,ap_intercompany_acct VARCHAR(25)   ENCODE lzo
	,ar_intercompany_acct VARCHAR(25)   ENCODE lzo
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
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,attribute_category_code VARCHAR(30)   ENCODE lzo
	,capital_gain_threshold NUMERIC(28,10)   ENCODE az64
	,copy_salvage_value_flag VARCHAR(3)   ENCODE lzo
	,cost_of_removal_clearing_acct VARCHAR(25)   ENCODE lzo
	,cost_of_removal_gain_acct VARCHAR(25)   ENCODE lzo
	,cost_of_removal_loss_acct VARCHAR(25)   ENCODE lzo
	,default_life_extension_ceiling NUMERIC(28,10)   ENCODE az64
	,default_life_extension_factor NUMERIC(28,10)   ENCODE az64
	,default_max_fully_rsvd_revals NUMERIC(28,10)   ENCODE az64
	,default_reval_fully_rsvd_flag VARCHAR(3)   ENCODE lzo
	,deferred_deprn_expense_acct VARCHAR(25)   ENCODE lzo
	,deferred_deprn_reserve_acct VARCHAR(25)   ENCODE lzo
	,deprn_allocation_code VARCHAR(1)   ENCODE lzo
	,deprn_status VARCHAR(1)   ENCODE lzo
	,fiscal_year_name VARCHAR(30)   ENCODE lzo
	,initial_period_counter NUMERIC(15,0)   ENCODE az64
	,je_cip_adjustment_category VARCHAR(30)   ENCODE lzo
	,je_cip_addition_category VARCHAR(30)   ENCODE lzo
	,je_cip_reclass_category VARCHAR(30)   ENCODE lzo
	,je_cip_retirement_category VARCHAR(30)   ENCODE lzo
	,je_cip_reval_category VARCHAR(30)   ENCODE lzo
	,je_cip_transfer_category VARCHAR(30)   ENCODE lzo
	,je_reval_category VARCHAR(30)   ENCODE lzo
	,last_mass_copy_period_counter NUMERIC(15,0)   ENCODE az64
	,last_period_counter NUMERIC(15,0)   ENCODE az64
	,last_purge_period_counter NUMERIC(15,0)   ENCODE az64
	,mass_copy_source_book VARCHAR(15)   ENCODE lzo
	,mass_request_id NUMERIC(15,0)   ENCODE az64
	,nbv_amount_threshold NUMERIC(28,10)   ENCODE az64
	,nbv_fraction_threshold NUMERIC(28,10)   ENCODE az64
	,nbv_retired_gain_acct VARCHAR(25)   ENCODE lzo
	,nbv_retired_loss_acct VARCHAR(25)   ENCODE lzo
	,proceeds_of_sale_clearing_acct VARCHAR(25)   ENCODE lzo
	,proceeds_of_sale_gain_acct VARCHAR(25)   ENCODE lzo
	,proceeds_of_sale_loss_acct VARCHAR(25)   ENCODE lzo
	,revalue_on_retirement_flag VARCHAR(3)   ENCODE lzo
	,reval_deprn_reserve_flag VARCHAR(3)   ENCODE lzo
	,reval_posting_flag VARCHAR(3)   ENCODE lzo
	,reval_rsv_retired_gain_acct VARCHAR(25)   ENCODE lzo
	,reval_rsv_retired_loss_acct VARCHAR(25)   ENCODE lzo
	,deprn_adjustment_acct VARCHAR(25)   ENCODE lzo
	,immediate_copy_flag VARCHAR(3)   ENCODE lzo
	,je_deprn_adjustment_category VARCHAR(30)   ENCODE lzo
	,depr_first_year_ret_flag VARCHAR(3)   ENCODE lzo
	,flexbuilder_defaults_ccid NUMERIC(15,0)   ENCODE az64
	,retire_reval_reserve_flag VARCHAR(3)   ENCODE lzo
	,use_current_nbv_for_deprn VARCHAR(3)   ENCODE lzo
	,copy_additions_flag VARCHAR(3)   ENCODE lzo
	,use_percent_salvage_value_flag VARCHAR(3)   ENCODE lzo
	,mc_source_flag VARCHAR(1)   ENCODE lzo
	,reval_ytd_deprn_flag VARCHAR(3)   ENCODE lzo
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
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,allow_group_deprn_flag VARCHAR(1)   ENCODE lzo
	,allow_cip_dep_group_flag VARCHAR(1)   ENCODE lzo
	,allow_interco_group_flag VARCHAR(1)   ENCODE lzo
	,copy_group_assignment_flag VARCHAR(1)   ENCODE lzo
	,copy_group_addition_flag VARCHAR(1)   ENCODE lzo
	,allow_cip_member_flag VARCHAR(1)   ENCODE lzo
	,allow_member_tracking_flag VARCHAR(1)   ENCODE lzo
	,intercompany_posting_flag VARCHAR(1)   ENCODE lzo
	,allow_backdated_transfers_flag VARCHAR(1)   ENCODE lzo
	,allow_cost_sign_change_flag VARCHAR(1)   ENCODE lzo
	,create_accounting_request_id NUMERIC(15,0)   ENCODE az64
	,allow_impairment_flag VARCHAR(1)   ENCODE lzo
	,allow_bonus_deprn_flag VARCHAR(1)   ENCODE lzo
	,sorp_enabled_flag VARCHAR(1)   ENCODE lzo
	,copy_amort_adaj_exp_flag VARCHAR(1)   ENCODE lzo
	,copy_group_change_flag VARCHAR(1)   ENCODE lzo
	,prevent_prior_period_txns_flag VARCHAR(1)   ENCODE lzo
	,allow_unallocated_lines_flag VARCHAR(1)   ENCODE lzo
	,default_period_end_reval_flag VARCHAR(1)   ENCODE lzo
	,import_alloc_inv_lines_as_new VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_BOOK_CONTROLS (
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
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date )
SELECT
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
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_BOOK_CONTROLS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_book_controls';