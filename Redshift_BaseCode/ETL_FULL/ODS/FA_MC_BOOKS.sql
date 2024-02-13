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

DROP TABLE if exists bec_ods.FA_MC_BOOKS;

CREATE TABLE IF NOT EXISTS bec_ods.FA_MC_BOOKS
(

    set_of_books_id NUMERIC(15,0)   ENCODE az64
	,asset_id NUMERIC(15,0)   ENCODE az64
	,book_type_code VARCHAR(30)   ENCODE lzo
	,transaction_header_id_in NUMERIC(15,0)   ENCODE az64
	,transaction_header_id_out NUMERIC(15,0)   ENCODE az64
	,adjusted_cost NUMERIC(28,10)   ENCODE az64
	,cost NUMERIC(28,10)   ENCODE az64
	,source_cost NUMERIC(28,10)   ENCODE az64
	,original_cost NUMERIC(28,10)   ENCODE az64
	,source_original_cost NUMERIC(28,10)   ENCODE az64
	,salvage_value NUMERIC(28,10)   ENCODE az64
	,adjustment_required_status VARCHAR(4)   ENCODE lzo
	,retirement_pending_flag VARCHAR(3)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,itc_amount NUMERIC(28,10)   ENCODE az64
	,itc_basis NUMERIC(28,10)   ENCODE az64
	,recoverable_cost NUMERIC(28,10)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,reval_ceiling NUMERIC(28,10)   ENCODE az64
	,period_counter_fully_reserved NUMERIC(15,0)   ENCODE az64
	,unrevalued_cost NUMERIC(28,10)   ENCODE az64
	,allowed_deprn_limit_amount NUMERIC(28,10)   ENCODE az64
	,period_counter_life_complete NUMERIC(15,0)   ENCODE az64
	,adjusted_recoverable_cost NUMERIC(28,10)   ENCODE az64
	,converted_flag VARCHAR(1)   ENCODE lzo
	,annual_deprn_rounding_flag VARCHAR(5)   ENCODE lzo
	,eofy_adj_cost NUMERIC(28,10)   ENCODE az64
	,old_adjusted_cost NUMERIC(28,10)   ENCODE az64
	,eofy_formula_factor NUMERIC(28,10)   ENCODE az64
	,formula_factor NUMERIC(28,10)   ENCODE az64
	,remaining_life1 NUMERIC(4,0)   ENCODE az64
	,remaining_life2 NUMERIC(4,0)   ENCODE az64
	,short_fiscal_year_flag VARCHAR(3)   ENCODE lzo
	,group_asset_id NUMERIC(15,0)   ENCODE az64
	,reval_amortization_basis NUMERIC(28,10)   ENCODE az64
	,itc_amount_id NUMERIC(15,0)   ENCODE az64
	,retirement_id NUMERIC(15,0)   ENCODE az64
	,tax_request_id NUMERIC(15,0)   ENCODE az64
	,basic_rate NUMERIC(28,10)   ENCODE az64
	,adjusted_rate NUMERIC(28,10)   ENCODE az64
	,bonus_rule VARCHAR(30)   ENCODE lzo
	,ceiling_name VARCHAR(30)   ENCODE lzo
	,adjusted_capacity NUMERIC(28,10)   ENCODE az64
	,fully_rsvd_revals_counter NUMERIC(15,0)   ENCODE az64
	,idled_flag VARCHAR(3)   ENCODE lzo
	,period_counter_capitalized NUMERIC(15,0)   ENCODE az64
	,period_counter_fully_retired NUMERIC(15,0)   ENCODE az64
	,production_capacity NUMERIC(28,10)   ENCODE az64
	,unit_of_measure VARCHAR(25)   ENCODE lzo
	,percent_salvage_value NUMERIC(28,10)   ENCODE az64
	,allowed_deprn_limit NUMERIC(28,10)   ENCODE az64
	,annual_rounding_flag VARCHAR(3)   ENCODE lzo
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
	,date_placed_in_service TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_effective TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,deprn_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,deprn_method_code VARCHAR(12)   ENCODE lzo
	,life_in_months NUMERIC(15,0)   ENCODE az64
	,rate_adjustment_factor NUMERIC(28,10)   ENCODE az64
	,prorate_convention_code VARCHAR(10)   ENCODE lzo
	,prorate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cost_change_flag VARCHAR(3)   ENCODE lzo
	,capitalize_flag VARCHAR(3)   ENCODE lzo
	,depreciate_flag VARCHAR(3)   ENCODE lzo
	,date_ineffective TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,conversion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_deprn_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,salvage_type VARCHAR(30)   ENCODE lzo
	,deprn_limit_type VARCHAR(30)   ENCODE lzo
	,reduction_rate NUMERIC(28,10)   ENCODE az64
	,reduce_addition_flag VARCHAR(1)   ENCODE lzo
	,reduce_adjustment_flag VARCHAR(1)   ENCODE lzo
	,reduce_retirement_flag VARCHAR(1)   ENCODE lzo
	,recognize_gain_loss VARCHAR(30)   ENCODE lzo
	,recapture_reserve_flag VARCHAR(1)   ENCODE lzo
	,limit_proceeds_flag VARCHAR(1)   ENCODE lzo
	,terminal_gain_loss VARCHAR(30)   ENCODE lzo
	,tracking_method VARCHAR(30)   ENCODE lzo
	,exclude_fully_rsv_flag VARCHAR(1)   ENCODE lzo
	,excess_allocation_option VARCHAR(30)   ENCODE lzo
	,depreciation_option VARCHAR(30)   ENCODE lzo
	,member_rollup_flag VARCHAR(1)   ENCODE lzo
	,allocate_to_fully_rsv_flag VARCHAR(1)   ENCODE lzo
	,allocate_to_fully_ret_flag VARCHAR(1)   ENCODE lzo
	,terminal_gain_loss_amount NUMERIC(28,10)   ENCODE az64
	,cip_cost NUMERIC(28,10)   ENCODE az64
	,ytd_proceeds NUMERIC(28,10)   ENCODE az64
	,ltd_proceeds NUMERIC(28,10)   ENCODE az64
	,ltd_cost_of_removal NUMERIC(28,10)   ENCODE az64
	,eofy_reserve NUMERIC(28,10)   ENCODE az64
	,prior_eofy_reserve NUMERIC(28,10)   ENCODE az64
	,eop_adj_cost NUMERIC(28,10)   ENCODE az64
	,eop_formula_factor NUMERIC(28,10)   ENCODE az64
	,exclude_proceeds_from_basis VARCHAR(1)   ENCODE lzo
	,retirement_deprn_option VARCHAR(30)   ENCODE lzo
	,terminal_gain_loss_flag VARCHAR(1)   ENCODE lzo
	,super_group_id NUMERIC(15,0)   ENCODE az64
	,over_depreciate_option VARCHAR(30)   ENCODE lzo
	,disabled_flag VARCHAR(1)   ENCODE lzo
	,old_adjusted_capacity NUMERIC(28,10)   ENCODE az64
	,dry_hole_flag VARCHAR(1)   ENCODE lzo
	,cash_generating_unit_id NUMERIC(15,0)   ENCODE az64
	,contract_id NUMERIC(15,0)   ENCODE az64
	,rate_in_use NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_MC_BOOKS (
    set_of_books_id,
	asset_id,
	book_type_code,
	transaction_header_id_in,
	transaction_header_id_out,
	adjusted_cost,
	cost,
	source_cost,
	original_cost,
	source_original_cost,
	salvage_value,
	adjustment_required_status,
	retirement_pending_flag,
	last_update_date,
	last_updated_by,
	itc_amount,
	itc_basis,
	recoverable_cost,
	last_update_login,
	reval_ceiling,
	period_counter_fully_reserved,
	unrevalued_cost,
	allowed_deprn_limit_amount,
	period_counter_life_complete,
	adjusted_recoverable_cost,
	converted_flag,
	annual_deprn_rounding_flag,
	eofy_adj_cost,
	old_adjusted_cost,
	eofy_formula_factor,
	formula_factor,
	remaining_life1,
	remaining_life2,
	short_fiscal_year_flag,
	group_asset_id,
	reval_amortization_basis,
	itc_amount_id,
	retirement_id,
	tax_request_id,
	basic_rate,
	adjusted_rate,
	bonus_rule,
	ceiling_name,
	adjusted_capacity,
	fully_rsvd_revals_counter,
	idled_flag,
	period_counter_capitalized,
	period_counter_fully_retired,
	production_capacity,
	unit_of_measure,
	percent_salvage_value,
	allowed_deprn_limit,
	annual_rounding_flag,
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
	date_placed_in_service,
	date_effective,
	deprn_start_date,
	deprn_method_code,
	life_in_months,
	rate_adjustment_factor,
	prorate_convention_code,
	prorate_date,
	cost_change_flag,
	capitalize_flag,
	depreciate_flag,
	date_ineffective,
	conversion_date,
	original_deprn_start_date,
	salvage_type,
	deprn_limit_type,
	reduction_rate,
	reduce_addition_flag,
	reduce_adjustment_flag,
	reduce_retirement_flag,
	recognize_gain_loss,
	recapture_reserve_flag,
	limit_proceeds_flag,
	terminal_gain_loss,
	tracking_method,
	exclude_fully_rsv_flag,
	excess_allocation_option,
	depreciation_option,
	member_rollup_flag,
	allocate_to_fully_rsv_flag,
	allocate_to_fully_ret_flag,
	terminal_gain_loss_amount,
	cip_cost,
	ytd_proceeds,
	ltd_proceeds,
	ltd_cost_of_removal,
	eofy_reserve,
	prior_eofy_reserve,
	eop_adj_cost,
	eop_formula_factor,
	exclude_proceeds_from_basis,
	retirement_deprn_option,
	terminal_gain_loss_flag,
	super_group_id,
	over_depreciate_option,
	disabled_flag,
	old_adjusted_capacity,
	dry_hole_flag,
	cash_generating_unit_id,
	contract_id,
	rate_in_use,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    set_of_books_id,
	asset_id,
	book_type_code,
	transaction_header_id_in,
	transaction_header_id_out,
	adjusted_cost,
	cost,
	source_cost,
	original_cost,
	source_original_cost,
	salvage_value,
	adjustment_required_status,
	retirement_pending_flag,
	last_update_date,
	last_updated_by,
	itc_amount,
	itc_basis,
	recoverable_cost,
	last_update_login,
	reval_ceiling,
	period_counter_fully_reserved,
	unrevalued_cost,
	allowed_deprn_limit_amount,
	period_counter_life_complete,
	adjusted_recoverable_cost,
	converted_flag,
	annual_deprn_rounding_flag,
	eofy_adj_cost,
	old_adjusted_cost,
	eofy_formula_factor,
	formula_factor,
	remaining_life1,
	remaining_life2,
	short_fiscal_year_flag,
	group_asset_id,
	reval_amortization_basis,
	itc_amount_id,
	retirement_id,
	tax_request_id,
	basic_rate,
	adjusted_rate,
	bonus_rule,
	ceiling_name,
	adjusted_capacity,
	fully_rsvd_revals_counter,
	idled_flag,
	period_counter_capitalized,
	period_counter_fully_retired,
	production_capacity,
	unit_of_measure,
	percent_salvage_value,
	allowed_deprn_limit,
	annual_rounding_flag,
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
	date_placed_in_service,
	date_effective,
	deprn_start_date,
	deprn_method_code,
	life_in_months,
	rate_adjustment_factor,
	prorate_convention_code,
	prorate_date,
	cost_change_flag,
	capitalize_flag,
	depreciate_flag,
	date_ineffective,
	conversion_date,
	original_deprn_start_date,
	salvage_type,
	deprn_limit_type,
	reduction_rate,
	reduce_addition_flag,
	reduce_adjustment_flag,
	reduce_retirement_flag,
	recognize_gain_loss,
	recapture_reserve_flag,
	limit_proceeds_flag,
	terminal_gain_loss,
	tracking_method,
	exclude_fully_rsv_flag,
	excess_allocation_option,
	depreciation_option,
	member_rollup_flag,
	allocate_to_fully_rsv_flag,
	allocate_to_fully_ret_flag,
	terminal_gain_loss_amount,
	cip_cost,
	ytd_proceeds,
	ltd_proceeds,
	ltd_cost_of_removal,
	eofy_reserve,
	prior_eofy_reserve,
	eop_adj_cost,
	eop_formula_factor,
	exclude_proceeds_from_basis,
	retirement_deprn_option,
	terminal_gain_loss_flag,
	super_group_id,
	over_depreciate_option,
	disabled_flag,
	old_adjusted_capacity,
	dry_hole_flag,
	cash_generating_unit_id,
	contract_id,
	rate_in_use,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_MC_BOOKS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_mc_books';
	
COMMIT;