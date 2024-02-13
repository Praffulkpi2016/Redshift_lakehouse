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

DROP TABLE if exists bec_ods.FA_MC_DEPRN_DETAIL;

CREATE TABLE IF NOT EXISTS bec_ods.FA_MC_DEPRN_DETAIL
(

    set_of_books_id NUMERIC(15,0)   ENCODE az64
	,book_type_code VARCHAR(30)   ENCODE lzo
	,asset_id NUMERIC(15,0)   ENCODE az64
	,period_counter NUMERIC(15,0)   ENCODE az64
	,distribution_id NUMERIC(15,0)   ENCODE az64
	,deprn_source_code VARCHAR(1)   ENCODE lzo
	,deprn_run_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,deprn_amount NUMERIC(28,10)   ENCODE az64
	,ytd_deprn NUMERIC(28,10)   ENCODE az64
	,deprn_reserve NUMERIC(28,10)   ENCODE az64
	,addition_cost_to_clear NUMERIC(28,10)   ENCODE az64
	,cost NUMERIC(28,10)   ENCODE az64
	,deprn_adjustment_amount NUMERIC(28,10)   ENCODE az64
	,deprn_expense_je_line_num NUMERIC(15,0)   ENCODE az64
	,deprn_reserve_je_line_num NUMERIC(15,0)   ENCODE az64
	,reval_amort_je_line_num NUMERIC(15,0)   ENCODE az64
	,reval_reserve_je_line_num NUMERIC(15,0)   ENCODE az64
	,je_header_id NUMERIC(15,0)   ENCODE az64
	,reval_amortization NUMERIC(28,10)   ENCODE az64
	,reval_deprn_expense NUMERIC(28,10)   ENCODE az64
	,reval_reserve NUMERIC(28,10)   ENCODE az64
	,ytd_reval_deprn_expense NUMERIC(28,10)   ENCODE az64
	,source_deprn_amount NUMERIC(28,10)   ENCODE az64
	,source_ytd_deprn NUMERIC(28,10)   ENCODE az64
	,source_deprn_reserve NUMERIC(28,10)   ENCODE az64
	,source_addition_cost_to_clear NUMERIC(28,10)   ENCODE az64
	,source_deprn_adjustment_amount NUMERIC(28,10)   ENCODE az64
	,source_reval_amortization NUMERIC(28,10)   ENCODE az64
	,source_reval_deprn_expense NUMERIC(28,10)   ENCODE az64
	,source_reval_reserve NUMERIC(28,10)   ENCODE az64
	,source_ytd_reval_deprn_expense NUMERIC(28,10)   ENCODE az64
	,converted_flag VARCHAR(1)   ENCODE lzo
	,bonus_deprn_amount NUMERIC(28,10)   ENCODE az64
	,bonus_ytd_deprn NUMERIC(28,10)   ENCODE az64
	,bonus_deprn_reserve NUMERIC(28,10)   ENCODE az64
	,bonus_deprn_adjustment_amount NUMERIC(28,10)   ENCODE az64
	,bonus_deprn_exp_je_line_num NUMERIC(15,0)   ENCODE az64
	,bonus_deprn_rsv_je_line_num NUMERIC(15,0)   ENCODE az64
	,deprn_expense_ccid NUMERIC(15,0)   ENCODE az64
	,deprn_reserve_ccid NUMERIC(15,0)   ENCODE az64
	,bonus_deprn_expense_ccid NUMERIC(15,0)   ENCODE az64
	,bonus_deprn_reserve_ccid NUMERIC(15,0)   ENCODE az64
	,reval_amort_ccid NUMERIC(15,0)   ENCODE az64
	,reval_reserve_ccid NUMERIC(15,0)   ENCODE az64
	,event_id NUMERIC(15,0)   ENCODE az64
	,deprn_run_id NUMERIC(15,0)   ENCODE az64
	,impairment_amount NUMERIC(28,10)   ENCODE az64
	,ytd_impairment NUMERIC(28,10)   ENCODE az64
	,impairment_reserve NUMERIC(15,0)   ENCODE az64
	,capital_adjustment NUMERIC(28,10)   ENCODE az64
	,general_fund NUMERIC(28,10)   ENCODE az64
	,reval_loss_balance NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_MC_DEPRN_DETAIL (
    set_of_books_id,
	book_type_code,
	asset_id,
	period_counter,
	distribution_id,
	deprn_source_code,
	deprn_run_date,
	deprn_amount,
	ytd_deprn,
	deprn_reserve,
	addition_cost_to_clear,
	cost,
	deprn_adjustment_amount,
	deprn_expense_je_line_num,
	deprn_reserve_je_line_num,
	reval_amort_je_line_num,
	reval_reserve_je_line_num,
	je_header_id,
	reval_amortization,
	reval_deprn_expense,
	reval_reserve,
	ytd_reval_deprn_expense,
	source_deprn_amount,
	source_ytd_deprn,
	source_deprn_reserve,
	source_addition_cost_to_clear,
	source_deprn_adjustment_amount,
	source_reval_amortization,
	source_reval_deprn_expense,
	source_reval_reserve,
	source_ytd_reval_deprn_expense,
	converted_flag,
	bonus_deprn_amount,
	bonus_ytd_deprn,
	bonus_deprn_reserve,
	bonus_deprn_adjustment_amount,
	bonus_deprn_exp_je_line_num,
	bonus_deprn_rsv_je_line_num,
	deprn_expense_ccid,
	deprn_reserve_ccid,
	bonus_deprn_expense_ccid,
	bonus_deprn_reserve_ccid,
	reval_amort_ccid,
	reval_reserve_ccid,
	event_id,
	deprn_run_id,
	impairment_amount,
	ytd_impairment,
	impairment_reserve,
	capital_adjustment,
	general_fund,
	reval_loss_balance, 
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    set_of_books_id,
	book_type_code,
	asset_id,
	period_counter,
	distribution_id,
	deprn_source_code,
	deprn_run_date,
	deprn_amount,
	ytd_deprn,
	deprn_reserve,
	addition_cost_to_clear,
	cost,
	deprn_adjustment_amount,
	deprn_expense_je_line_num,
	deprn_reserve_je_line_num,
	reval_amort_je_line_num,
	reval_reserve_je_line_num,
	je_header_id,
	reval_amortization,
	reval_deprn_expense,
	reval_reserve,
	ytd_reval_deprn_expense,
	source_deprn_amount,
	source_ytd_deprn,
	source_deprn_reserve,
	source_addition_cost_to_clear,
	source_deprn_adjustment_amount,
	source_reval_amortization,
	source_reval_deprn_expense,
	source_reval_reserve,
	source_ytd_reval_deprn_expense,
	converted_flag,
	bonus_deprn_amount,
	bonus_ytd_deprn,
	bonus_deprn_reserve,
	bonus_deprn_adjustment_amount,
	bonus_deprn_exp_je_line_num,
	bonus_deprn_rsv_je_line_num,
	deprn_expense_ccid,
	deprn_reserve_ccid,
	bonus_deprn_expense_ccid,
	bonus_deprn_reserve_ccid,
	reval_amort_ccid,
	reval_reserve_ccid,
	event_id,
	deprn_run_id,
	impairment_amount,
	ytd_impairment,
	impairment_reserve,
	capital_adjustment,
	general_fund,
	reval_loss_balance, 
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_MC_DEPRN_DETAIL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_mc_deprn_detail';
	
COMMIT;