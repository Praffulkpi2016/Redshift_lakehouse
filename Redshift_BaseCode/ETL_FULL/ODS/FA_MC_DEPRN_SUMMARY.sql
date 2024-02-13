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

DROP TABLE if exists bec_ods.FA_MC_DEPRN_SUMMARY;

CREATE TABLE IF NOT EXISTS bec_ods.FA_MC_DEPRN_SUMMARY
(

    set_of_books_id NUMERIC(15,0)   ENCODE az64
	,book_type_code VARCHAR(15)   ENCODE lzo
	,asset_id NUMERIC(15,0)   ENCODE az64
	,deprn_run_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,deprn_amount  NUMERIC(28,10)   ENCODE az64
	,ytd_deprn  NUMERIC(28,10)   ENCODE az64
	,deprn_reserve  NUMERIC(28,10)   ENCODE az64
	,deprn_source_code VARCHAR(15)   ENCODE lzo
	,adjusted_cost  NUMERIC(28,10)   ENCODE az64
	,bonus_rate  NUMERIC(28,10)   ENCODE az64
	,ltd_production  NUMERIC(28,10)   ENCODE az64
	,period_counter NUMERIC(15,0)   ENCODE az64
	,production  NUMERIC(28,10)   ENCODE az64
	,reval_amortization  NUMERIC(28,10)   ENCODE az64
	,reval_amortization_basis  NUMERIC(28,10)   ENCODE az64
	,reval_deprn_expense  NUMERIC(28,10)   ENCODE az64
	,reval_reserve  NUMERIC(28,10)   ENCODE az64
	,ytd_production  NUMERIC(28,10)   ENCODE az64
	,ytd_reval_deprn_expense  NUMERIC(28,10)   ENCODE az64
	,prior_fy_expense  NUMERIC(28,10)   ENCODE az64
	,converted_flag VARCHAR(16383)   ENCODE lzo
	,bonus_deprn_amount  NUMERIC(28,10)   ENCODE az64
	,bonus_ytd_deprn  NUMERIC(28,10)   ENCODE az64
	,bonus_deprn_reserve  NUMERIC(28,10)   ENCODE az64
	,prior_fy_bonus_expense  NUMERIC(28,10)   ENCODE az64
	,deprn_override_flag VARCHAR(1)   ENCODE lzo
	,system_deprn_amount  NUMERIC(28,10)   ENCODE az64
	,system_bonus_deprn_amount  NUMERIC(28,10)   ENCODE az64
	,event_id NUMERIC(15,0)   ENCODE az64
	,deprn_run_id NUMERIC(15,0)   ENCODE az64
	,deprn_adjustment_amount  NUMERIC(28,10)   ENCODE az64
	,bonus_deprn_adjustment_amount  NUMERIC(28,10)   ENCODE az64
	,impairment_amount  NUMERIC(28,10)   ENCODE az64
	,ytd_impairment  NUMERIC(28,10)   ENCODE az64
	,impairment_reserve  NUMERIC(28,10)   ENCODE az64
	,capital_adjustment  NUMERIC(28,10)   ENCODE az64
	,general_fund  NUMERIC(28,10)   ENCODE az64
	,reval_loss_balance  NUMERIC(28,10)   ENCODE az64 
	,UNREVALUED_COST  NUMERIC(28,10)   ENCODE az64
	,HISTORICAL_NBV  NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_MC_DEPRN_SUMMARY (
    set_of_books_id,
	book_type_code,
	asset_id,
	deprn_run_date,
	deprn_amount,
	ytd_deprn,
	deprn_reserve,
	deprn_source_code,
	adjusted_cost,
	bonus_rate,
	ltd_production,
	period_counter,
	production,
	reval_amortization,
	reval_amortization_basis,
	reval_deprn_expense,
	reval_reserve,
	ytd_production,
	ytd_reval_deprn_expense,
	prior_fy_expense,
	converted_flag,
	bonus_deprn_amount,
	bonus_ytd_deprn,
	bonus_deprn_reserve,
	prior_fy_bonus_expense,
	deprn_override_flag,
	system_deprn_amount,
	system_bonus_deprn_amount,
	event_id,
	deprn_run_id,
	deprn_adjustment_amount,
	bonus_deprn_adjustment_amount,
	impairment_amount,
	ytd_impairment,
	impairment_reserve,
	capital_adjustment,
	general_fund,
	reval_loss_balance, 
	UNREVALUED_COST,
	HISTORICAL_NBV,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    set_of_books_id,
	book_type_code,
	asset_id,
	deprn_run_date,
	deprn_amount,
	ytd_deprn,
	deprn_reserve,
	deprn_source_code,
	adjusted_cost,
	bonus_rate,
	ltd_production,
	period_counter,
	production,
	reval_amortization,
	reval_amortization_basis,
	reval_deprn_expense,
	reval_reserve,
	ytd_production,
	ytd_reval_deprn_expense,
	prior_fy_expense,
	converted_flag,
	bonus_deprn_amount,
	bonus_ytd_deprn,
	bonus_deprn_reserve,
	prior_fy_bonus_expense,
	deprn_override_flag,
	system_deprn_amount,
	system_bonus_deprn_amount,
	event_id,
	deprn_run_id,
	deprn_adjustment_amount,
	bonus_deprn_adjustment_amount,
	impairment_amount,
	ytd_impairment,
	impairment_reserve,
	capital_adjustment,
	general_fund,
	reval_loss_balance, 
	UNREVALUED_COST,
	HISTORICAL_NBV,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_MC_DEPRN_SUMMARY;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_mc_deprn_summary';
	
COMMIT;