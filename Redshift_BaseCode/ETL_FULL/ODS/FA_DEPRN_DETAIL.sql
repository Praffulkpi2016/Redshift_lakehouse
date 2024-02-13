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

DROP TABLE if exists bec_ods.fa_deprn_detail;

CREATE TABLE IF NOT EXISTS bec_ods.fa_deprn_detail
(
	BOOK_TYPE_CODE VARCHAR(15)   ENCODE lzo
	,ASSET_ID NUMERIC(15,0)   ENCODE az64
	,PERIOD_COUNTER NUMERIC(15,0)   ENCODE az64
	,DISTRIBUTION_ID NUMERIC(15,0)   ENCODE az64
	,DEPRN_SOURCE_CODE VARCHAR(1)   ENCODE lzo
	,DEPRN_RUN_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,DEPRN_AMOUNT NUMERIC(28,10)   ENCODE az64
	,YTD_DEPRN NUMERIC(28,10)   ENCODE az64
	,DEPRN_RESERVE NUMERIC(28,10)   ENCODE az64
	,ADDITION_COST_TO_CLEAR NUMERIC(28,10)   ENCODE az64
	,COST NUMERIC(28,10)   ENCODE az64
	,DEPRN_ADJUSTMENT_AMOUNT NUMERIC(28,10)   ENCODE az64
	,DEPRN_EXPENSE_JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,DEPRN_RESERVE_JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,REVAL_AMORT_JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,REVAL_RESERVE_JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,JE_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,REVAL_AMORTIZATION NUMERIC(28,10)   ENCODE az64
	,REVAL_DEPRN_EXPENSE NUMERIC(28,10)   ENCODE az64
	,REVAL_RESERVE NUMERIC(28,10)   ENCODE az64
	,YTD_REVAL_DEPRN_EXPENSE NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_AMOUNT NUMERIC(28,10)   ENCODE az64
	,BONUS_YTD_DEPRN NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_RESERVE NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_ADJUSTMENT_AMOUNT NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_EXP_JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,BONUS_DEPRN_RSV_JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,DEPRN_EXPENSE_CCID NUMERIC(15,0)   ENCODE az64
	,DEPRN_RESERVE_CCID NUMERIC(15,0)   ENCODE az64
	,BONUS_DEPRN_EXPENSE_CCID NUMERIC(15,0)   ENCODE az64
	,BONUS_DEPRN_RESERVE_CCID NUMERIC(15,0)   ENCODE az64
	,REVAL_AMORT_CCID NUMERIC(15,0)   ENCODE az64
	,REVAL_RESERVE_CCID NUMERIC(15,0)   ENCODE az64
	,EVENT_ID NUMERIC(38,0)   ENCODE az64
	,DEPRN_RUN_ID NUMERIC(15,0)   ENCODE az64
	,IMPAIRMENT_AMOUNT NUMERIC(28,10)   ENCODE az64
	,YTD_IMPAIRMENT NUMERIC(28,10)   ENCODE az64
	,IMPAIRMENT_RESERVE NUMERIC(28,10)   ENCODE az64
	,CAPITAL_ADJUSTMENT NUMERIC(28,10)   ENCODE az64
	,GENERAL_FUND NUMERIC(28,10)   ENCODE az64
	,REVAL_LOSS_BALANCE NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,KCA_SEQ_ID NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.fa_deprn_detail
(
	BOOK_TYPE_CODE,
	ASSET_ID,
	PERIOD_COUNTER,
	DISTRIBUTION_ID,
	DEPRN_SOURCE_CODE,
	DEPRN_RUN_DATE,
	DEPRN_AMOUNT,
	YTD_DEPRN,
	DEPRN_RESERVE,
	ADDITION_COST_TO_CLEAR,
	COST,
	DEPRN_ADJUSTMENT_AMOUNT,
	DEPRN_EXPENSE_JE_LINE_NUM,
	DEPRN_RESERVE_JE_LINE_NUM,
	REVAL_AMORT_JE_LINE_NUM,
	REVAL_RESERVE_JE_LINE_NUM,
	JE_HEADER_ID,
	REVAL_AMORTIZATION,
	REVAL_DEPRN_EXPENSE,
	REVAL_RESERVE,
	YTD_REVAL_DEPRN_EXPENSE,
	BONUS_DEPRN_AMOUNT,
	BONUS_YTD_DEPRN,
	BONUS_DEPRN_RESERVE,
	BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	BONUS_DEPRN_EXP_JE_LINE_NUM,
	BONUS_DEPRN_RSV_JE_LINE_NUM,
	DEPRN_EXPENSE_CCID,
	DEPRN_RESERVE_CCID,
	BONUS_DEPRN_EXPENSE_CCID,
	BONUS_DEPRN_RESERVE_CCID,
	REVAL_AMORT_CCID,
	REVAL_RESERVE_CCID,
	EVENT_ID,
	DEPRN_RUN_ID,
	IMPAIRMENT_AMOUNT,
	YTD_IMPAIRMENT,
	IMPAIRMENT_RESERVE,
	CAPITAL_ADJUSTMENT,
	GENERAL_FUND,
	REVAL_LOSS_BALANCE,
	KCA_OPERATION,
	IS_DELETED_FLG,
	KCA_SEQ_ID,
	kca_seq_date
)
SELECT
	BOOK_TYPE_CODE,
	ASSET_ID,
	PERIOD_COUNTER,
	DISTRIBUTION_ID,
	DEPRN_SOURCE_CODE,
	DEPRN_RUN_DATE,
	DEPRN_AMOUNT,
	YTD_DEPRN,
	DEPRN_RESERVE,
	ADDITION_COST_TO_CLEAR,
	COST,
	DEPRN_ADJUSTMENT_AMOUNT,
	DEPRN_EXPENSE_JE_LINE_NUM,
	DEPRN_RESERVE_JE_LINE_NUM,
	REVAL_AMORT_JE_LINE_NUM,
	REVAL_RESERVE_JE_LINE_NUM,
	JE_HEADER_ID,
	REVAL_AMORTIZATION,
	REVAL_DEPRN_EXPENSE,
	REVAL_RESERVE,
	YTD_REVAL_DEPRN_EXPENSE,
	BONUS_DEPRN_AMOUNT,
	BONUS_YTD_DEPRN,
	BONUS_DEPRN_RESERVE,
	BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	BONUS_DEPRN_EXP_JE_LINE_NUM,
	BONUS_DEPRN_RSV_JE_LINE_NUM,
	DEPRN_EXPENSE_CCID,
	DEPRN_RESERVE_CCID,
	BONUS_DEPRN_EXPENSE_CCID,
	BONUS_DEPRN_RESERVE_CCID,
	REVAL_AMORT_CCID,
	REVAL_RESERVE_CCID,
	EVENT_ID,
	DEPRN_RUN_ID,
	IMPAIRMENT_AMOUNT,
	YTD_IMPAIRMENT,
	IMPAIRMENT_RESERVE,
	CAPITAL_ADJUSTMENT,
	GENERAL_FUND,
	REVAL_LOSS_BALANCE,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date

    FROM
        bec_ods_stg.fa_deprn_detail;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_deprn_detail'; 
	
COMMIT;