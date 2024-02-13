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

DROP TABLE if exists bec_ods.fa_deprn_summary;

CREATE TABLE IF NOT EXISTS bec_ods.fa_deprn_summary
(
	BOOK_TYPE_CODE	VARCHAR(15)  ENCODE lzo
	,ASSET_ID	NUMERIC(15,0)   ENCODE az64
	,DEPRN_RUN_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,DEPRN_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,YTD_DEPRN	NUMERIC(28,10)   ENCODE az64
	,DEPRN_RESERVE	NUMERIC(28,10)   ENCODE az64
	,DEPRN_SOURCE_CODE	VARCHAR(15)  ENCODE lzo
	,ADJUSTED_COST	NUMERIC(28,10)   ENCODE az64
	,BONUS_RATE	NUMERIC(28,10)   ENCODE az64
	,LTD_PRODUCTION	NUMERIC(28,10)   ENCODE az64
	,PERIOD_COUNTER	NUMERIC(15,0)   ENCODE az64
	,PRODUCTION	NUMERIC(28,10)   ENCODE az64
	,REVAL_AMORTIZATION	NUMERIC(28,10)   ENCODE az64
	,REVAL_AMORTIZATION_BASIS	NUMERIC(28,10)   ENCODE az64
	,REVAL_DEPRN_EXPENSE	NUMERIC(28,10)   ENCODE az64
	,REVAL_RESERVE	NUMERIC(28,10)   ENCODE az64
	,YTD_PRODUCTION	NUMERIC(28,10)   ENCODE az64
	,YTD_REVAL_DEPRN_EXPENSE	NUMERIC(28,10)   ENCODE az64
	,PRIOR_FY_EXPENSE	NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,BONUS_YTD_DEPRN	NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_RESERVE	NUMERIC(28,10)   ENCODE az64
	,PRIOR_FY_BONUS_EXPENSE	NUMERIC(28,10)   ENCODE az64
	,DEPRN_OVERRIDE_FLAG	VARCHAR(1) ENCODE lzo
	,SYSTEM_DEPRN_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,SYSTEM_BONUS_DEPRN_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,EVENT_ID	NUMERIC(38,0)   ENCODE az64
	,DEPRN_RUN_ID	NUMERIC(15,0)   ENCODE az64
	,DEPRN_ADJUSTMENT_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,BONUS_DEPRN_ADJUSTMENT_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,IMPAIRMENT_AMOUNT	NUMERIC(28,10)   ENCODE az64
	,YTD_IMPAIRMENT	NUMERIC(28,10)   ENCODE az64
	,IMPAIRMENT_RESERVE	NUMERIC(28,10)   ENCODE az64
	,CAPITAL_ADJUSTMENT	NUMERIC(28,10)   ENCODE az64
	,GENERAL_FUND	NUMERIC(28,10)   ENCODE az64
	,REVAL_LOSS_BALANCE	NUMERIC(28,10)   ENCODE az64
	,UNREVALUED_COST	NUMERIC(28,10)   ENCODE az64
	,HISTORICAL_NBV	NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,KCA_SEQ_ID NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.fa_deprn_summary
(
	BOOK_TYPE_CODE,
	ASSET_ID,
	DEPRN_RUN_DATE,
	DEPRN_AMOUNT,
	YTD_DEPRN,
	DEPRN_RESERVE,
	DEPRN_SOURCE_CODE,
	ADJUSTED_COST,
	BONUS_RATE,
	LTD_PRODUCTION,
	PERIOD_COUNTER,
	PRODUCTION,
	REVAL_AMORTIZATION,
	REVAL_AMORTIZATION_BASIS,
	REVAL_DEPRN_EXPENSE,
	REVAL_RESERVE,
	YTD_PRODUCTION,
	YTD_REVAL_DEPRN_EXPENSE,
	PRIOR_FY_EXPENSE,
	BONUS_DEPRN_AMOUNT,
	BONUS_YTD_DEPRN,
	BONUS_DEPRN_RESERVE,
	PRIOR_FY_BONUS_EXPENSE,
	DEPRN_OVERRIDE_FLAG,
	SYSTEM_DEPRN_AMOUNT,
	SYSTEM_BONUS_DEPRN_AMOUNT,
	EVENT_ID,
	DEPRN_RUN_ID,
	DEPRN_ADJUSTMENT_AMOUNT,
	BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	IMPAIRMENT_AMOUNT,
	YTD_IMPAIRMENT,
	IMPAIRMENT_RESERVE,
	CAPITAL_ADJUSTMENT,
	GENERAL_FUND,
	REVAL_LOSS_BALANCE,
	UNREVALUED_COST,
	HISTORICAL_NBV,
	KCA_OPERATION,
	IS_DELETED_FLG,
	KCA_SEQ_ID,
	kca_seq_date
)
SELECT
	BOOK_TYPE_CODE,
	ASSET_ID,
	DEPRN_RUN_DATE,
	DEPRN_AMOUNT,
	YTD_DEPRN,
	DEPRN_RESERVE,
	DEPRN_SOURCE_CODE,
	ADJUSTED_COST,
	BONUS_RATE,
	LTD_PRODUCTION,
	PERIOD_COUNTER,
	PRODUCTION,
	REVAL_AMORTIZATION,
	REVAL_AMORTIZATION_BASIS,
	REVAL_DEPRN_EXPENSE,
	REVAL_RESERVE,
	YTD_PRODUCTION,
	YTD_REVAL_DEPRN_EXPENSE,
	PRIOR_FY_EXPENSE,
	BONUS_DEPRN_AMOUNT,
	BONUS_YTD_DEPRN,
	BONUS_DEPRN_RESERVE,
	PRIOR_FY_BONUS_EXPENSE,
	DEPRN_OVERRIDE_FLAG,
	SYSTEM_DEPRN_AMOUNT,
	SYSTEM_BONUS_DEPRN_AMOUNT,
	EVENT_ID,
	DEPRN_RUN_ID,
	DEPRN_ADJUSTMENT_AMOUNT,
	BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	IMPAIRMENT_AMOUNT,
	YTD_IMPAIRMENT,
	IMPAIRMENT_RESERVE,
	CAPITAL_ADJUSTMENT,
	GENERAL_FUND,
	REVAL_LOSS_BALANCE,
	UNREVALUED_COST,
	HISTORICAL_NBV,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date

    FROM
        bec_ods_stg.fa_deprn_summary;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_deprn_summary'; 
	
COMMIT;