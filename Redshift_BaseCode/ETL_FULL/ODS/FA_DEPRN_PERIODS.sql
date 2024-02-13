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

DROP TABLE if exists bec_ods.FA_DEPRN_PERIODS;

CREATE TABLE IF NOT EXISTS bec_ods.FA_DEPRN_PERIODS
(
	book_type_code VARCHAR(15)   ENCODE lzo
	,period_name VARCHAR(15)   ENCODE lzo
	,period_counter NUMERIC(15,0)   ENCODE az64
	,fiscal_year SMALLINT   ENCODE az64
	,period_num SMALLINT   ENCODE az64
	,period_open_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,period_close_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,depreciation_batch_id NUMERIC(15,0)   ENCODE az64
	,retirement_batch_id NUMERIC(15,0)   ENCODE az64
	,reclass_batch_id NUMERIC(15,0)   ENCODE az64
	,transfer_batch_id NUMERIC(15,0)   ENCODE az64
	,addition_batch_id NUMERIC(15,0)   ENCODE az64
	,adjustment_batch_id NUMERIC(15,0)   ENCODE az64
	,deferred_deprn_batch_id NUMERIC(15,0)   ENCODE az64
	,calendar_period_open_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,calendar_period_close_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cip_addition_batch_id NUMERIC(15,0)   ENCODE az64
	,cip_adjustment_batch_id NUMERIC(15,0)   ENCODE az64
	,cip_reclass_batch_id NUMERIC(15,0)   ENCODE az64
	,cip_retirement_batch_id NUMERIC(15,0)   ENCODE az64
	,cip_reval_batch_id NUMERIC(15,0)   ENCODE az64
	,cip_transfer_batch_id NUMERIC(15,0)   ENCODE az64
	,reval_batch_id NUMERIC(15,0)   ENCODE az64
	,deprn_adjustment_batch_id NUMERIC(15,0)   ENCODE az64
	,deprn_run VARCHAR(1)   ENCODE lzo
	,xla_conversion_status VARCHAR(2)   ENCODE lzo
	,gl_transfer_flag VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_DEPRN_PERIODS (
	book_type_code,
	period_name,
	period_counter,
	fiscal_year,
	period_num,
	period_open_date,
	period_close_date,
	depreciation_batch_id,
	retirement_batch_id,
	reclass_batch_id,
	transfer_batch_id,
	addition_batch_id,
	adjustment_batch_id,
	deferred_deprn_batch_id,
	calendar_period_open_date,
	calendar_period_close_date,
	cip_addition_batch_id,
	cip_adjustment_batch_id,
	cip_reclass_batch_id,
	cip_retirement_batch_id,
	cip_reval_batch_id,
	cip_transfer_batch_id,
	reval_batch_id,
	deprn_adjustment_batch_id,
	deprn_run,
	xla_conversion_status,
	gl_transfer_flag,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date )
SELECT
	book_type_code,
	period_name,
	period_counter,
	fiscal_year,
	period_num,
	period_open_date,
	period_close_date,
	depreciation_batch_id,
	retirement_batch_id,
	reclass_batch_id,
	transfer_batch_id,
	addition_batch_id,
	adjustment_batch_id,
	deferred_deprn_batch_id,
	calendar_period_open_date,
	calendar_period_close_date,
	cip_addition_batch_id,
	cip_adjustment_batch_id,
	cip_reclass_batch_id,
	cip_retirement_batch_id,
	cip_reval_batch_id,
	cip_transfer_batch_id,
	reval_batch_id,
	deprn_adjustment_batch_id,
	deprn_run,
	xla_conversion_status,
	gl_transfer_flag,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_DEPRN_PERIODS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_deprn_periods';