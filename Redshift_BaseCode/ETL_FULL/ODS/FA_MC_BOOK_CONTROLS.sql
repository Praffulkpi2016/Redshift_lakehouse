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

DROP TABLE if exists bec_ods.FA_MC_BOOK_CONTROLS;

CREATE TABLE IF NOT EXISTS bec_ods.FA_MC_BOOK_CONTROLS
(
	set_of_books_id NUMERIC(15,0)   ENCODE az64
	,book_type_code VARCHAR(30)   ENCODE lzo
	,currency_code VARCHAR(15)   ENCODE lzo
	,deprn_status VARCHAR(1)   ENCODE lzo
	,deprn_request_id NUMERIC(15,0)   ENCODE az64
	,last_period_counter NUMERIC(15,0)   ENCODE az64
	,last_deprn_run_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_fiscal_year SMALLINT   ENCODE az64
	,retired_status VARCHAR(1)   ENCODE lzo
	,retired_request_id NUMERIC(15,0)   ENCODE az64
	,primary_set_of_books_id NUMERIC(15,0)   ENCODE az64
	,primary_currency_code VARCHAR(15)   ENCODE lzo
	,source_retired_status VARCHAR(1)   ENCODE lzo
	,source_retired_request_id NUMERIC(15,0)   ENCODE az64
	,mrc_converted_flag VARCHAR(1)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,nbv_amount_threshold NUMERIC(28,10)   ENCODE az64
	,conversion_status VARCHAR(1)   ENCODE lzo
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,mass_request_id NUMERIC(15,0)   ENCODE az64
	,allow_impairment_flag VARCHAR(1)   ENCODE lzo
	,gl_posting_allowed_flag VARCHAR(3)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_MC_BOOK_CONTROLS (
	set_of_books_id,
	book_type_code,
	currency_code,
	deprn_status,
	deprn_request_id,
	last_period_counter,
	last_deprn_run_date,
	current_fiscal_year,
	retired_status,
	retired_request_id,
	primary_set_of_books_id,
	primary_currency_code,
	source_retired_status,
	source_retired_request_id,
	mrc_converted_flag,
	enabled_flag,
	nbv_amount_threshold,
	conversion_status,
	last_updated_by,
	last_update_date,
	last_update_login,
	mass_request_id,
	allow_impairment_flag,
	gl_posting_allowed_flag,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date )
SELECT
	set_of_books_id,
	book_type_code,
	currency_code,
	deprn_status,
	deprn_request_id,
	last_period_counter,
	last_deprn_run_date,
	current_fiscal_year,
	retired_status,
	retired_request_id,
	primary_set_of_books_id,
	primary_currency_code,
	source_retired_status,
	source_retired_request_id,
	mrc_converted_flag,
	enabled_flag,
	nbv_amount_threshold,
	conversion_status,
	last_updated_by,
	last_update_date,
	last_update_login,
	mass_request_id,
	allow_impairment_flag,
	gl_posting_allowed_flag,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FA_MC_BOOK_CONTROLS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_mc_book_controls';