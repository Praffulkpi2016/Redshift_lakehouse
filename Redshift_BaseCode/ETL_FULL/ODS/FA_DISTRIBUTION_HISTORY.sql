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

DROP TABLE if exists bec_ods.fa_distribution_history;

CREATE TABLE IF NOT EXISTS bec_ods.fa_distribution_history
(
	DISTRIBUTION_ID NUMERIC(15,0)   ENCODE az64
	,BOOK_TYPE_CODE VARCHAR(15)   ENCODE lzo
	,ASSET_ID NUMERIC(15,0)   ENCODE az64
	,UNITS_ASSIGNED NUMERIC(28,10)   ENCODE az64
	,DATE_EFFECTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CODE_COMBINATION_ID NUMERIC(15,0)   ENCODE az64
	,LOCATION_ID NUMERIC(15,0)   ENCODE az64
	,TRANSACTION_HEADER_ID_IN NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,DATE_INEFFECTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ASSIGNED_TO NUMERIC(15,0)   ENCODE az64
	,TRANSACTION_HEADER_ID_OUT NUMERIC(15,0)   ENCODE az64
	,TRANSACTION_UNITS NUMERIC(28,10)   ENCODE az64
	,RETIREMENT_ID NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
	,CAPITAL_ADJ_ACCOUNT_CCID NUMERIC(15,0)   ENCODE az64
	,GENERAL_FUND_ACCOUNT_CCID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.fa_distribution_history (
	DISTRIBUTION_ID,
	BOOK_TYPE_CODE,
	ASSET_ID,
	UNITS_ASSIGNED,
	DATE_EFFECTIVE,
	CODE_COMBINATION_ID,
	LOCATION_ID,
	TRANSACTION_HEADER_ID_IN,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	DATE_INEFFECTIVE,
	ASSIGNED_TO,
	TRANSACTION_HEADER_ID_OUT,
	TRANSACTION_UNITS,
	RETIREMENT_ID,
	LAST_UPDATE_LOGIN,
	CAPITAL_ADJ_ACCOUNT_CCID,
	GENERAL_FUND_ACCOUNT_CCID,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		DISTRIBUTION_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		UNITS_ASSIGNED,
		DATE_EFFECTIVE,
		CODE_COMBINATION_ID,
		LOCATION_ID,
		TRANSACTION_HEADER_ID_IN,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		DATE_INEFFECTIVE,
		ASSIGNED_TO,
		TRANSACTION_HEADER_ID_OUT,
		TRANSACTION_UNITS,
		RETIREMENT_ID,
		LAST_UPDATE_LOGIN,
		CAPITAL_ADJ_ACCOUNT_CCID,
		GENERAL_FUND_ACCOUNT_CCID,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.fa_distribution_history;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_distribution_history';