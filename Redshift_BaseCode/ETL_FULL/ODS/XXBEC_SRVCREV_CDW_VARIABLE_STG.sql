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

DROP TABLE if exists bec_ods.XXBEC_SRVCREV_CDW_VARIABLE_STG;

CREATE TABLE IF NOT EXISTS bec_ods.XXBEC_SRVCREV_CDW_VARIABLE_STG
(
	source VARCHAR(500)   ENCODE lzo
	,contract_group VARCHAR(500)   ENCODE lzo
	,contract_id NUMERIC(15,0)   ENCODE az64
	,customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,gl_dist_id NUMERIC(15,0)   ENCODE az64
	,event_id NUMERIC(15,0)   ENCODE az64
	,actual_forecast VARCHAR(500)   ENCODE lzo
	,ledger_id NUMERIC(15,0)   ENCODE az64
	,ledger_name VARCHAR(500)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,org_name VARCHAR(250)   ENCODE lzo
	,site_id VARCHAR(500)   ENCODE lzo
	,revenue_type VARCHAR(250)   ENCODE lzo
	,transaction_currency VARCHAR(50)   ENCODE lzo
	,accounted_currency VARCHAR(50)   ENCODE lzo
	,period_name VARCHAR(50)   ENCODE lzo
	,revenue_amount_trans_curr NUMERIC(28,10)   ENCODE az64
	,revenue_amount_acctd_curr NUMERIC(28,10)   ENCODE az64
	,conversion_rate NUMERIC(15,0)   ENCODE az64
	,tolling_rates_trans_curr NUMERIC(28,10)   ENCODE az64
	,tolling_rates_acctd_curr NUMERIC(28,10)   ENCODE az64
	,tmo_prcntge NUMERIC(28,10)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,extract_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,from_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,to_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,period_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,no_days NUMERIC(15,0)   ENCODE az64
	,query_name VARCHAR(50)   ENCODE lzo
	,kwh VARCHAR(250)   ENCODE lzo
	,kwh_type VARCHAR(250)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.XXBEC_SRVCREV_CDW_VARIABLE_STG (
    "source",
	contract_group,
	contract_id,
	customer_trx_line_id,
	gl_dist_id,
	event_id,
	actual_forecast,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	revenue_type,
	transaction_currency,
	accounted_currency,
	period_name,
	revenue_amount_trans_curr,
	revenue_amount_acctd_curr,
	conversion_rate,
	tolling_rates_trans_curr,
	tolling_rates_acctd_curr,
	tmo_prcntge,
	last_update_date,
	extract_date,
	from_date,
	to_date,
	period_end_date,
	no_days,
	query_name,
	kwh,
	kwh_type,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
  "source",
	contract_group,
	contract_id,
	customer_trx_line_id,
	gl_dist_id,
	event_id,
	actual_forecast,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	revenue_type,
	transaction_currency,
	accounted_currency,
	period_name,
	revenue_amount_trans_curr,
	revenue_amount_acctd_curr,
	conversion_rate,
	tolling_rates_trans_curr,
	tolling_rates_acctd_curr,
	tmo_prcntge,
	last_update_date,
	extract_date,
	from_date,
	to_date,
	period_end_date,
	no_days,
	query_name,
	kwh,
	kwh_type,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.XXBEC_SRVCREV_CDW_VARIABLE_STG;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xxbec_srvcrev_cdw_variable_stg';
	
COMMIT;