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

DROP TABLE IF EXISTS bec_ods.ap_payment_hist_dists;

CREATE TABLE if NOT EXISTS bec_ods.ap_payment_hist_dists
(
	payment_hist_dist_id NUMERIC(15,0)   ENCODE az64
	,accounting_event_id NUMERIC(15,0)   ENCODE az64
	,pay_dist_lookup_code VARCHAR(30)   ENCODE lzo
	,invoice_distribution_id NUMERIC(15,0)   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,payment_history_id NUMERIC(15,0)   ENCODE az64
	,invoice_payment_id NUMERIC(15,0)   ENCODE az64
	,bank_curr_amount NUMERIC(28,10)   ENCODE az64
	,cleared_base_amount NUMERIC(28,10)   ENCODE az64
	,historical_flag VARCHAR(1)   ENCODE lzo
	,invoice_dist_amount NUMERIC(28,10)   ENCODE az64
	,invoice_dist_base_amount NUMERIC(28,10)   ENCODE az64
	,invoice_adjustment_event_id NUMERIC(15,0)   ENCODE az64
	,matured_base_amount NUMERIC(28,10)   ENCODE az64
	,paid_base_amount NUMERIC(28,10)   ENCODE az64
	,rounding_amt NUMERIC(28,10)   ENCODE az64
	,reversal_flag VARCHAR(1)   ENCODE lzo
	,reversed_pay_hist_dist_id NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_login_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,awt_related_id NUMERIC(15,0)   ENCODE az64
	,release_inv_dist_derived_from NUMERIC(15,0)   ENCODE az64
	,pa_addition_flag VARCHAR(1)   ENCODE lzo
	,amount_variance NUMERIC(28,10)   ENCODE az64
	,invoice_base_amt_variance NUMERIC(28,10)   ENCODE az64
	,quantity_variance NUMERIC(28,10)   ENCODE az64
	,invoice_base_qty_variance NUMERIC(28,10)   ENCODE az64
	,GAIN_LOSS_INDICATOR VARCHAR(1) ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_payment_hist_dists (
    payment_hist_dist_id,
    accounting_event_id,
    pay_dist_lookup_code,
    invoice_distribution_id,
    amount,
    payment_history_id,
    invoice_payment_id,
    bank_curr_amount,
    cleared_base_amount,
    historical_flag,
    invoice_dist_amount,
    invoice_dist_base_amount,
    invoice_adjustment_event_id,
    matured_base_amount,
    paid_base_amount,
    rounding_amt,
    reversal_flag,
    reversed_pay_hist_dist_id,
    created_by,
    creation_date,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_login_id,
    program_update_date,
    request_id,
    awt_related_id,
    release_inv_dist_derived_from,
    pa_addition_flag,
    amount_variance,
    invoice_base_amt_variance,
    quantity_variance,
    invoice_base_qty_variance,
	GAIN_LOSS_INDICATOR,
	KCA_OPERATION,
    IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    (SELECT
        payment_hist_dist_id,
        accounting_event_id,
        pay_dist_lookup_code,
        invoice_distribution_id,
        amount,
        payment_history_id,
        invoice_payment_id,
        bank_curr_amount,
        cleared_base_amount,
        historical_flag,
        invoice_dist_amount,
        invoice_dist_base_amount,
        invoice_adjustment_event_id,
        matured_base_amount,
        paid_base_amount,
        rounding_amt,
        reversal_flag,
        reversed_pay_hist_dist_id,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_application_id,
        program_id,
        program_login_id,
        program_update_date,
        request_id,
        awt_related_id,
        release_inv_dist_derived_from,
        pa_addition_flag,
        amount_variance,
        invoice_base_amt_variance,
        quantity_variance,
        invoice_base_qty_variance,
		GAIN_LOSS_INDICATOR,
        KCA_OPERATION,
       'N' as IS_DELETED_FLG,
	   cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	   kca_seq_date
    FROM
        bec_ods_stg.ap_payment_hist_dists);
		
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_payment_hist_dists';
	
commit;