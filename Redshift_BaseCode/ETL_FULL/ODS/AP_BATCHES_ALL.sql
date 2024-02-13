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

DROP TABLE IF EXISTS bec_ods.ap_batches_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_batches_all
(
	batch_id NUMERIC(15,0)   ENCODE az64
	,batch_name VARCHAR(50)   ENCODE lzo
	,batch_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,control_invoice_count NUMERIC(15,0)   ENCODE az64
	,control_invoice_total NUMERIC(28,10)   ENCODE az64
	,actual_invoice_count NUMERIC(15,0)   ENCODE az64
	,actual_invoice_total NUMERIC(28,10)   ENCODE az64
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,payment_currency_code VARCHAR(15)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,pay_group_lookup_code VARCHAR(25)   ENCODE lzo
	,batch_code_combination_id NUMERIC(15,0)   ENCODE az64
	,terms_id NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,invoice_type_lookup_code VARCHAR(25)   ENCODE lzo
	,hold_lookup_code VARCHAR(25)   ENCODE lzo
	,hold_reason VARCHAR(240)   ENCODE lzo
	,doc_category_code VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payment_priority NUMERIC(2,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_batches_all (
    batch_id,
    batch_name,
    batch_date,
    last_update_date,
    last_updated_by,
    control_invoice_count,
    control_invoice_total,
    actual_invoice_count,
    actual_invoice_total,
    invoice_currency_code,
    payment_currency_code,
    last_update_login,
    creation_date,
    created_by,
    pay_group_lookup_code,
    batch_code_combination_id,
    terms_id,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    invoice_type_lookup_code,
    hold_lookup_code,
    hold_reason,
    doc_category_code,
    org_id,
    gl_date,
    payment_priority,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        batch_id,
        batch_name,
        batch_date,
        last_update_date,
        last_updated_by,
        control_invoice_count,
        control_invoice_total,
        actual_invoice_count,
        actual_invoice_total,
        invoice_currency_code,
        payment_currency_code,
        last_update_login,
        creation_date,
        created_by,
        pay_group_lookup_code,
        batch_code_combination_id,
        terms_id,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        invoice_type_lookup_code,
        hold_lookup_code,
        hold_reason,
        doc_category_code,
        org_id,
        gl_date,
        payment_priority,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.ap_batches_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_batches_all'; 
	
commit;