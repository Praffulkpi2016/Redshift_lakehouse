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

DROP TABLE if exists bec_ods.OKS_BILL_TXN_LINES;

CREATE TABLE IF NOT EXISTS bec_ods.oks_bill_txn_lines
(
	id VARCHAR(50)   ENCODE lzo
	,btn_id VARCHAR(50)   ENCODE lzo
	,bsl_id VARCHAR(50)   ENCODE lzo
	,bcl_id VARCHAR(50)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,bill_instance_number NUMERIC(15,0)   ENCODE az64
	,trx_line_amount VARCHAR(240)   ENCODE lzo
	,trx_line_tax_amount NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(90)   ENCODE lzo
	,attribute1 VARCHAR(450)   ENCODE lzo
	,attribute2 VARCHAR(450)   ENCODE lzo
	,attribute3 VARCHAR(450)   ENCODE lzo
	,attribute4 VARCHAR(450)   ENCODE lzo
	,attribute5 VARCHAR(450)   ENCODE lzo
	,attribute6 VARCHAR(450)   ENCODE lzo
	,attribute7 VARCHAR(450)   ENCODE lzo
	,attribute8 VARCHAR(450)   ENCODE lzo
	,attribute9 VARCHAR(450)   ENCODE lzo
	,attribute10 VARCHAR(450)   ENCODE lzo
	,attribute11 VARCHAR(450)   ENCODE lzo
	,attribute12 VARCHAR(450)   ENCODE lzo
	,attribute13 VARCHAR(450)   ENCODE lzo
	,attribute14 VARCHAR(450)   ENCODE lzo
	,attribute15 VARCHAR(450)   ENCODE lzo
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,trx_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,trx_number VARCHAR(60)   ENCODE lzo
	,trx_class VARCHAR(60)   ENCODE lzo
	,manual_credit NUMERIC(15,0)   ENCODE az64
	,trx_amount NUMERIC(28,10)   ENCODE az64
	,split_flag VARCHAR(1)   ENCODE lzo
	,cycle_refrence VARCHAR(120)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.oks_bill_txn_lines (
	id,
	btn_id,
	bsl_id,
	bcl_id,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	bill_instance_number,
	trx_line_amount,
	trx_line_tax_amount,
	last_update_login,
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
	security_group_id,
	trx_date,
	trx_number,
	trx_class,
	manual_credit,
	trx_amount,
	split_flag,
	cycle_refrence,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	id,
	btn_id,
	bsl_id,
	bcl_id,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	bill_instance_number,
	trx_line_amount,
	trx_line_tax_amount,
	last_update_login,
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
	security_group_id,
	trx_date,
	trx_number,
	trx_class,
	manual_credit,
	trx_amount,
	split_flag,
	cycle_refrence,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.oks_bill_txn_lines;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oks_bill_txn_lines';
	
commit;