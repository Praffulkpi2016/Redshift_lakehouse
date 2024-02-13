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

DROP TABLE if exists bec_ods.OKS_BILL_TRANSACTIONS;

CREATE TABLE IF NOT EXISTS bec_ods.OKS_BILL_TRANSACTIONS
(
	id VARCHAR(50)   ENCODE lzo
	,currency_code VARCHAR(45)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,trx_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,trx_number VARCHAR(60)   ENCODE lzo
	,trx_amount NUMERIC(28,10)   ENCODE az64
	,trx_class VARCHAR(60)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OKS_BILL_TRANSACTIONS (
	id,
	currency_code,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	trx_date,
	trx_number,
	trx_amount,
	trx_class,
	last_update_login,
	security_group_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	id,
	currency_code,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	trx_date,
	trx_number,
	trx_amount,
	trx_class,
	last_update_login,
	security_group_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.OKS_BILL_TRANSACTIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oks_bill_transactions';
	
commit;