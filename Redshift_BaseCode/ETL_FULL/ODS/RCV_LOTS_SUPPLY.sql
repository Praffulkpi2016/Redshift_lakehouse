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

DROP TABLE if exists bec_ods.RCV_LOTS_SUPPLY;

CREATE TABLE IF NOT EXISTS bec_ods.RCV_LOTS_SUPPLY
(
     
	supply_type_code VARCHAR(25)   ENCODE lzo
	,lot_num VARCHAR(80)   ENCODE lzo
	,quantity NUMERIC(28,10)   ENCODE az64
	,primary_quantity NUMERIC(28,10)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_line_id NUMERIC(15,0)   ENCODE az64
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,secondary_quantity NUMERIC(28,10)   ENCODE az64
	,sublot_num VARCHAR(32)   ENCODE lzo
	,reason_code VARCHAR(4)   ENCODE lzo
	,reason_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RCV_LOTS_SUPPLY (
	
	supply_type_code,
	lot_num,
	quantity,
	primary_quantity,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	shipment_line_id,
	transaction_id,
	expiration_date,
	secondary_quantity,
	sublot_num,
	reason_code,
	reason_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
     
	supply_type_code,
	lot_num,
	quantity,
	primary_quantity,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	shipment_line_id,
	transaction_id,
	expiration_date,
	secondary_quantity,
	sublot_num,
	reason_code,
	reason_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.RCV_LOTS_SUPPLY;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'rcv_lots_supply';
	
COMMIT;
	
	 