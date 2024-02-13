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

DROP TABLE if exists bec_ods.PO_LINE_TYPES_VL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_LINE_TYPES_VL
(
	attribute3 VARCHAR(150)   ENCODE lzo
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
	,outside_operation_flag VARCHAR(1)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,receive_close_tolerance NUMERIC(15,0)   ENCODE az64
	,line_type_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,order_type_lookup_code VARCHAR(25)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,category_id NUMERIC(15,0)  ENCODE az64
	,unit_of_measure VARCHAR(25)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,receiving_flag VARCHAR(1)   ENCODE lzo
	,inactive_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,line_type VARCHAR(30)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,purchase_basis VARCHAR(30)   ENCODE lzo
	,matching_basis VARCHAR(30)   ENCODE lzo
	,CLM_SEVERABLE_FLAG VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PO_LINE_TYPES_VL (
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
	outside_operation_flag,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	receive_close_tolerance,
	line_type_id,
	last_update_date,
	last_updated_by,
	order_type_lookup_code,
	last_update_login,
	creation_date,
	created_by,
	category_id,
	unit_of_measure,
	unit_price,
	receiving_flag,
	inactive_date,
	attribute_category,
	attribute1,
	attribute2,
	line_type,
	description,
	purchase_basis,
	matching_basis,
	CLM_SEVERABLE_FLAG,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
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
	outside_operation_flag,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	receive_close_tolerance,
	line_type_id,
	last_update_date,
	last_updated_by,
	order_type_lookup_code,
	last_update_login,
	creation_date,
	created_by,
	category_id,
	unit_of_measure,
	unit_price,
	receiving_flag,
	inactive_date,
	attribute_category,
	attribute1,
	attribute2,
	line_type,
	description,
	purchase_basis,
	matching_basis,
	CLM_SEVERABLE_FLAG,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PO_LINE_TYPES_VL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_line_types_vl';
	
COMMIT;
	
