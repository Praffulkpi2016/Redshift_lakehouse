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

DROP TABLE if exists bec_ods.POBV_BUYERS;

CREATE TABLE IF NOT EXISTS bec_ods.POBV_BUYERS
(
	 buyer_id NUMERIC(9,0)   ENCODE az64
	,buyer_employee_number VARCHAR(30)   ENCODE lzo
	,buyer_name VARCHAR(240)   ENCODE lzo
	,authorization_limit NUMERIC(15,0)   ENCODE az64
	,start_effective_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_effective_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
	,location_id NUMERIC(15,0)   ENCODE az64
	,created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_updated_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.POBV_BUYERS (
   buyer_id,
	buyer_employee_number,
	buyer_name,
	authorization_limit,
	start_effective_date,
	end_effective_date,
	category_id,
	location_id,
	created_date,
	created_by,
	last_updated_date,
	last_updated_by,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
     buyer_id,
	buyer_employee_number,
	buyer_name,
	authorization_limit,
	start_effective_date,
	end_effective_date,
	category_id,
	location_id,
	created_date,
	created_by,
	last_updated_date,
	last_updated_by,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.POBV_BUYERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'pobv_buyers';
	
commit;