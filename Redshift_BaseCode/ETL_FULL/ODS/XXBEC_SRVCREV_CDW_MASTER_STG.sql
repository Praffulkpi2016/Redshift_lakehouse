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

DROP TABLE if exists bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG;

CREATE TABLE IF NOT EXISTS bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG
(
	source VARCHAR(500)   ENCODE lzo
	,contract_group VARCHAR(500)   ENCODE lzo
	,contract_id BIGINT   ENCODE az64
	,ledger_id BIGINT   ENCODE az64
	,ledger_name VARCHAR(500)   ENCODE lzo
	,org_id BIGINT   ENCODE az64
	,org_name VARCHAR(500)   ENCODE lzo
	,site_id VARCHAR(500)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,extract_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,site_use_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG (
   "source",
	contract_group,
	contract_id,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	start_date,
	end_date,
	last_update_date,
	extract_date,
	site_use_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
 "source",
	contract_group,
	contract_id,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	start_date,
	end_date,
	last_update_date,
	extract_date,
	site_use_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xxbec_srvcrev_cdw_master_stg';
	
COMMIT;