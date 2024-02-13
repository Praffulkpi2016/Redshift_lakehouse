/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach FOR ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.CST_COST_ELEMENTS;

CREATE TABLE IF NOT EXISTS bec_ods.CST_COST_ELEMENTS
(
   COST_ELEMENT_ID NUMERIC(15,0) ENCODE az64,
LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 NOT NULL,
LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64,
CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 NOT NULL,
CREATED_BY NUMERIC(15,0) ENCODE az64,
LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64,
COST_ELEMENT VARCHAR(50) ENCODE lzo,
DESCRIPTION VARCHAR(240) ENCODE lzo,
REQUEST_ID NUMERIC(15,0) ENCODE az64,
PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64,
PROGRAM_ID NUMERIC(15,0) ENCODE az64,
PROGRAM_UPDATE_DATE DATE ENCODE az64,
ZD_EDITION_NAME VARCHAR(30) ENCODE lzo,
ZD_SYNC VARCHAR(30) ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CST_COST_ELEMENTS (
   COST_ELEMENT_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
COST_ELEMENT,
DESCRIPTION,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
ZD_EDITION_NAME,
ZD_SYNC,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
  COST_ELEMENT_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
COST_ELEMENT,
DESCRIPTION,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
ZD_EDITION_NAME,
ZD_SYNC,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CST_COST_ELEMENTS;

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_cost_elements';

COMMIT;