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

DROP TABLE if exists bec_ods.PO_LINE_TYPES_TL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_LINE_TYPES_TL
(
	line_type_id NUMERIC(15,0)   ENCODE az64
	,"language" VARCHAR(4)   ENCODE lzo
	,source_lang VARCHAR(4)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,line_type VARCHAR(25)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,zd_edition_name VARCHAR(30)   ENCODE lzo
	,zd_sync VARCHAR(30)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;



INSERT INTO bec_ods.PO_LINE_TYPES_TL (
	line_type_id,
	"language",
	source_lang,
	description,
	line_type,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	ZD_EDITION_NAME ,
	ZD_SYNC,	
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
     line_type_id,
	"language",
	source_lang,
	description,
	line_type,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,	
	ZD_EDITION_NAME ,
	ZD_SYNC,	
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PO_LINE_TYPES_TL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_line_types_tl';
	
commit;