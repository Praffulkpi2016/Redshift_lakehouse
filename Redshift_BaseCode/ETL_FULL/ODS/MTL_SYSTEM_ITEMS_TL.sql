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

DROP TABLE if exists bec_ods.MTL_SYSTEM_ITEMS_TL;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_SYSTEM_ITEMS_TL
(

    inventory_item_id NUMERIC(15,0)   ENCODE az64 ,
organization_id NUMERIC(15,0)   ENCODE az64,
"language" VARCHAR(4) ENCODE lzo ,
source_lang VARCHAR(4) ENCODE lzo ,
description VARCHAR(240) ENCODE lzo,
last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
last_updated_by NUMERIC(15,0)   ENCODE az64,
creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
created_by NUMERIC(15,0)   ENCODE az64,
last_update_login NUMERIC(15,0)   ENCODE az64,
long_description VARCHAR(4000)
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
 ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
 ,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 
  
 )
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_SYSTEM_ITEMS_TL (
 inventory_item_id,
	organization_id,
	"language",
	source_lang,
	description,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	long_description,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
 inventory_item_id,
	organization_id,
	"language",
	source_lang,
	description,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	long_description,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_SYSTEM_ITEMS_TL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_system_items_tl';
	
commit;