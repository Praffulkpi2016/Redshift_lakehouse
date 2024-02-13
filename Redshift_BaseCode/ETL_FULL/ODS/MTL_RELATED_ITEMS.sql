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

DROP TABLE if exists bec_ods.MTL_RELATED_ITEMS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_RELATED_ITEMS
(
	 inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,related_item_id NUMERIC(15,0)   ENCODE az64
	,relationship_type_id NUMERIC(15,0)   ENCODE az64
	,reciprocal_flag VARCHAR(1)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,object_version_number INTEGER   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attr_context VARCHAR(30)   ENCODE lzo
	,attr_char1 VARCHAR(150)   ENCODE lzo
	,attr_char2 VARCHAR(150)   ENCODE lzo
	,attr_char3 VARCHAR(150)   ENCODE lzo
	,attr_char4 VARCHAR(150)   ENCODE lzo
	,attr_char5 VARCHAR(150)   ENCODE lzo
	,attr_char6 VARCHAR(150)   ENCODE lzo
	,attr_char7 VARCHAR(150)   ENCODE lzo
	,attr_char8 VARCHAR(150)   ENCODE lzo
	,attr_char9 VARCHAR(150)   ENCODE lzo
	,attr_char10 VARCHAR(150)   ENCODE lzo
	,attr_num1 VARCHAR(150)   ENCODE lzo
	,attr_num2 VARCHAR(150)   ENCODE lzo
	,attr_num3 VARCHAR(150)   ENCODE lzo
	,attr_num4 VARCHAR(150)   ENCODE lzo
	,attr_num5 VARCHAR(150)   ENCODE lzo
	,attr_num6 VARCHAR(150)   ENCODE lzo
	,attr_num7 VARCHAR(150)   ENCODE lzo
	,attr_num8 VARCHAR(150)   ENCODE lzo
	,attr_num9 VARCHAR(150)   ENCODE lzo
	,attr_num10 VARCHAR(150)   ENCODE lzo
	,attr_date1 VARCHAR(150)   ENCODE lzo
	,attr_date2 VARCHAR(150)   ENCODE lzo
	,attr_date3 VARCHAR(150)   ENCODE lzo
	,attr_date4 VARCHAR(150)   ENCODE lzo
	,attr_date5 VARCHAR(150)   ENCODE lzo
	,attr_date6 VARCHAR(150)   ENCODE lzo
	,attr_date7 VARCHAR(150)   ENCODE lzo
	,attr_date8 VARCHAR(150)   ENCODE lzo
	,attr_date9 VARCHAR(150)   ENCODE lzo
	,attr_date10 VARCHAR(150)   ENCODE lzo
	,planning_enabled_flag VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_RELATED_ITEMS (
	inventory_item_id,
	organization_id,
	related_item_id,
	relationship_type_id,
	reciprocal_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	object_version_number,
	start_date,
	end_date,
	attr_context,
	attr_char1,
	attr_char2,
	attr_char3,
	attr_char4,
	attr_char5,
	attr_char6,
	attr_char7,
	attr_char8,
	attr_char9,
	attr_char10,
	attr_num1,
	attr_num2,
	attr_num3,
	attr_num4,
	attr_num5,
	attr_num6,
	attr_num7,
	attr_num8,
	attr_num9,
	attr_num10,
	attr_date1,
	attr_date2,
	attr_date3,
	attr_date4,
	attr_date5,
	attr_date6,
	attr_date7,
	attr_date8,
	attr_date9,
	attr_date10,
	planning_enabled_flag,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
 SELECT
	inventory_item_id,
	organization_id,
	related_item_id,
	relationship_type_id,
	reciprocal_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	object_version_number,
	start_date,
	end_date,
	attr_context,
	attr_char1,
	attr_char2,
	attr_char3,
	attr_char4,
	attr_char5,
	attr_char6,
	attr_char7,
	attr_char8,
	attr_char9,
	attr_char10,
	attr_num1,
	attr_num2,
	attr_num3,
	attr_num4,
	attr_num5,
	attr_num6,
	attr_num7,
	attr_num8,
	attr_num9,
	attr_num10,
	attr_date1,
	attr_date2,
	attr_date3,
	attr_date4,
	attr_date5,
	attr_date6,
	attr_date7,
	attr_date8,
	attr_date9,
	attr_date10,
	planning_enabled_flag,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.MTL_RELATED_ITEMS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_related_items';

COMMIT;