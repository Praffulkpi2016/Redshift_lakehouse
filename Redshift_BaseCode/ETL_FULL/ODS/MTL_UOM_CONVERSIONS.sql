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

DROP TABLE if exists bec_ods.MTL_UOM_CONVERSIONS ;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_UOM_CONVERSIONS 
(
	unit_of_measure VARCHAR(25)   ENCODE lzo
	,uom_code VARCHAR(3)   ENCODE lzo
	,uom_class VARCHAR(10)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,conversion_rate NUMERIC(28,10)   ENCODE az64
	,default_conversion_flag VARCHAR(1)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,length NUMERIC(28,10)   ENCODE az64
	,width NUMERIC(28,10)   ENCODE az64
	,height NUMERIC(28,10)   ENCODE az64
	,dimension_uom VARCHAR(3)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_UOM_CONVERSIONS  (
    unit_of_measure,
	uom_code,
	uom_class,
	inventory_item_id,
	conversion_rate,
	default_conversion_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	length,
	width,
	height,
	dimension_uom,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date	
)
    SELECT
    unit_of_measure,
	uom_code,
	uom_class,
	inventory_item_id,
	conversion_rate,
	default_conversion_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	length,
	width,
	height,
	dimension_uom,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.MTL_UOM_CONVERSIONS ;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_uom_conversions ';
	
commit;