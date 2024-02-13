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

DROP TABLE if exists bec_ods.BOM_STRUCTURES_B;

CREATE TABLE IF NOT EXISTS bec_ods.bom_structures_b
(
	assembly_item_id NUMERIC(15,0) ENCODE az64
	,organization_id NUMERIC(15,0) ENCODE az64
	,alternate_bom_designator VARCHAR(10)  ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0) ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0) ENCODE az64
	,last_update_login NUMERIC(15,0) ENCODE az64
	,common_assembly_item_id NUMERIC(15,0)  ENCODE az64
	,specific_assembly_comment VARCHAR(240)  ENCODE lzo
	,pending_from_ecn VARCHAR(10)  ENCODE lzo
	,attribute_category VARCHAR(30)  ENCODE lzo
	,attribute1 VARCHAR(150)  ENCODE lzo
	,attribute2 VARCHAR(150)  ENCODE lzo
	,attribute3 VARCHAR(150)  ENCODE lzo
	,attribute4 VARCHAR(150)  ENCODE lzo
	,attribute5 VARCHAR(150)  ENCODE lzo
	,attribute6 VARCHAR(150)  ENCODE lzo
	,attribute7 VARCHAR(150)  ENCODE lzo
	,attribute8 VARCHAR(150)  ENCODE lzo
	,attribute9 VARCHAR(150)  ENCODE lzo
	,attribute10 VARCHAR(150)  ENCODE lzo
	,attribute11 VARCHAR(150)  ENCODE lzo
	,attribute12 VARCHAR(150)  ENCODE lzo
	,attribute13 VARCHAR(150)  ENCODE lzo
	,attribute14 VARCHAR(150)  ENCODE lzo
	,attribute15 VARCHAR(150)  ENCODE lzo
	,assembly_type NUMERIC(15,0) ENCODE az64
	,common_bill_sequence_id NUMERIC(15,0)  ENCODE az64
	,bill_sequence_id NUMERIC(15,0) ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0) ENCODE az64
	,program_id NUMERIC(15,0)    ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,common_organization_id NUMERIC(15,0) ENCODE az64
	,next_explode_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,project_id NUMERIC(15,0) ENCODE az64
	,task_id NUMERIC(15,0)    ENCODE az64
	,original_system_reference VARCHAR(50)  ENCODE lzo
	,structure_type_id NUMERIC(15,0)    ENCODE az64
	,implementation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,obj_name VARCHAR(30)  ENCODE lzo
	,pk1_value VARCHAR(240)  ENCODE lzo
	,pk2_value VARCHAR(240)  ENCODE lzo
	,pk3_value VARCHAR(240)  ENCODE lzo
	,pk4_value VARCHAR(240)  ENCODE lzo
	,pk5_value VARCHAR(240)  ENCODE lzo
	,effectivity_control NUMERIC(15,0) ENCODE az64
	,is_preferred VARCHAR(1)  ENCODE lzo
	,source_bill_sequence_id NUMERIC(15,0)    ENCODE az64
	,KCA_OPERATION VARCHAR(10)  ENCODE lzo
    ,"IS_DELETED_FLG" VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.bom_structures_b (
assembly_item_id
,organization_id
,alternate_bom_designator
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,common_assembly_item_id
,specific_assembly_comment
,pending_from_ecn
,attribute_category
,attribute1
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,assembly_type
,common_bill_sequence_id
,bill_sequence_id
,request_id
,program_application_id
,program_id
,program_update_date
,common_organization_id
,next_explode_date
,project_id
,task_id
,original_system_reference
,structure_type_id
,implementation_date
,obj_name
,pk1_value
,pk2_value
,pk3_value
,pk4_value
,pk5_value
,effectivity_control
,is_preferred
,source_bill_sequence_id
,kca_operation
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date
)
    SELECT
assembly_item_id
,organization_id
,alternate_bom_designator
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,common_assembly_item_id
,specific_assembly_comment
,pending_from_ecn
,attribute_category
,attribute1
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,assembly_type
,common_bill_sequence_id
,bill_sequence_id
,request_id
,program_application_id
,program_id
,program_update_date
,common_organization_id
,next_explode_date
,project_id
,task_id
,original_system_reference
,structure_type_id
,implementation_date
,obj_name
,pk1_value
,pk2_value
,pk3_value
,pk4_value
,pk5_value
,effectivity_control
,is_preferred
,source_bill_sequence_id
,kca_operation
        ,'N' IS_DELETED_FLG  
        ,CAST(nullif(kca_seq_id, '') AS NUMERIC(36, 0)) AS kca_seq_id,
	kca_seq_date
    FROM
        bec_ods_stg.bom_structures_b;

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bom_structures_b'; 
	
commit;