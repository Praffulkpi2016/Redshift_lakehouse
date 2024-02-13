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

DROP TABLE IF EXISTS bec_ods.MSC_BOMS;

CREATE TABLE if NOT EXISTS bec_ods.MSC_BOMS
(
	     plan_id NUMERIC(15,0)   ENCODE az64
	,bill_sequence_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,assembly_type NUMERIC(15,0)   ENCODE az64
	,alternate_bom_designator VARCHAR(40)   ENCODE lzo
	,specific_assembly_comment VARCHAR(240)   ENCODE lzo
	,pending_from_ecn VARCHAR(10)   ENCODE lzo
	,scaling_type NUMERIC(15,0)   ENCODE az64
	,assembly_quantity NUMERIC(28,10)   ENCODE az64
	,uom VARCHAR(3)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,assembly_item_id NUMERIC(15,0)   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
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
	,yielding_op_seq_num NUMERIC(15,0)   ENCODE az64
	,new_plan_id NUMERIC(15,0)   ENCODE az64
	,new_plan_list VARCHAR(256)   ENCODE lzo
	,applied NUMERIC(15,0)   ENCODE az64
	,simulation_set_id NUMERIC(15,0)   ENCODE az64
	,repairable NUMERIC(15,0)   ENCODE az64
	,activity_item_id NUMERIC(15,0)   ENCODE az64
	,"alternate_bom_designator#1" VARCHAR(40)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_BOMS (
    PLAN_ID
,      BILL_SEQUENCE_ID
,      SR_INSTANCE_ID
,      ASSEMBLY_TYPE
,      ALTERNATE_BOM_DESIGNATOR
,      SPECIFIC_ASSEMBLY_COMMENT
,      PENDING_FROM_ECN
,      SCALING_TYPE
,      ASSEMBLY_QUANTITY
,      UOM
,      ORGANIZATION_ID
,      ASSEMBLY_ITEM_ID
,      REFRESH_NUMBER
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      YIELDING_OP_SEQ_NUM
,      NEW_PLAN_ID
,      NEW_PLAN_LIST
,      APPLIED
,      SIMULATION_SET_ID
,      REPAIRABLE
,      ACTIVITY_ITEM_ID
,"alternate_bom_designator#1"
	,KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
       PLAN_ID
,      BILL_SEQUENCE_ID
,      SR_INSTANCE_ID
,      ASSEMBLY_TYPE
,      ALTERNATE_BOM_DESIGNATOR
,      SPECIFIC_ASSEMBLY_COMMENT
,      PENDING_FROM_ECN
,      SCALING_TYPE
,      ASSEMBLY_QUANTITY
,      UOM
,      ORGANIZATION_ID
,      ASSEMBLY_ITEM_ID
,      REFRESH_NUMBER
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      YIELDING_OP_SEQ_NUM
,      NEW_PLAN_ID
,      NEW_PLAN_LIST
,      APPLIED
,      SIMULATION_SET_ID
,      REPAIRABLE
,      ACTIVITY_ITEM_ID
,"alternate_bom_designator#1"
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.MSC_BOMS;

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_boms';
	
commit;