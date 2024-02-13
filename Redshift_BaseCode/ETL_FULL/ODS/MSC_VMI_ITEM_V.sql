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

DROP TABLE if exists bec_ods.MSC_VMI_ITEM_V ;

CREATE TABLE bec_ods.MSC_VMI_ITEM_V 
(
PLAN_ID NUMERIC(15,0)   ENCODE az64
,SR_INSTANCE_ID NUMERIC(15,0)   ENCODE az64
,ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64
,INVENTORY_ITEM_ID NUMERIC(15,0)   ENCODE az64
,SUPPLIER_ID NUMERIC(15,0)   ENCODE az64
,SUPPLIER_SITE_ID NUMERIC(15,0)   ENCODE az64
,ITEM_NAME VARCHAR(250)   ENCODE lzo
,DESCRIPTION VARCHAR(240)   ENCODE lzo
,PLANNER_CODE VARCHAR(10)   ENCODE lzo
,BUYER_CODE VARCHAR(4000)   ENCODE lzo
,"IS_DELETED_FLG" VARCHAR(2) ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,kca_seq_id NUMERIC(36,0)
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64

)
DISTSTYLE
auto;

-- Insert Records 

INSERT INTO bec_ods.MSC_VMI_ITEM_V
select 
PLAN_ID
,SR_INSTANCE_ID
,ORGANIZATION_ID
,INVENTORY_ITEM_ID
,SUPPLIER_ID
,SUPPLIER_SITE_ID
,ITEM_NAME
,DESCRIPTION
,PLANNER_CODE
,BUYER_CODE
,'N' IS_DELETED_FLG
,kca_operation
,CAST(nullif(kca_seq_id, '') AS NUMERIC(36,0)) AS kca_seq_id
,kca_seq_date
FROM bec_ods_stg.MSC_VMI_ITEM_V;
end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_vmi_item_v';
	
commit;



