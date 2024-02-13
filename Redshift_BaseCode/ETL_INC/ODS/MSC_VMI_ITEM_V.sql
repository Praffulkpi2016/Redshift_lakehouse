/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;

truncate table bec_ods.MSC_VMI_ITEM_V ;

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



