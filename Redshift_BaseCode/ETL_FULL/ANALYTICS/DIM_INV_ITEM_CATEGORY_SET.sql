/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/


begin;
drop table if exists bec_dwh.DIM_INV_ITEM_CATEGORY_SET;

CREATE TABLE bec_dwh.DIM_INV_ITEM_CATEGORY_SET 
diststyle all 
sortkey(INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_ID,CATEGORY_SET_ID )
AS
(
SELECT    NVL(MIC.INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,
          NVL(MIC.ORGANIZATION_ID,0) as ORGANIZATION_ID,
          NVL(MIC.CATEGORY_SET_ID,0) as CATEGORY_SET_ID,
          NVL(MC.STRUCTURE_ID,0) as STRUCTURE_ID,
          NVL(MIC.CATEGORY_ID,0) as CATEGORY_ID,
          MSI.SEGMENT1 "ITEM_NAME",
          MSI.DESCRIPTION "ITEM_DESCRIPTION",
          CSET.CATEGORY_SET_NAME,
          CSET.DESCRIPTION "CATEGORY_SET_DESC",
          MC.SEGMENT1 "ITEM_CATEGORY_SEGMENT1",
          NVL (MC.SEGMENT2, 'LEVEL2') "ITEM_CATEGORY_SEGMENT2",
          NVL (MC.SEGMENT3, 'LEVEL3') "ITEM_CATEGORY_SEGMENT3",
          NVL (MC.SEGMENT4, 'LEVEL4') "ITEM_CATEGORY_SEGMENT4",
          MC.DESCRIPTION "ITEM_CATEGORY_DESC", 
		  'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(MIC.INVENTORY_ITEM_ID,0) || '-' || nvl(MIC.ORGANIZATION_ID, 0)  
|| '-' || nvl(MC.CATEGORY_ID, 0) 	
|| '-' || nvl(MIC.CATEGORY_SET_ID, 0) 
	 AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date

FROM      bec_ods.MTL_ITEM_CATEGORIES MIC,
          bec_ods.MTL_CATEGORY_SETS_TL CSET,
          bec_ods.MTL_SYSTEM_ITEMS_B MSI,
          bec_ods.MTL_CATEGORIES_B MC
WHERE   1=1 
        AND MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
        AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
        AND MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID
        AND MIC.CATEGORY_ID = MC.CATEGORY_ID
        AND CSET.LANGUAGE ='US'
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_item_category_set' and batch_name = 'ap';

COMMIT;	