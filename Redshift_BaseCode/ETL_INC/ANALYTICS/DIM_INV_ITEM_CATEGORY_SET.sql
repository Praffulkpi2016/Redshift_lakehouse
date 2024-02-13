/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
-- Delete Records
DELETE FROM bec_dwh.DIM_INV_ITEM_CATEGORY_SET
WHERE EXISTS (
SELECT 1
FROM bec_ods.MTL_ITEM_CATEGORIES MIC
INNER JOIN bec_ods.MTL_CATEGORY_SETS_TL CSET ON MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID 
AND CSET.LANGUAGE = 'US'
INNER JOIN bec_ods.MTL_SYSTEM_ITEMS_B MSI ON MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID 
AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
INNER JOIN bec_ods.MTL_CATEGORIES_B MC ON MIC.CATEGORY_ID = MC.CATEGORY_ID
WHERE 
    NVL(MIC.INVENTORY_ITEM_ID, 0) = NVL(dim_inv_item_category_set.INVENTORY_ITEM_ID, 0)
    AND NVL(MIC.ORGANIZATION_ID, 0) = NVL(dim_inv_item_category_set.ORGANIZATION_ID, 0)
    AND NVL(MIC.CATEGORY_ID, 0) = NVL(dim_inv_item_category_set.CATEGORY_ID, 0)
    AND NVL(MIC.CATEGORY_SET_ID, 0) = NVL(dim_inv_item_category_set.CATEGORY_SET_ID, 0)
    AND (MIC.kca_seq_date > (
        SELECT (executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap'
    ) OR MC.kca_seq_date > (
        SELECT (executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap'
    ))
);
commit;
-- Insert records

insert into bec_dwh.DIM_INV_ITEM_CATEGORY_SET
(
INVENTORY_ITEM_ID ,
ORGANIZATION_ID,
CATEGORY_SET_ID,
STRUCTURE_ID,
CATEGORY_ID,
ITEM_NAME,
ITEM_DESCRIPTION,
CATEGORY_SET_NAME,
CATEGORY_SET_DESC,
ITEM_CATEGORY_SEGMENT1,
ITEM_CATEGORY_SEGMENT2,
ITEM_CATEGORY_SEGMENT3,
ITEM_CATEGORY_SEGMENT4,
ITEM_CATEGORY_DESC,
is_deleted_flg,
SOURCE_APP_ID,
DW_LOAD_ID,
DW_INSERT_DATE,
DW_UPDATE_DATE
)
(
SELECT    
          nvl(MIC.INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,
          nvl(MIC.ORGANIZATION_ID,0) as ORGANIZATION_ID,
          nvl(MIC.CATEGORY_SET_ID,0) as CATEGORY_SET_ID ,
          nvl(MC.STRUCTURE_ID,0) as STRUCTURE_ID,
          nvl(MIC.CATEGORY_ID,0) as CATEGORY_ID,
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
    )|| '-'|| nvl(MIC.INVENTORY_ITEM_ID,0) || '-' || nvl(MIC.ORGANIZATION_ID, 0) 
|| '-' || nvl(MC.CATEGORY_ID, 0)
	|| '-' || nvl(MIC.CATEGORY_SET_ID, 0)  	 AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date

FROM      bec_ods.MTL_ITEM_CATEGORIES MIC,
          bec_ods.MTL_CATEGORY_SETS_TL CSET,
          bec_ods.MTL_SYSTEM_ITEMS_B MSI,
          bec_ods.MTL_CATEGORIES_B MC
WHERE   1=1 AND CSET.LANGUAGE ='US'
		and (MIC.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='dim_inv_item_category_set' and batch_name = 'ap')
OR
MC.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='dim_inv_item_category_set' and batch_name = 'ap')
)
AND MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
AND MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID
AND MIC.CATEGORY_ID = MC.CATEGORY_ID
);
commit;
-- soft Delete Records
update bec_dwh.dim_inv_item_category_set set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
SELECT 1
FROM bec_ods.MTL_ITEM_CATEGORIES MIC
INNER JOIN bec_ods.MTL_CATEGORY_SETS_TL CSET ON MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID 
AND CSET.LANGUAGE = 'US'
INNER JOIN bec_ods.MTL_SYSTEM_ITEMS_B MSI ON MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID 
AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
INNER JOIN bec_ods.MTL_CATEGORIES_B MC ON MIC.CATEGORY_ID = MC.CATEGORY_ID
WHERE 
    NVL(MIC.INVENTORY_ITEM_ID, 0) = NVL(dim_inv_item_category_set.INVENTORY_ITEM_ID, 0)
    AND NVL(MIC.ORGANIZATION_ID, 0) = NVL(dim_inv_item_category_set.ORGANIZATION_ID, 0)
    AND NVL(MIC.CATEGORY_ID, 0) = NVL(dim_inv_item_category_set.CATEGORY_ID, 0)
    AND NVL(MIC.CATEGORY_SET_ID, 0) = NVL(dim_inv_item_category_set.CATEGORY_SET_ID, 0)
	AND (MIC.is_deleted_flg <> 'Y' or MC.is_deleted_flg <> 'Y' or MSI.is_deleted_flg <> 'Y')
);
commit;

end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_item_category_set'
and batch_name = 'ap';

COMMIT;