/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records
DELETE FROM bec_dwh.dim_abc_classes
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.MTL_ABC_ASSIGNMENT_GROUPS maag
    JOIN bec_ods.MTL_ABC_ASSIGNMENTS maa ON maag.assignment_group_id = maa.assignment_group_id
    JOIN bec_ods.MTL_ABC_CLASSES mac ON maa.abc_class_id = mac.abc_class_id
    WHERE maag.kca_seq_date > (
            SELECT (executebegints - prune_days) 
            FROM bec_etl_ctrl.batch_dw_info 
            WHERE dw_table_name = 'dim_abc_classes' 
              AND batch_name = 'inv'
        )
        AND dim_abc_classes.assignment_group_id = maag.assignment_group_id
        AND dim_abc_classes.abc_class_id = mac.abc_class_id
        AND dim_abc_classes.inventory_item_id = maa.inventory_item_id
        AND dim_abc_classes.organization_id = maag.organization_id
    );
commit;

-- Insert records

insert into bec_dwh.DIM_ABC_CLASSES
(
	ASSIGNMENT_GROUP_ID
   ,INVENTORY_ITEM_ID
   ,organization_id
   ,ABC_CLASS_ID
   ,ABC_CLASS_NAME
   ,description
   ,disable_date
   ,ASSIGNMENT_GROUP_NAME
   ,COMPILE_ID	
   ,SECONDARY_INVENTORY	
   ,ITEM_SCOPE_TYPE	
   ,CLASSIFICATION_METHOD_TYPE,
	is_deleted_flg,
	SOURCE_APP_ID,
	DW_LOAD_ID,
	DW_INSERT_DATE,
	DW_UPDATE_DATE
)
(
select
	mAAG.ASSIGNMENT_GROUP_ID
   ,MAA.INVENTORY_ITEM_ID
   ,maag.organization_id
   ,mac.ABC_CLASS_ID
   ,MAC.ABC_CLASS_NAME
   ,mac.description
   ,mac.disable_date
   ,maag.ASSIGNMENT_GROUP_NAME
   ,COMPILE_ID	
   ,SECONDARY_INVENTORY	
   ,ITEM_SCOPE_TYPE	
   ,CLASSIFICATION_METHOD_TYPE
	-- audit columns
	,'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(maa.ASSIGNMENT_GROUP_ID, 0)|| '-' || nvl(maa.ABC_CLASS_ID, 0)|| '-' || nvl(maa.INVENTORY_ITEM_ID, 0)|| '-'||  nvl(maag.organization_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
bec_ods.MTL_ABC_ASSIGNMENT_GROUPS maag,
bec_ods.MTL_ABC_ASSIGNMENTS maa,
bec_ods.MTL_ABC_CLASSES mac
WHERE 1 = 1
AND (maag.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
	where dw_table_name ='dim_abc_classes' and batch_name = 'inv'))
AND maag.assignment_group_id = MAA.ASSIGNMENT_GROUP_ID
AND MAA.ABC_CLASS_ID         = MAC.ABC_CLASS_ID	 
 );
 commit;
 -- Soft delete

WITH ValidRecords AS (
    SELECT 
        maag.ASSIGNMENT_GROUP_ID,
        maa.ABC_CLASS_ID,
        maa.INVENTORY_ITEM_ID
    FROM 
        bec_ods.MTL_ABC_ASSIGNMENT_GROUPS maag
    JOIN 
        bec_ods.MTL_ABC_ASSIGNMENTS maa ON maag.ASSIGNMENT_GROUP_ID = maa.ASSIGNMENT_GROUP_ID
    JOIN 
        bec_ods.MTL_ABC_CLASSES mac ON maa.ABC_CLASS_ID = mac.ABC_CLASS_ID
    WHERE 
        maag.is_deleted_flg <> 'Y'
)
UPDATE 
    bec_dwh.dim_abc_classes
SET 
    is_deleted_flg = 'Y'
WHERE 
    NOT EXISTS (
        SELECT 1
        FROM 
            ValidRecords ods
        WHERE 
            dim_abc_classes.ASSIGNMENT_GROUP_ID = ods.ASSIGNMENT_GROUP_ID
            AND dim_abc_classes.ABC_CLASS_ID = ods.ABC_CLASS_ID
            AND dim_abc_classes.INVENTORY_ITEM_ID = ods.INVENTORY_ITEM_ID
    );

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_abc_classes'
and batch_name = 'inv';

COMMIT;