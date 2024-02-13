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
drop table if exists bec_dwh.DIM_MSC_ITEM_CATEGORIES;

CREATE TABLE bec_dwh.dim_msc_item_categories 
DISTKEY (SR_CATEGORY_ID)
SORTKEY (organization_id, inventory_item_id, SR_CATEGORY_ID, START_DATE_ACTIVE, END_DATE_ACTIVE)
AS
(
select organization_id
      ,inventory_item_id
      ,SR_INSTANCE_ID 
      ,SR_CATEGORY_ID 
      ,CATEGORY_SET_ID 
      ,CATEGORY_NAME 
      ,DESCRIPTION 
      ,DISABLE_DATE 
      ,SUMMARY_FLAG 
      ,ENABLED_FLAG 
      ,START_DATE_ACTIVE 
      ,END_DATE_ACTIVE 
      ,CREATION_DATE 
      ,CREATED_BY 
      ,LAST_UPDATE_DATE 
      ,LAST_UPDATED_BY, 
	 'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )           AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(organization_id,0) || '-'|| nvl(inventory_item_id,0) || '-'|| nvl(SR_CATEGORY_ID,0)
	|| '-'|| nvl(CATEGORY_SET_ID,0) || '-'|| nvl(SR_INSTANCE_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM BEC_ODS.msc_item_categories
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_msc_item_categories' 
	and batch_name = 'ascp';

COMMIT;