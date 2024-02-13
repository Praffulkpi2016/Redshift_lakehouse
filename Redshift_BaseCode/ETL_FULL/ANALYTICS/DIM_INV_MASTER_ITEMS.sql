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

BEGIN;

drop table if exists bec_dwh.DIM_INV_MASTER_ITEMS;

create table bec_dwh.dim_inv_master_items 
	distkey(ORGANIZATION_ID)
	sortkey (INVENTORY_ITEM_ID,ORGANIZATION_ID)
as
(SELECT INVENTORY_ITEM_ID,
 ORGANIZATION_ID, 
 ENABLED_FLAG, 
 DESCRIPTION ITEM_DESCRIPTION, 
 SEGMENT1 ITEM_NAME, 
 ITEM_TYPE,
 PURCHASING_ITEM_FLAG,
 SHIPPABLE_ITEM_FLAG,
 CUSTOMER_ORDER_FLAG,
 SERVICE_ITEM_FLAG, 
 EXPENSE_ACCOUNT,
 ENCUMBRANCE_ACCOUNT, 
 UNIT_WEIGHT, 
 UNIT_VOLUME,
 WEIGHT_UOM_CODE, 
 VOLUME_UOM_CODE,
 PRIMARY_UOM_CODE,
 PRIMARY_UNIT_OF_MEASURE,
 CONSIGNED_FLAG,
 attribute5 as program_name,
--aduit columns
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(INVENTORY_ITEM_ID,0)||'-'||nvl(ORGANIZATION_ID,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM bec_ods.MTL_SYSTEM_ITEMS_B
);

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_master_items'
and batch_name = 'ap';

COMMIT;

