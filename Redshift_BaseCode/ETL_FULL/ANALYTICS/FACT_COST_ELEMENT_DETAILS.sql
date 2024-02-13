/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/ 

begin;

drop table if exists bec_dwh.FACT_COST_ELEMENT_DETAILS;

create table bec_dwh.FACT_COST_ELEMENT_DETAILS diststyle all sortkey (ORGANIZATION_ID,INVENTORY_ITEM_ID)
as 
(
SELECT
	 MSI.INVENTORY_ITEM_ID,
	 MSI.ORGANIZATION_ID,
	 (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||MSI.ORGANIZATION_ID ORGANIZATION_ID_KEY,
	 MIC.CATEGORY_SET_ID,
	 CIC.COST_TYPE_ID,
	 MSI.SEGMENT1 ITEM,
	 (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||MSI.SEGMENT1 ITEM_ID_KEY,
	 MSI.DESCRIPTION ITEM_DESCRIPTION,
	 MSI.PRIMARY_UOM_CODE UOM,
	 CIC.MATERIAL_COST MATERIAL_COST,
	 CIC.MATERIAL_OVERHEAD_COST MATERIAL_OVHD,
	 CIC.RESOURCE_COST RESOURCE_COST,
	 CIC.OUTSIDE_PROCESSING_COST OUTSIDE_PROC,
	 CIC.OVERHEAD_COST OVHD_COST,
	 CIC.ITEM_COST TOTAL_UNIT_COST,
	 CT.COST_TYPE,
	 '1' EXCHANGE_RATE,
	 	'N' AS IS_DELETED_FLG,

	 	   (
			select
				system_id
			from
				bec_etl_ctrl.etlsourceappid
			where
				source_system = 'EBS'
			) as source_app_id,
			(
			select
				system_id
			from
				bec_etl_ctrl.etlsourceappid
			where
				source_system = 'EBS'
			)
			   || '-' || nvl(MSI.INVENTORY_ITEM_ID, 0)
			   || '-' || nvl(MSI.ORGANIZATION_ID, 0) 
			   || '-' || nvl(CIC.COST_TYPE_ID, 0) 
			   as dw_load_id, 
			getdate() as dw_insert_date,
			getdate() as dw_update_date
FROM
	 (select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg <> 'Y')  MSI,
	 (select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y')  MP,
	 (select * from bec_ods.MTL_ITEM_CATEGORIES where is_deleted_flg <> 'Y')  MIC,
	 (select * from bec_ods.MTL_CATEGORIES_B where is_deleted_flg <> 'Y')  MC,
	 (select * from bec_ods.CST_COST_TYPES where is_deleted_flg <> 'Y')  CT,
	 (select * from bec_ods.CST_ITEM_COSTS where is_deleted_flg <> 'Y')  CIC
WHERE 1=1
	 AND CIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
	 AND CIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
	 AND CIC.ORGANIZATION_ID = MP.COST_ORGANIZATION_ID
	 AND CIC.INVENTORY_ITEM_ID = MIC.INVENTORY_ITEM_ID
	 AND CIC.ORGANIZATION_ID = MIC.ORGANIZATION_ID
	 AND MIC.CATEGORY_SET_ID = 1
	 AND MIC.CATEGORY_ID = MC.CATEGORY_ID
	 AND CIC.COST_TYPE_ID = CT.COST_TYPE_ID
);

END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_cost_element_details'
	and batch_name = 'costing';

commit;