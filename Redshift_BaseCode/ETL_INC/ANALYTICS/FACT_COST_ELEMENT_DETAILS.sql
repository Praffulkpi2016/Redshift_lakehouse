/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/

BEGIN;

--Delete Records
delete from bec_dwh.FACT_COST_ELEMENT_DETAILS
where (nvl(INVENTORY_ITEM_ID, 0),nvl(ORGANIZATION_ID, 0),nvl(COST_TYPE_ID, 0))
in
(
select ods.INVENTORY_ITEM_ID,ods.ORGANIZATION_ID,ods.COST_TYPE_ID
from bec_dwh.FACT_COST_ELEMENT_DETAILS dw,
(
SELECT nvl(MSI.INVENTORY_ITEM_ID, 0) INVENTORY_ITEM_ID
,nvl(MSI.ORGANIZATION_ID, 0) ORGANIZATION_ID
,nvl(CIC.COST_TYPE_ID, 0)  COST_TYPE_ID
FROM
	 bec_ods.MTL_SYSTEM_ITEMS_B  MSI,
	 bec_ods.MTL_PARAMETERS  MP,
	 bec_ods.MTL_ITEM_CATEGORIES  MIC,
	 bec_ods.MTL_CATEGORIES_B  MC,
	 bec_ods.CST_COST_TYPES  CT,
	 bec_ods.CST_ITEM_COSTS  CIC
WHERE 1=1
	 AND CIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
	 AND CIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
	 AND CIC.ORGANIZATION_ID = MP.COST_ORGANIZATION_ID
	 AND CIC.INVENTORY_ITEM_ID = MIC.INVENTORY_ITEM_ID
	 AND CIC.ORGANIZATION_ID = MIC.ORGANIZATION_ID
	 AND MIC.CATEGORY_SET_ID = 1
	 AND MIC.CATEGORY_ID = MC.CATEGORY_ID
	 AND CIC.COST_TYPE_ID = CT.COST_TYPE_ID 
	 and 
	( nvl(MSI.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_cost_element_details' and batch_name = 'costing')
OR
nvl(CIC.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_cost_element_details' and batch_name = 'costing')
or MSI.is_deleted_flg = 'Y'
or MP.is_deleted_flg = 'Y'
or MIC.is_deleted_flg = 'Y'
or MC.is_deleted_flg = 'Y'
or CT.is_deleted_flg = 'Y'
or CIC.is_deleted_flg = 'Y')
)ods
where dw.dw_load_id = 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')
			   || '-' || nvl(ODS.INVENTORY_ITEM_ID, 0)
			   || '-' || nvl(ODS.ORGANIZATION_ID, 0) 
			   || '-' || nvl(ODS.COST_TYPE_ID, 0) 
);
commit;


insert INTO bec_dwh.FACT_COST_ELEMENT_DETAILS
(
INVENTORY_ITEM_ID
,ORGANIZATION_ID
,ORGANIZATION_ID_KEY
,CATEGORY_SET_ID
,COST_TYPE_ID
,ITEM
,ITEM_ID_KEY
,ITEM_DESCRIPTION
,UOM
,MATERIAL_COST
,MATERIAL_OVHD
,RESOURCE_COST
,OUTSIDE_PROC
,OVHD_COST
,TOTAL_UNIT_COST
,COST_TYPE
,EXCHANGE_RATE
,IS_DELETED_FLG
,SOURCE_APP_ID
,DW_LOAD_ID
,DW_INSERT_DATE
,DW_UPDATE_DATE
)
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
	 	 AND 
	( nvl(MSI.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_cost_element_details' and batch_name = 'costing')
OR
nvl(CIC.kca_seq_date) > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_cost_element_details' and batch_name = 'costing')
)
);

commit;

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