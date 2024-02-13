/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for dimension.
# File Version: KPI v1.0
*/
begin;

truncate table bec_dwh.DIM_SFDC_ITEM_BOMS;

insert into bec_dwh.DIM_SFDC_ITEM_BOMS
( SELECT 
    iasy.organization_id,
	 iasy.inventory_item_id,
	substring(iasy.segment1, regexp_instr(iasy.segment1, '-')+ 1) "SiteId",
	ood.organization_code "Org Code",
	iasy.segment1 "Assembly Item",
	iasy.description "Assembly Description",
	comp.component_item_id,
	icmp.segment1 "Component",
	icmp.description " Component Description",
	comp.operation_seq_num "Operation Sequence",
	comp.item_num "Component Item Num",
	comp.component_quantity "Quantity",
    'N' AS is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    ) AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-' || nvl(iasy.inventory_item_id, 0)  
    ||'-'|| nvl(iasy.organization_id, 0)
    ||'-'|| nvl(comp.component_item_id, 0)AS dw_load_id,
    getdate() AS dw_insert_date,
    getdate() AS dw_update_date
FROM
	bec_ods.mtl_system_items_B iasy,
	bec_ods.bom_bill_of_materials bom,
	bec_ods.bom_inventory_components comp,
	bec_ods.mtl_system_items_B icmp,
	bec_ods.org_organization_definitions ood
WHERE
	1 = 1
	AND ood.organization_id = icmp.organization_id
	AND iasy.organization_id = icmp.organization_id
	AND iasy.inventory_item_id = bom.assembly_item_id
	AND iasy.organization_id = bom.organization_id
	AND bom.bill_sequence_id = comp.bill_sequence_id
	AND comp.component_item_id = icmp.inventory_item_id
	AND comp.EFFECTIVITY_DATE = (SELECT max(EFFECTIVITY_DATE)
									FROM bec_ods.bom_inventory_components
									WHERE COMPONENT_ITEM_ID = comp.component_item_id
									AND bill_sequence_id = comp.bill_sequence_id
									GROUP BY COMPONENT_ITEM_ID)
	AND (iasy.segment1 LIKE 'SI-%'
		OR iasy.segment1 LIKE 'SD-%'
		OR iasy.segment1 LIKE 'SP-%')
	AND nvl(comp.DISABLE_DATE,getdate()+1) > trunc(getdate())	
);
		
commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
        dw_table_name = 'dim_sfdc_item_boms'
    AND batch_name = 'salesforce';

COMMIT;