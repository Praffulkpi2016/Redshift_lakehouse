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
drop table if exists bec_dwh.FACT_MSC_SUP_DEM_VMI;

CREATE TABLE  bec_dwh.FACT_MSC_SUP_DEM_VMI 
	diststyle all sortkey(INVENTORY_ITEM_ID,ORGANIZATION_ID)
as 
(
select 
  plan_id
,sr_instance_id
,customer_id
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||customer_id   CUSTOMER_ID_KEY
,customer_site_id
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||customer_site_id   CUSTOMER_SITE_ID_KEY
,customer_name
,customer_site_name
,supplier_id
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||supplier_id   SUPPLIER_ID_KEY
,supplier_site_id
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||supplier_site_id   SUPPLIER_SITE_ID_KEY
,supplier_name
,supplier_site_name
,inventory_item_id
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||inventory_item_id   INVENTORY_ITEM_ID_KEY
,item_name
,description
,order_details
,planner_code
,organization_id
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||organization_id   ORGANIZATION_ID_KEY
,vmi_type
,aps_supplier_id
,aps_supplier_site_id
,aps_customer_id
,aps_customer_site_id
,buyer_code
,ltrim(rtrim(SPLIT_PART(order_details, '#', 1)))::DECIMAL(28,2) as Replenishment_Qty
,to_date(ltrim(rtrim(SPLIT_PART(order_details, '#', 2))),'DD-MON-YYYY') as Replenishment_Date
,ltrim(rtrim(SPLIT_PART(order_details, '#', 3)))::DECIMAL(28,2) as Ordered_Qty
,ltrim(rtrim(SPLIT_PART(order_details, '#', 4)))::DECIMAL(28,2) as Onhand_Qty
,to_date(ltrim(rtrim(SPLIT_PART(order_details, '#', 5))),'DD-MON-YYYY') Onhand_Date
,ltrim(rtrim(SPLIT_PART(order_details, '#', 6)))::DECIMAL(15,0) as segment6
,ltrim(rtrim(SPLIT_PART(order_details, '#', 7)))::DECIMAL(28,2) as Intransit_Qty
,ltrim(rtrim(SPLIT_PART(order_details, '#', 8)))::VARCHAR(100) as "PO_Number.ASN_Num"
,to_date(ltrim(rtrim(SPLIT_PART(order_details, '#', 9))),'DD-MON-YYYY') as Expected_Receipt_Date
,ltrim(rtrim(SPLIT_PART(order_details, '#', 29)))::DECIMAL(28,2) as Max_Value
,ltrim(rtrim(SPLIT_PART(order_details, '#', 28)))::DECIMAL(28,2) as Min_Value
,nullif(ltrim(rtrim(SPLIT_PART(order_details, '#', 27))),'')::DECIMAL(28,2) as Receipt_Qty
,ltrim(rtrim(SPLIT_PART(order_details, '#', 24)))::DECIMAL(28,2) as Process_Qty
,ltrim(rtrim(SPLIT_PART(order_details, '#', 10))) as segment10
,ltrim(rtrim(SPLIT_PART(order_details, '#', 11))) as segment11
,ltrim(rtrim(SPLIT_PART(order_details, '#', 12))) as segment12
,ltrim(rtrim(SPLIT_PART(order_details, '#', 13))) as segment13
,ltrim(rtrim(SPLIT_PART(order_details, '#', 14))) as segment14
,ltrim(rtrim(SPLIT_PART(order_details, '#', 15))) as segment15
,ltrim(rtrim(SPLIT_PART(order_details, '#', 16))) as segment16
,ltrim(rtrim(SPLIT_PART(order_details, '#', 17))) as segment17
,ltrim(rtrim(SPLIT_PART(order_details, '#', 18))) as segment18
,ltrim(rtrim(SPLIT_PART(order_details, '#', 19))) as segment19
,ltrim(rtrim(SPLIT_PART(order_details, '#', 20))) as segment20
,ltrim(rtrim(SPLIT_PART(order_details, '#', 21))) as segment21
,ltrim(rtrim(SPLIT_PART(order_details, '#', 22))) as segment22
,ltrim(rtrim(SPLIT_PART(order_details, '#', 23))) as segment23
,ltrim(rtrim(SPLIT_PART(order_details, '#', 25))) as segment25
,ltrim(rtrim(SPLIT_PART(order_details, '#', 26))) as segment26
,ltrim(rtrim(SPLIT_PART(order_details, '#', 30))) as segment30
,ltrim(rtrim(SPLIT_PART(order_details, '#', 31))) as segment31
,ltrim(rtrim(SPLIT_PART(order_details, '#', 32))) as segment32
,ltrim(rtrim(SPLIT_PART(order_details, '#', 33))) as segment33
,ltrim(rtrim(SPLIT_PART(order_details, '#', 34))) as segment34
,ltrim(rtrim(SPLIT_PART(order_details, '#', 35))) as segment35
,ltrim(rtrim(SPLIT_PART(order_details, '#', 36))) as segment36
,ltrim(rtrim(SPLIT_PART(order_details, '#', 37))) as segment37
,'N' as is_deleted_flg
		,(
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
		) ||'-'||nvl(INVENTORY_ITEM_ID,0) 
	   ||'-'|| nvl(organization_id,0)	
       ||'-'||nvl(CUSTOMER_ID,0) 
	   ||'-'||nvl(CUSTOMER_SITE_ID,0) 
	   ||'-'||nvl(SUPPLIER_SITE_ID,0)			   AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
from (select * from bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V where is_deleted_flg<>'Y')MSC_SUP_DEM_ENTRIES_VMI_V
);

end;  
  
UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_msc_sup_dem_vmi'
	and batch_name = 'inv';

commit;