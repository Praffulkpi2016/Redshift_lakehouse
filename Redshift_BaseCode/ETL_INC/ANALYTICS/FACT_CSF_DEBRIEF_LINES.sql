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
begin;
--DELETE
delete from bec_dwh.fact_csf_debrief_lines
where exists
(
select 1
FROM
bec_ods.csf_debrief_lines cdh
WHERE   cdh.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_csf_debrief_lines' and batch_name = 'inv')
and nvl(fact_csf_debrief_lines.debrief_header_id,0)  = nvl(cdh.debrief_header_id,0)
and nvl(fact_csf_debrief_lines.debrief_line_id,0)  = nvl(cdh.debrief_line_id,0)
);
commit;

insert into bec_dwh.FACT_CSF_DEBRIEF_LINES 
SELECT 
  cdl.channel_code,
  cdl.debrief_line_id,
  cdl.debrief_header_id,
  cdl.service_date,
  cdl.inventory_item_id,
  nvl(cdl.issuing_inventory_org_id, cdl.receiving_inventory_org_id) inventory_org_id,
  nvl(cdl.issuing_sub_inventory_code, cdl.receiving_sub_inventory_code) sub_inventory_code,
  nvl(cdl.issuing_locator_id, cdl.receiving_locator_id) LOCATOR,
  msibk.segment1 item,
  msibk.description,
  cdl.uom_code,
  cdl.parent_product_id,
  cdl.removed_product_id,
  cdl.status_of_received_part,
  cdl.quantity,
  cdl.item_revision,
  cdl.item_lotnumber,
  cdl.item_serial_number,
  cdl.created_by,
  cdl.creation_date,
  cdl.last_updated_by,
  cdl.last_update_date,
  cdl.last_update_login,
  cdl.attribute1,
  cdl.attribute2,
  cdl.attribute3,
  cdl.attribute4,
  cdl.attribute5,
  cdl.attribute6,
  cdl.attribute7,
  cdl.attribute8,
  cdl.attribute9,
  cdl.attribute10,
  cdl.attribute11,
  cdl.attribute12,
  cdl.attribute13,
  cdl.attribute14,
  cdl.attribute15,
  cdl.attribute_category,
  cdl.material_reason_code,
  cdl.business_process_id,
  cdl.transaction_type_id,
  ctb.line_order_category_code,
  fl.meaning material_meaning,
  cdl.ib_update_status,
  cdl.ib_update_msg_code,
  cdl.ib_update_message,
  cdl.spare_update_status,
  cdl.spare_update_msg_code,
  cdl.spare_update_message,
  cdl.charge_upload_status,
  cdl.charge_upload_msg_code,
  cdl.charge_upload_message,
  cdl.return_reason_code,
  arc.meaning as return_reason,
  cdl.return_date,
  cdl.error_text,
  cdl.material_transaction_id,
  cdl.usage_type,
  cdl.return_subinventory_name,
  cdl.return_organization_id,
  cdl.carrier_code,
  cdl.shipping_method,
  cdl.shipping_number,
  cdl.waybill,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cdl.debrief_line_id debrief_line_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cdl.debrief_header_id debrief_header_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cdl.inventory_item_id inventory_item_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cdl.transaction_type_id transaction_type_id_KEY,
  'N' as is_deleted_flg, 
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
  ) || '-' || nvl(cdl.debrief_line_id, 0) as dw_load_id,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date
FROM
    (select * from bec_ods.csf_debrief_lines where is_deleted_flg <> 'Y'
	and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_csf_debrief_lines' and batch_name = 'inv')) cdl,
    (select * from bec_ods.CS_TRANSACTION_TYPES_B where is_deleted_flg <> 'Y') ctb,
    (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') fl,
    (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') arc,
    (select * from bec_ods.cs_billing_type_categories where is_deleted_flg <> 'Y') cbtc,
    (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msibk
WHERE 1=1 
    AND  ctb.transaction_type_id (+) = cdl.transaction_type_id
    AND msibk.material_billable_flag = cbtc.billing_type
    AND cbtc.billing_category = 'M'
    AND msibk.inventory_item_id = cdl.inventory_item_id
    AND msibk.organization_id = nvl(cdl.issuing_inventory_org_id, cdl.receiving_inventory_org_id)
    AND fl.lookup_type (+) = 'CSF_MATERIAL_REASON'
    AND cdl.material_reason_code = fl.lookup_code (+)
    AND cdl.return_reason_code = arc.lookup_code (+)
	AND arc.lookup_type(+) = 'CREDIT_MEMO_REASON'
;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_csf_debrief_lines' 
  and batch_name = 'inv';
commit;