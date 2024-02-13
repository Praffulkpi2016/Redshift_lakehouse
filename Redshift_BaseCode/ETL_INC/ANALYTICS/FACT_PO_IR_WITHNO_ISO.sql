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
Truncate table bec_dwh.FACT_PO_IR_WITHNO_ISO;
Insert Into bec_dwh.FACT_PO_IR_WITHNO_ISO
  ( 
select 
 name,
 IR_Number,
 line_num,
 ITEM_DESCRIPTION,
 UNIT_PRICE,
 QUANTITY,
 IR_Creation_date ,
 need_by_date,
	-- audit columns
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
	)
	 || '-' || nvl(line_num,0)|| '-' || nvl(IR_Number,'NA') 
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
select distinct
 hou.name,
 prh.segment1 IR_Number,
 prl.line_num,
 ITEM_DESCRIPTION,
 UNIT_PRICE,
 QUANTITY,
 prh.CREATION_DATE IR_Creation_date ,
 prl.need_by_date as need_by_date
from 
  ( select * from bec_ods.po_requisition_lines_all  where is_deleted_flg <> 'Y') prl,
  ( select * from bec_ods.po_requisition_headers_all  where is_deleted_flg <> 'Y') prh,
  ( select * from bec_ods.oe_lines_iface_all  where is_deleted_flg <> 'Y') oli,
  ( select * from bec_ods.hr_operating_units  where is_deleted_flg <> 'Y') hou
where 
     prh.requisition_header_id =prl.requisition_header_id
     and oli. orig_sys_line_ref =prl.requisition_line_id
     --and msi.inventory_item_id = prl.item_id
     --and msi.organization_id= prl.source_organization_id(+)
     and hou.ORGANIZATION_ID =prh.org_id
     and TYPE_LOOKUP_CODE ='INTERNAL'
     --and ORIG_SYS_LINE_REF =2291
     and AUTHORIZATION_STATUS !='CANCELLED'
     --and prh.  creation_date > sysdate -90
)
);

END;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_po_ir_withno_iso' 
  and batch_name = 'po';
commit;

