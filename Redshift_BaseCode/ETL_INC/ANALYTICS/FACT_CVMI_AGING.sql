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

Truncate table bec_dwh.FACT_CVMI_AGING  ;

insert into bec_dwh.FACT_CVMI_AGING
(
  select  a.as_of_date,
   a.ORGANIZATION_ID,
   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||a.ORGANIZATION_ID as ORGANIZATION_ID_KEY,
   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||OOD.OPERATING_UNIT as ORG_ID_KEY,   
   OOD.OPERATING_UNIT as ORG_ID,
   a.VENDOR_ID,
   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||a.VENDOR_ID as VENDOR_ID_KEY,
   a.VENDOR_SITE_ID,
   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||a.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
   a.ITEM_ID,
   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||a.ITEM_ID as ITEM_ID_KEY,
   a.PRIMARY_VENDOR_ITEM,
   a.NAME,
   a.VENDORNAME,
   a.VENDORSITECODE,
   a.ITEMS,
   a.DESCRIPTION,
   a.UOM,
   a.SUBINVENTORY,
   a.LOCATOR,
   a.REVISION,
   a.RECEIPT_DATE,
   a.CONSUMEBEFORE,
   a.QUANTITY,
   a.AGING_PERIOD,
   a.aging_days,
   a.aging_bucket,
   a.RECEIVED_DATE_TIME,
   a.serial_lot_number,
   a.unit_price,
   a.extended_cost,
   a.percent_aging,
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
    || '-' || nvl(a.item_id, 0)   
	|| '-' || nvl(a.serial_lot_number, 'NA')  
	|| '-' || nvl(a.received_date_time, 'NA')
	||'-'||nvl(a.SUBINVENTORY,'NA')
	||'-'||nvl(a.LOCATOR,'NA')
	as dw_load_id,
    getdate() as dw_insert_date,
    getdate() as dw_update_date
from (select * from bec_ods.BEC_CVMI_AGING_VIEW where is_deleted_flg <> 'Y')  a, 
     (select * from bec_ods.org_organization_definitions where is_deleted_flg <> 'Y') ood 
where a.organization_id  = ood.organization_id
 ) ;
    
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_cvmi_aging'
	and batch_name = 'po';

commit;
