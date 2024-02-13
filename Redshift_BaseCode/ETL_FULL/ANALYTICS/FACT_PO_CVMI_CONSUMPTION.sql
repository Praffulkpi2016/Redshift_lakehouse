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

drop table if exists bec_dwh.FACT_PO_CVMI_CONSUMPTION ;

create table bec_dwh.fact_po_cvmi_consumption diststyle all sortkey(consumption_release_id)
as 
(
with CTE as 
(
	select
		ch.material,ch.organization_id,
		ch.inventory_item_id,
		mmt.transaction_id
	from
		(select * from bec_ods.CST_COST_HISTORY_V where is_deleted_flg <> 'Y') ch,
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt
		--bec_ods.MTL_CONSUMPTION_TRANSACTIONS mct1
	where 1=1
	    and ch.organization_id = mmt.organization_id
	    and ch.inventory_item_id = mmt.inventory_item_id
		and ch.cost_update_id = mmt.cost_update_id
		--and mmt.transaction_id = mct1.transaction_id)
		)
select
	mct.TRANSACTION_ID,poh.PO_HEADER_ID,prl.PO_RELEASE_ID,
	mct.CREATION_DATE consumption_date,
	poh.SEGMENT1 po_number,
	prl.RELEASE_NUM,
	mct.NET_QTY consumption_quantity,
	mct.blanket_price unit_price,
	(mct.NET_QTY * mct.blanket_price) as consumption_value,
	mct.consumption_release_id,
	mct.INVENTORY_ITEM_ID,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||mct.INVENTORY_ITEM_ID as INVENTORY_ITEM_ID_KEY,
	mct.ORGANIZATION_ID,
	poh.VENDOR_ID ,
	poh.VENDOR_SITE_ID,
	mct.CONSUMPTION_PROCESSED_FLAG,
	cte.material as material_cost,
	(mct.NET_QTY *cte.material) as extended_cost,
	(mct.NET_QTY * mct.blanket_price - mct.NET_QTY *(cte.material)) as consigned_ppv,
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
		|| '-'|| nvl(mct.TRANSACTION_ID, 0)
		|| '-'|| nvl(poh.PO_HEADER_ID, 0)
		|| '-'|| nvl(prl.PO_RELEASE_ID, 0) 
  as dw_load_id, 
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.MTL_CONSUMPTION_TRANSACTIONS where is_deleted_flg <> 'Y') mct ,
	(select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh,
	(select * from bec_ods.po_releases_all where is_deleted_flg <> 'Y') prl,
	CTE
where 1=1
and mct.consumption_RELEASE_ID = prl.PO_RELEASE_ID(+)
and prl.PO_HEADER_ID = poh.PO_HEADER_ID
and mct.organization_id=CTE.organization_id(+)
and mct.inventory_item_id=CTE.inventory_item_id(+)
and mct.transaction_id=CTE.transaction_id(+)
)
;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_cvmi_consumption'
	and batch_name = 'po';

commit;