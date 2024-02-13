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
-- Delete Records
delete from bec_dwh.FACT_OPEN_PO_DATA
where exists 
( select 1 
from
bec_ods.PO_HEADERS_ALL poh,
bec_ods.PO_LINES_ALL pol,
bec_ods.MTL_SYSTEM_ITEMS_B msib,
bec_ods.PO_LINE_LOCATIONS_ALL poll,
bec_ods.PO_DISTRIBUTIONS_ALL pod 
where 1=1
and pol.item_id = msib.inventory_item_id
and poh.po_header_id = pol.po_header_id
and pol.po_line_id = pod.po_line_id
and poll.line_location_id = pod.line_location_id
and pOd.destination_organization_id = MSIB.organization_id
and (pod.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_open_po_data' and batch_name = 'po')
or
poh.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_open_po_data' and batch_name = 'po')
or
poll.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_open_po_data' and batch_name = 'po'))
and FACT_OPEN_PO_DATA.po_header_id = poh.po_header_id
and FACT_OPEN_PO_DATA.po_line_id = pol.po_line_id
and FACT_OPEN_PO_DATA.line_location_id = poll.line_location_id
and FACT_OPEN_PO_DATA.po_distribution_id = pod.po_distribution_id
);
commit;
-- Insert records

insert
	into
	bec_dwh.FACT_OPEN_PO_DATA 
(
	po_header_id,
	po_number,
	po_type,
	po_status,
	closed_code,
	po_creation_date,
	po_last_update_date,
	org_id,
	po_line_id,
	line_num,
	unit_price,
	line_quantity,
	item_id,
	item,
	organization_id,
	line_location_id,
	shipment_num,
	ship_quantity,
	need_by_date,
	promised_date,
	ship_to_organization_id,
	shipment_closed_code,
	po_distribution_id,
	amount,
	destination_subinventory,
	po_header_id_key,
	org_id_key,
	po_line_id_key,
	item_id_key,
	organization_id_key,
	line_location_id_key,
	ship_to_organization_id_key,
	po_distribution_id_key,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
	)
(
	select 
		poh.po_header_id,
		poh.segment1 po_number,
		poh.type_lookup_code po_type,
		poh.authorization_status po_status,
		poh.closed_code ,
		poh.creation_date po_creation_date,
		poh.last_update_date po_last_update_date,
		poh.org_id,
		pol.po_line_id,
		pol.line_num,
		pol.unit_price,
		pol.quantity line_quantity,
		pol.item_id,
		msib.segment1 item,
		msib.organization_id,
		poll.line_location_id,
		poll.shipment_num,
		poll.quantity  ship_quantity,
		poll.need_by_date,
		poll.promised_date,
		poll.ship_to_organization_id,
		poll.closed_code shipment_closed_code,
		pod.po_distribution_id,
		pod.amount_billed amount,
		pod.destination_subinventory,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || poh.po_header_id as po_header_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || poh.org_id as org_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || pol.po_line_id as po_line_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || pol.item_id as item_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || msib.organization_id as organization_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || poll.line_location_id as line_location_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || poll.ship_to_organization_id as ship_to_organization_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || pod.po_distribution_id as po_distribution_id_key,
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
		source_system = 'EBS') 
		|| '-' || nvl(poh.po_header_id, 0)
		|| '-' || nvl(pol.po_line_id, 0)
		|| '-' || nvl(poll.line_location_id, 0)
		|| '-' || nvl(pod.po_distribution_id, 0) as dw_load_id,  
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		from
			(select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg <> 'Y') poh,
			(select * from bec_ods.PO_LINES_ALL where is_deleted_flg <> 'Y') pol,
			(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msib,
			(select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg <> 'Y') poll,
			(select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y') pod
		where
			1 = 1
			and pol.item_id = msib.inventory_item_id
			and poh.po_header_id = pol.po_header_id
			and pol.po_line_id = pod.po_line_id
			and poll.line_location_id = pod.line_location_id
			and pOd.destination_organization_id = MSIB.organization_id
			and NVL(poll.closed_code,'OPEN') = 'OPEN'
			and NVL(poh.closed_code,'OPEN') = 'OPEN'
and (pod.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_open_po_data' and batch_name = 'po')
or
poh.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_open_po_data' and batch_name = 'po')
or
poll.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_open_po_data' and batch_name = 'po'))
);

commit;



end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_open_po_data'
	and batch_name = 'po';

COMMIT;