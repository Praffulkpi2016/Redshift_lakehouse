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
	drop  table if exists bec_dwh.FACT_OPEN_PO_DATA;
	create table bec_dwh.FACT_OPEN_PO_DATA diststyle all sortkey ( po_header_id  ) as 
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
			(select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg <> 'Y') msib,
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
	);
	
END;
update 
bec_etl_ctrl.batch_dw_info 
set 
load_type = 'I', 
last_refresh_date = getdate() 
where 
dw_table_name = 'fact_open_po_data' 
and batch_name = 'po';
COMMIT;