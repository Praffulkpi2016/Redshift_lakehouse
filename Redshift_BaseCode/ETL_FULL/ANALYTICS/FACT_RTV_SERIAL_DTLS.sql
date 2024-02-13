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
	drop 
	table if exists bec_dwh.fact_rtv_serial_dtls;
	create table bec_dwh.fact_rtv_serial_dtls diststyle all sortkey(
	transaction_id, ORGANIZATION_ID,INVENTORY_ITEM_ID ) AS 
	(
		SELECT 
		A.transaction_type_id,
		A.inventory_item_id,
		A.organization_id,
		A.po_line_location_id,
		A.po_distribution_id,
		A.SHIPMENT_HEADER_ID,
		A.transaction_id,
		A.invoice_num,
		A.receipt_number,
		A.OLD_CONSUMPTION_PO,
		A.OLD_USE_INVOICE,				
		A.NEW_CONSUMPTION_PO,
		A.NEW_USE_INVOICE,
		A.part_number,
		A.description,
		A.serial_number,
		--A.transaction_type,
		A.transaction_date,
		A.transaction_cost,
		A.shipment_number,
		A.rcv_transaction_id,
		A.current_subinventory_code,
		A.current_locator_id,
		(
			SELECT
			'PO:-' || poh.segment1 || '; LINE:-' || pol.line_num || '; REL:-' || nvl(por.release_num,'0') || '; CVMI FLAG:-' || nvl(poll.consigned_flag, 'N')
			FROM
			(select * from  bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh,
			(select * from  bec_ods.po_lines_all where is_deleted_flg <> 'Y') pol,
			(select * from  bec_ods.po_releases_all where is_deleted_flg <> 'Y') por,
			(select * from  bec_ods.po_line_locations_all where is_deleted_flg <> 'Y') poll
			WHERE
			poll.po_header_id = poh.po_header_id
			AND poll.po_line_id = pol.po_line_id
			AND poll.po_release_id = por.Po_release_id(+)
		AND poll.line_location_id = a.po_line_location_id ) po_details,
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || A.transaction_type_id as transaction_type_id_KEY, 
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || A.ORGANIZATION_ID as ORGANIZATION_ID_KEY, 
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || A.inventory_item_id as inventory_item_id_KEY ,
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
		) || '-' || nvl(A.transaction_id, 0)|| '-' || nvl(A.ORGANIZATION_ID, 0) || '-' || nvl(A.inventory_item_id, 0)
		|| '-' || nvl(A.SERIAL_NUMBER, 'NA') || '-' || nvl(A.transaction_type_id, 0)|| '-' || nvl(A.PO_distribution_id, 0)
		|| '-' || nvl(A.shipment_header_id, 0) || '-' || nvl(A.current_locator_id, 0)|| '-' || nvl(A.transaction_date,getdate())	as dw_load_id,  
		getdate() as dw_insert_date, 
		getdate() as dw_update_date 
		from 
		(
			(
				WITH ap_invoice_cnt AS (  --3936865  3548183  
					SELECT APIL.PO_DISTRIBUTION_ID, 
					min(invoice_num)|| ';' || max(invoice_num)|| ' ; ' || count(DISTINCT invoice_num) invoice_dtls
					FROM
					(select * from  bec_ods.ap_invoices_all where is_deleted_flg <> 'Y') api,
					(select * from bec_ods.ap_invoice_lines_all where is_deleted_flg <> 'Y') apil
					WHERE
					api.invoice_id = apil.invoice_id
				group by APIL.PO_DISTRIBUTION_ID ),
				po_consumption as	(
					SELECT mmt.inventory_item_id, mut.serial_number,
					min( poh.segment1 || '-' || por.release_num) OLD_CONSUMPTION_PO,max( poh.segment1 || '-' || por.release_num) NEW_CONSUMPTION_PO
					FROM
					(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
					(select * from  bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y') mut,
					(select * from bec_ods.mtl_consumption_transactions where is_deleted_flg <> 'Y') mct,
					(select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh,
					(select * from bec_ods.po_releases_all where is_deleted_flg <> 'Y') por
					WHERE
					mmt.transaction_id = mut.transaction_id
					AND mmt.transaction_type_id = 74
					--AND mut.serial_number = d.serial_number
					--AND mmt.inventory_item_id = d.inventory_item_id
					AND mmt.transaction_id = mct.transaction_id
					AND mct.consumption_release_id = por.po_release_id
					AND por.po_header_id = poh.po_header_id
					GROUP BY mmt.inventory_item_id, mut.serial_number
				),
				use_invoice as	(
					SELECT mmt.inventory_item_id, mut.serial_number,
					min(api.invoice_num) as OLD_USE_INVOICE, max(api.invoice_num) as NEW_USE_INVOICE
					FROM
					(select * from  bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
					(select * from bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y') mut,
					(select * from bec_ods.mtl_consumption_transactions where is_deleted_flg <> 'Y') mct,
					(select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh,
					(select * from bec_ods.po_releases_all where is_deleted_flg <> 'Y') por,
					(select * from bec_ods.ap_invoices_all where is_deleted_flg <> 'Y') api,
					(select * from bec_ods.ap_invoice_lines_all where is_deleted_flg <> 'Y') apil
					WHERE
					mmt.transaction_id = mut.transaction_id
					AND mmt.transaction_type_id = 74
					--AND mut.serial_number = d.serial_number
					--AND mmt.inventory_item_id = d.inventory_item_id
					AND mmt.transaction_id = mct.transaction_id
					AND mct.consumption_release_id = por.po_release_id
					AND por.po_header_id = poh.po_header_id
					AND mct.po_distribution_id = apil.po_distribution_id
					AND api.invoice_id = apil.invoice_id
					GROUP BY mmt.inventory_item_id, mut.serial_number
				) 
				SELECT DISTINCT
				msi.inventory_item_id,
				mmt.organization_id,
				mmt.transaction_type_id,
				rct.po_line_location_id,
				rct.po_distribution_id,
				RCT.SHIPMENT_HEADER_ID,
				rct.transactioN_id,
				invct.invoice_dtls as invoice_num,
				rsh.RECEIPT_NUM receipt_number,
				poc.OLD_CONSUMPTION_PO,
				inv_use.OLD_USE_INVOICE,				
				poc.NEW_CONSUMPTION_PO,
				inv_use.NEW_USE_INVOICE,
				msi.segment1 part_number,
				msi.description,
				mut.serial_number,
				--mtt.transaction_type_name transaction_type,
				mmt.transaction_date,
				mmt.transaction_cost,
				mmt.shipment_number,
				mmt.rcv_transaction_id,
				msn.current_subinventory_code,
				msn.current_locator_id
				--Can be replaced with FACT_PO_DETAILS by adding release num to this fact.
				FROM
				(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt 
				inner join (select * from bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y') mut on mmt.transaction_id = mut.transaction_id
				inner join (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi on 		MMT.INVENTORY_ITEM_ID = msi.inventory_item_id
				AND mmt.organizatioN_id = msi.organization_id 
				AND msi.segment1 NOT LIKE '%DMG%'
				LEFT JOIN (select * from bec_ods.rcv_transactions where is_deleted_flg <> 'Y') rct on mmt.rcv_transaction_id = rct.transactioN_id
				LEFT JOIN (select * from bec_ods.rcv_shipment_headers where is_deleted_flg <> 'Y') RSH on RCT.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID
				INNER JOIN (select * from bec_ods.mtl_serial_numbers where is_deleted_flg <> 'Y') msn on MMT.INVENTORY_ITEM_ID = msn.inventory_item_id
				and mut.serial_number = msn.serial_number
				LEFT JOIN ap_invoice_cnt invct on rct.po_distribution_id = invct.po_distribution_id
				LEFT JOIN po_consumption poc on mut.serial_number = poc.serial_number
				and mmt.inventory_item_id  = poc.inventory_item_id
				LEFT JOIN use_invoice inv_use on mut.serial_number = inv_use.serial_number(+)
				and mmt.inventory_item_id  = inv_use.inventory_item_id
				WHERE mmt.transaction_type_id = 18
				--AND mut.serial_number = 'STD134'
				UNION ALL
				SELECT  DISTINCT
				msi.inventory_item_id,
				mmt.organization_id,
				mmt.transaction_type_id,
				rct.po_line_location_id,
				rct.po_distribution_id,
				RCT.SHIPMENT_HEADER_ID,
				rct.transactioN_id,
				invct.invoice_dtls as invoice_num,
				rsh.RECEIPT_NUM receipt_number,
				poc.OLD_CONSUMPTION_PO,
				inv_use.OLD_USE_INVOICE,				
				poc.NEW_CONSUMPTION_PO,
				inv_use.NEW_USE_INVOICE,
				msi.segment1 part_number,
				msi.description,
				mut.serial_number,
				--mtt.transaction_type_name transaction_type,
				mmt.transaction_date,
				mmt.transaction_cost,
				mmt.shipment_number,
				rcv_transaction_id,
				msn.current_subinventory_code,
				msn.current_locator_id
				FROM
				(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt 
				inner join (select * from bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y') mut on mmt.transaction_id = mut.transaction_id
				inner join (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi on 		MMT.INVENTORY_ITEM_ID = msi.inventory_item_id
				AND mmt.organizatioN_id = msi.organization_id 
				AND msi.segment1 LIKE '%DMG%'
				LEFT JOIN (select * from bec_ods.rcv_transactions where is_deleted_flg <> 'Y') rct on mmt.rcv_transaction_id = rct.transactioN_id
				LEFT JOIN (select * from bec_ods.rcv_shipment_headers where is_deleted_flg <> 'Y') RSH on RCT.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID
				INNER JOIN (select * from bec_ods.mtl_serial_numbers where is_deleted_flg <> 'Y')msn on MMT.INVENTORY_ITEM_ID = msn.inventory_item_id
				and mut.serial_number = msn.serial_number
				LEFT JOIN ap_invoice_cnt invct on rct.po_distribution_id = invct.po_distribution_id
				LEFT JOIN po_consumption poc on mut.serial_number = poc.serial_number
				and mmt.inventory_item_id  = poc.inventory_item_id
				LEFT JOIN use_invoice inv_use on mut.serial_number = inv_use.serial_number(+)
				and mmt.inventory_item_id  = inv_use.inventory_item_id
				WHERE mmt.transaction_type_id <> 18
				--AND item_segments = '130676'   and demand_date = to_date('2023-03-31','YYYY-MM-DD')
				--AND  A.PLAN_ID = 40029
			)
		) A
	);
	end;
	update 
	bec_etl_ctrl.batch_dw_info 
	set 
	load_type = 'I', 
	last_refresh_date = getdate() 
	where 
	dw_table_name = 'fact_rtv_serial_dtls' 
	and batch_name = 'po';
	commit;
		