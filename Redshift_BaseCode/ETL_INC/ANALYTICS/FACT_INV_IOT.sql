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
	
	delete from bec_dwh.FACT_INV_IOT
	where nvl(shipment_line_id,0)
	in 
	(
		select nvl(ods.shipment_line_id,0) as shipment_line_id 
		from bec_dwh.FACT_INV_IOT dw,
		(
			Select
			rch.shipment_header_id
			,rcl.shipment_line_id
			,rcl.item_id 		
			,rch.organization_id
			,rcl.mmt_transaction_id
			,rch.shipment_num iot			
			--	 ,a.rid rid
			,msib.segment1 part_number			
			,REGEXP_REPLACE(msib.description,'[^0-9A-Za-z]', ' ') part_description			
			,rcl.line_num line_num			
			,ood.organization_code from_location			
			,ood1.organization_code to_location			
			,rcl.quantity_shipped ship_qty			
			,mmt.transaction_date ship_date			
			,rcl.quantity_received receive_quantity			
			,rcl.last_update_date receive_Date			
			,rcl.shipment_line_status_code iot_status
			,(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||rcl.item_id as ITEM_ID_KEY,
			(select system_id from bec_etl_ctrl.etlsourceappid 
			where source_system='EBS')||'-'||rch.organization_id as ORGANIZATION_ID_KEY,
			(
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
			)
			|| '-'
			|| nvl( rcl.shipment_line_id,0)
			AS dw_load_id,
			getdate()           AS dw_insert_date,
			getdate()           AS dw_update_date	 
			FROM   bec_ods.rcv_shipment_headers   rch			
			,  bec_ods.rcv_shipment_lines   rcl			
			,  bec_ods.mtl_system_items_b   msib			
			,  bec_ods.org_organization_definitions   ood			
			,  bec_ods.org_organization_definitions   ood1			
			,  bec_ods.mtl_material_transactions   mmt			
			--	 ,xxbec_iot_number a
			WHERE 1=1						
			AND rch.shipment_header_id = rcl.shipment_header_id			
			AND rcl.item_id = msib.inventory_item_id			
			AND rch.organization_id= msib.organization_id			
			AND rcl.from_organization_id = ood.organization_id			
			AND rcl.to_organization_id = ood1.organization_id			
			AND rcl.mmt_transaction_id = mmt.transaction_id			
			--  and rch.shipment_num = a.iot	
			AND source_document_code ='INVENTORY'
			and ( rcl.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_inv_iot'
			and batch_name = 'inv')
			or mmt.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_inv_iot'
			and batch_name = 'inv') 
			or rcl.is_deleted_flg = 'Y'
			or mmt.is_deleted_flg = 'Y'
			or rch.is_deleted_flg = 'Y'
			or msib.is_deleted_flg = 'Y'
			or ood.is_deleted_flg = 'Y'
			or ood1.is_deleted_flg = 'Y'

			)
		)ods
		where 1=1 
		and dw.dw_load_id 
		=     (
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		)
		|| '-'
		|| nvl(ods.shipment_line_id,0)
	);
	-- Insert Records
	insert into bec_dwh.FACT_INV_IOT
	(
		shipment_header_id
		,shipment_line_id
		,item_id
		,organization_id
		,mmt_transaction_id
		,iot
		,part_number
		,part_description
		,line_num
		,from_location
		,to_location
		,ship_qty
		,ship_date
		,receive_quantity
		,receive_date
		,iot_status
		,item_id_key
		,organization_id_key
		,is_deleted_flg
		,source_app_id
		,dw_load_id
		,dw_insert_date
		,dw_update_date
	)
	(
		Select
		rch.shipment_header_id
		,rcl.shipment_line_id
		,rcl.item_id 		
		,rch.organization_id
		,rcl.mmt_transaction_id
		,rch.shipment_num iot			
		--	 ,a.rid rid
		,msib.segment1 part_number			
		,REGEXP_REPLACE(msib.description,'[^0-9A-Za-z]', ' ') part_description			
		,rcl.line_num line_num			
		,ood.organization_code from_location			
		,ood1.organization_code to_location			
		,rcl.quantity_shipped ship_qty			
		,mmt.transaction_date ship_date			
		,rcl.quantity_received receive_quantity			
		,rcl.last_update_date receive_Date			
		,rcl.shipment_line_status_code iot_status
		,(select system_id from bec_etl_ctrl.etlsourceappid 
		where source_system='EBS')||'-'||rcl.item_id as ITEM_ID_KEY,
		(select system_id from bec_etl_ctrl.etlsourceappid 
		where source_system='EBS')||'-'||rch.organization_id as ORGANIZATION_ID_KEY,
		'N' as is_deleted_flg,
		(
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
		)
		|| '-'
		|| nvl( rcl.shipment_line_id,0)
		AS dw_load_id,
		getdate()           AS dw_insert_date,
		getdate()           AS dw_update_date	 
		FROM (select * from bec_ods.rcv_shipment_headers where is_deleted_flg<>'Y') rch			
		,(select * from bec_ods.rcv_shipment_lines where is_deleted_flg<>'Y') rcl			
		,(select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msib			
		,(select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') ood			
		,(select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') ood1			
		,(select * from bec_ods.mtl_material_transactions where is_deleted_flg<>'Y') mmt		 
		--	 ,xxbec_iot_number a
		WHERE 1=1						
		AND rch.shipment_header_id = rcl.shipment_header_id			
		AND rcl.item_id = msib.inventory_item_id			
		AND rch.organization_id= msib.organization_id			
		AND rcl.from_organization_id = ood.organization_id			
		AND rcl.to_organization_id = ood1.organization_id			
		AND rcl.mmt_transaction_id = mmt.transaction_id			
		--  and rch.shipment_num = a.iot	
		AND source_document_code ='INVENTORY'
		and ( rcl.kca_seq_date > (
			select
			(executebegints-prune_days)
			from
			bec_etl_ctrl.batch_dw_info
			where
			dw_table_name = 'fact_inv_iot'
		and batch_name = 'inv')
		or mmt.kca_seq_date > (
			select
			(executebegints-prune_days)
			from
			bec_etl_ctrl.batch_dw_info
			where
			dw_table_name = 'fact_inv_iot'
		and batch_name = 'inv') )
	);
	
end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name  = 'fact_inv_iot'
and batch_name = 'inv';

commit;