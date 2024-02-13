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
	delete from bec_dwh.FACT_INV_CYCLE_COUNT_ADJ
	where (nvl(inventory_item_id,0),nvl(organization_id,0),
	nvl(cycle_count_header_id,0),nvl(cycle_count_entry_id,0))
	in
	(
		select nvl(ODS.inventory_item_id,0) as inventory_item_id
		,nvl(ODS.organization_id,0) as organization_id
		,nvl(ODS.cycle_count_header_id,0) as cycle_count_header_id
		,nvl(ODS.cycle_count_entry_id,0) as cycle_count_entry_id
		from bec_dwh.FACT_INV_CYCLE_COUNT_ADJ dw,
		(
			select 
			mcce.inventory_item_id	
			,mcce.organization_id 
			,mcce.cycle_count_entry_id,
			mcch.cycle_count_header_id  
			
			FROM    bec_ods.mtl_cycle_count_headers    mcch		
			,  bec_ods.mtl_cycle_count_entries    mcce		
			,  bec_ods.mtl_system_items_b    msib		
			,  bec_ods.mtl_item_locations    mil		
			,  bec_ods.mtl_transaction_reasons    REA
			WHERE 1=1	
			AND mcce.inventory_item_id = msib.inventory_item_id		
			AND mcce.organization_id = msib.organization_id		
			AND mcce.cycle_count_header_id = mcch.cycle_count_header_id		
			AND mcce.locator_id = mil.inventory_location_id				
			AND rea.reason_id(+) = mcce.transaction_reason_id 
			and ( mcce.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_inv_cycle_count_adj'
			and batch_name = 'inv')
			or mcch.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_inv_cycle_count_adj'
			and batch_name = 'inv')  
			or mcce.is_deleted_flg = 'Y'
			or mcch.is_deleted_flg = 'Y' 
			or msib.is_deleted_flg = 'Y'
			or mil.is_deleted_flg = 'Y'
			or REA.is_deleted_flg = 'Y')
		) ODS
		where 1=1 
		and dw.dw_load_id =
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		)
		|| '-'
		|| nvl( ods.inventory_item_id,0) ||'-'||nvl(ods.organization_id,0)
		||'-'||nvl(ods.cycle_count_header_id,0)
		||'-'||nvl(ods.cycle_count_entry_id,0)
	);
	
	commit;
	
	-- Insert records
	
	insert into bec_dwh.FACT_INV_CYCLE_COUNT_ADJ 
	(
		inventory_item_id	
		,organization_id	
		,cycle_count_header_name	
		,item	
		,system_quantity	
		,count_uom_first	
		,adjustment_quantity	
		,adjustment_amount	
		,adjustment_percentage	
		,subinventory	
		,"locator"	
		,remaining_quantity	
		,counter	
		,count_date	
		,count_list_sequence	
		,count_status_code	
		,reason	
		,description	
		,cycle_count_request_date	
		,cycle_count_due_date	
		,approval_date	
		,cycle_count_approved_by	
		,cycle_count_entry_id	
		,cycle_count_header_id	
		,inventory_item_id_key	
		,organization_id_key	
		,is_deleted_flg	
		,source_app_id	
		,dw_load_id	
		,dw_insert_date	
		,dw_update_date
	)
	(
		select 
		mcce.inventory_item_id	
		,mcce.organization_id
		,mcch.cycle_count_header_name		
		,msib.segment1 Item		
		,mcce.system_quantity_current system_quantity		
		,mcce.count_uom_first		
		,mcce.adjustment_quantity 		
		,mcce.adjustment_amount		
		,round((mcce.adjustment_quantity/decode(mcce.system_quantity_current,0,1,mcce.system_quantity_current))*100,2) Adjustment_percentage		
		,mcce.SUBINVENTORY		
		,mil.segment1||'.'||mil.segment2||'.'||mil.segment3 locator	
		,(mcce.system_quantity_current +(mcce.adjustment_quantity) ) Remaining_quantity		
		,(SELECT distinct full_name 		
			FROM (select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y') per_all_people_f		
			WHERE person_id = mcce.counted_by_employee_id_current		
		and effective_end_Date > getdate()) counter
		,mcce.count_date_current count_Date		
		,mcce.count_list_sequence		
		,mcce.entry_status_code count_status_code	
		,rea.reason_name reason		
		,rea.description 	
		,mcce.creation_Date Cycle_count_request_date	
		,mcce.count_due_Date cycle_count_due_date	
		,mcce.approval_Date 		
		,(SELECT distinct full_name  FROM (select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y') per_all_people_f		
			WHERE person_id = mcce.approver_employee_id		
		and effective_end_Date > getdate()) cycle_count_approved_by,
		mcce.cycle_count_entry_id,
		mcch.cycle_count_header_id ,
		(select system_id from bec_etl_ctrl.etlsourceappid 
		where source_system='EBS')||'-'||mcce.inventory_item_id as INVENTORY_ITEM_ID_KEY,
		(select system_id from bec_etl_ctrl.etlsourceappid 
		where source_system='EBS')||'-'||mcce.organization_id as ORGANIZATION_ID_KEY,
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
		|| nvl( mcce.inventory_item_id,0) ||'-'||nvl(mcce.organization_id,0)
		||'-'||nvl(mcch.cycle_count_header_id,0)
		||'-'||nvl(mcce.cycle_count_entry_id,0)
		AS dw_load_id,
		getdate()           AS dw_insert_date,
		getdate()           AS dw_update_date
		FROM  (select * from bec_ods.mtl_cycle_count_headers where is_deleted_flg <> 'Y')  mcch		
		,(select * from bec_ods.mtl_cycle_count_entries where is_deleted_flg <> 'Y')  mcce		
		,(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')  msib		
		,(select * from bec_ods.mtl_item_locations where is_deleted_flg <> 'Y')  mil		
		,(select * from bec_ods.mtl_transaction_reasons where is_deleted_flg <> 'Y')  REA		
		WHERE 1=1		
		AND mcce.inventory_item_id = msib.inventory_item_id		
		AND mcce.organization_id = msib.organization_id		
		AND mcce.cycle_count_header_id = mcch.cycle_count_header_id		
		AND mcce.locator_id = mil.inventory_location_id		 		
		AND rea.reason_id(+) = mcce.transaction_reason_id 
		and ( mcce.kca_seq_date > (
			select
			(executebegints-prune_days)
			from
			bec_etl_ctrl.batch_dw_info
			where
			dw_table_name = 'fact_inv_cycle_count_adj'
		and batch_name = 'inv')
		or mcch.kca_seq_date > (
			select
			(executebegints-prune_days)
			from
			bec_etl_ctrl.batch_dw_info
			where
			dw_table_name = 'fact_inv_cycle_count_adj'
		and batch_name = 'inv'))
	);
	commit;
end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name  = 'fact_inv_cycle_count_adj'
and batch_name = 'inv';

commit;