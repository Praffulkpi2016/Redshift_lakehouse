/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/


begin;
-- Delete Records

delete
from
	bec_dwh.DIM_INV_TRANSACTION_SOURCES
where
	(nvl(transaction_id,0)) in
(
	select
		nvl(ods.transaction_id,0) as transaction_id
	from
		bec_dwh.DIM_INV_TRANSACTION_SOURCES dw,
		bec_ods.mtl_material_transactions ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.transaction_id, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_inv_transaction_sources'
				and batch_name = 'inv')
				 )
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_INV_TRANSACTION_SOURCES
(
transaction_id,
	transaction_type_name,
	transaction_type_id,
	transaction_source,
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	with ct1 as 
(
	select
		po_header_id,
		segment1
	from
		bec_ods.po_headers_all
),
	ct2 as 
(
	select
		wip_entity_id,
		wip_entity_name
	from
		bec_ods.wip_entities
),
	ct3 as
(
	select
		oola.line_id,
		ooha.order_number::varchar as order_number
	from
		bec_ods.oe_order_headers_all ooha
	inner join bec_ods.oe_order_lines_all oola
on
		oola.header_id = ooha.header_id
),
	ct4 as
(
	select
		header_id,
		request_number
	from
		bec_ods.mtl_txn_request_headers
),
	ct5 as
(
	select
		disposition_id,
		segment1
	from
		bec_ods.mtl_generic_dispositions
),
	ct6 as
(
	select
		requisition_header_id,
		segment1
	from
		bec_ods.po_requisition_headers_all
),
	ct7 as 
(
	select
		organization_id,
		cycle_count_header_id,
		cycle_count_header_name
	from
		bec_ods.mtl_cycle_count_headers
),
	ct8 as
(
	select
		organization_id,
		physical_inventory_id,
		physical_inventory_name
	from
		bec_ods.mtl_physical_inventories
),
	ct9 as
(
	select
		cost_update_id,
		description
	from
		bec_ods.cst_cost_updates
)
	--main query starts here
	select
		mmt.transaction_id,
		mtt.transaction_type_name,
		mmt.transaction_type_id,
		decode(
            substring(mtt.transaction_type_name, 1, 2), 
            'PO', ct1.segment1, 
            'WI', ct2.wip_entity_name, 
            'Sa', ct3.order_number, 
            decode(mmt.transaction_source_type_id,
            4, ct4.request_number, 
            6, ct5.segment1, 
            7, ct6.segment1, 
            9, ct7.cycle_count_header_name,
            10, ct8.physical_inventory_name, 
            11, ct9.description, null)
        ) transaction_source,
		mmt.last_update_date,
		-- audit columns
	'N' as is_deleted_flg,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS') as source_app_id,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
	|| '-' || nvl(mmt.transaction_id, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.mtl_material_transactions mmt
	left outer join bec_ods.mtl_transaction_types mtt
on
		mmt.transaction_type_id = mtt.transaction_type_id
	left outer join ct1
on
		mmt.transaction_source_id = ct1.po_header_id
	left outer join ct2
on
		mmt.transaction_source_id = ct2.wip_entity_id
	left outer join ct3
on
		mmt.trx_source_line_id = ct3.line_id
	left outer join ct4
on
		mmt.transaction_source_id = ct4.header_id
	left outer join ct5
on
		mmt.transaction_source_id = ct5.disposition_id
	left outer join ct6
on
		mmt.transaction_source_id = ct6.requisition_header_id
	left outer join ct7
on
		mmt.transaction_source_id = ct7.cycle_count_header_id
		and mmt.organization_id = ct7.organization_id
	left outer join ct8
on
		mmt.transaction_source_id = ct8.physical_inventory_id
		and mmt.organization_id = ct8.organization_id
	left outer join ct9
on
		mmt.transaction_source_id = ct9.cost_update_id
	where 1=1
			and (mmt.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_inv_transaction_sources'
				and batch_name = 'inv')
				 )
 );
-- Soft delete

update
	bec_dwh.DIM_INV_TRANSACTION_SOURCES
set
	is_deleted_flg = 'Y'
where
	(nvl(transaction_id,0)) not in (
	select
		nvl(ods.transaction_id,0) as transaction_id
	from
		bec_dwh.DIM_INV_TRANSACTION_SOURCES dw,
		bec_ods.mtl_material_transactions ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.transaction_id, 0)
			and ods.is_deleted_flg <> 'Y'
);

commit;
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_transaction_sources' and batch_name = 'inv';

COMMIT;