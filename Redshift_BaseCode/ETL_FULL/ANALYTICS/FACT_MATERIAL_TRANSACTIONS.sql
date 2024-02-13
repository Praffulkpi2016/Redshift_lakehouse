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

drop table if exists bec_dwh.FACT_MATERIAL_TRANSACTIONS;

create table bec_dwh.FACT_MATERIAL_TRANSACTIONS 
distkey(transaction_id) sortkey(transaction_id,inventory_item_id,organization_id)
as
select
	msi.segment1,
	msi.description,
	mmt.transaction_date,
	mmt.revision,
	mmt.transaction_type_id,
	mmt.transaction_source_type_id,
	mmt.transaction_source_id,
	mmt.subinventory_code,
	mmt.transfer_subinventory,
	msi.primary_uom_code,
	mmt.new_cost,
	mmt.transaction_id,
	mmt.transaction_set_id,
	mmt.created_by,
	mmt.transaction_reference,
	mil1.segment1
        || '.'
        || mil1.segment2
        || '.'
        || mil1.segment3 locator,
	mil2.segment1
        || '.'
        || mil2.segment2
        || '.'
        || mil2.segment3 transfer_locator,
	mut.serial_number,
	mln.lot_number,
	mmt.inventory_item_id,
	mmt.organization_id,
	mmt.rcv_transaction_id,
	mmt.move_order_line_id,
	mmt.move_transaction_id,
	mmt.rma_line_id,
	mmt.operation_seq_num,
	mmt.distribution_account_id,
	--        gcc.segment3     dist_acct,
	--        gcc.segment2     dist_dept,
	mmt.shipment_number,
		mmt.last_update_date,
		mln.primary_quantity as mln_primary_quantity,
		mmt.primary_quantity as mmt_primary_quantity,
		DECODE (mln.lot_number,
                  NULL, DECODE (mut.serial_number,
                                NULL, mmt.primary_quantity,
                                SIGN (mmt.primary_quantity) * 1
                               ),
                    DECODE (mut.serial_number,
                            NULL, mmt.primary_quantity,
                            SIGN (mmt.primary_quantity) * 1
                           )
                  / DECODE (mut.serial_number,
                            NULL, mmt.primary_quantity,
                            SIGN (mmt.primary_quantity) * 1
                           )
                  * mln.primary_quantity
                 ) as primary_quantity,
	-- audit columns
	'N' AS IS_DELETED_FLG,
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
	|| '-' || nvl(mmt.transaction_id, 0)
    || '-' || nvl(mmt.inventory_item_id, 0)
	|| '-' || nvl(mmt.organization_id, 0)
	|| '-' || nvl(mut.serial_number, 'NA') 
	|| '-' || nvl(mln.lot_number, 'NA')	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
	(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
	(select distinct transaction_id,inventory_item_id,organization_id,serial_number from bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y') mut,
	(select transaction_id,inventory_item_id,organization_id,lot_number,
sum(primary_quantity) as primary_quantity from bec_ods.mtl_transaction_lot_numbers where is_deleted_flg <> 'Y'
group by transaction_id,inventory_item_id,organization_id,lot_number) mln,
	(select * from bec_ods.mtl_item_locations where is_deleted_flg <> 'Y') mil1,
	(select * from bec_ods.mtl_item_locations where is_deleted_flg <> 'Y') mil2
where
	mmt.inventory_item_id = msi.inventory_item_id(+)
	and mmt.organization_id = msi.organization_id(+)
	and mmt.transaction_id = mut.transaction_id (+)
	and mmt.inventory_item_id = mut.inventory_item_id (+)
	and mmt.organization_id = mut.organization_id (+)
	and mmt.transaction_id = mln.transaction_id (+)
	and mmt.inventory_item_id = mln.inventory_item_id (+)
	and mmt.organization_id = mln.organization_id (+)
	and mmt.locator_id = mil1.inventory_location_id (+)
	and mmt.organization_id = mil1.organization_id (+)
	and mmt.transfer_locator_id = mil2.inventory_location_id (+)
	and mmt.organization_id = mil2.organization_id (+);
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_material_transactions'
	and batch_name = 'inv';

commit;