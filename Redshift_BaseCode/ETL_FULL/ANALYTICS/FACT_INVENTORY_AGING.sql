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

drop table if exists bec_dwh.FACT_INVENTORY_AGING;

create table bec_dwh.FACT_INVENTORY_AGING 
	diststyle all
	sortkey (organization_id,
inventory_item_id,
locator_id)
as
select
	organization_id,
	inventory_item_id,
	subinventory_code,
	locator_code,
	locator_id,
	item_code,
	item_category,
	item_desc,
	uom_code,
	owing_party,
	inventory_asset_flag,
	asset_inventory,
	aging_cal,
	onhand_quantities_id,
	orig_date_received,	
	INCLUDE_EXPENSE_ITEMS,
	include_expense_sub_inventory,
	trunc(MAX(org_last_trx_date)) org_last_trx_date,
	trunc(MAX(subinv_last_trx_date)) subinv_last_trx_date,
	trunc(MAX(loc_last_trx_date)) loc_last_trx_date,
	trunc(MIN(org_first_trx_date)) org_first_trx_date,
	trunc(MIN(subinv_first_trx_date)) subinv_first_trx_date,
	trunc(MIN(loc_first_trx_date)) loc_first_trx_date,
	SUM(on_hand) on_hand,
	MAX(org_sum_on_hand) org_sum_on_hand,
	MAX(subinv_sum_on_hand) subinv_sum_on_hand,
	MAX(loc_sum_on_hand) loc_sum_on_hand,
	SUM(net_value) net_value,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || organization_id as organization_id_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || subinventory_code as subinventory_code_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || locator_id as locator_id_KEY,
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
		source_system = 'EBS')|| '-' || nvl(organization_id, 0)|| '-' || nvl(inventory_item_id, 0)
	|| '-' || nvl(subinventory_code, 'NA')|| '-' || nvl(locator_id, 0)|| '-' || nvl(onhand_quantities_id, 0)|| '-' || nvl(orig_date_received, '1900-01-01 12:00:00') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(
	select
		msib.segment1 item_code,
		msib.inventory_item_id,
		moqd.organization_id,
		moqd.subinventory_code,
		ccg.cost_group,
		msib.inventory_asset_flag,
		msiv.asset_inventory,
		mil.segment1
            || '.'
            || mil.segment2
            || '.'
            || mil.segment3 locator_code,
		mil.inventory_location_id locator_id,
		mc.segment3
            || '.'
            || mc.segment4
            || '.'
            || mc.segment1
            || '.'
            || mc.segment2
            || '.'
            || mc.segment5
            || '.'
            || mc.segment6 item_category,
		msib.description item_desc,
		decode(is_consigned,
                   1,
                   decode(moqd.owning_tp_type,
                          1,
                          (
                select
                    vendor_name
                    || '-'
                    || pvsa.vendor_site_code
                from
                    (select * from bec_ods.po_vendors where is_deleted_flg<>'Y') pv,
                    (select * from bec_ods.ap_supplier_sites_all where is_deleted_flg<>'Y') pvsa
                where
                        pvsa.vendor_id = pv.vendor_id
                    and pvsa.vendor_site_id = moqd.owning_organization_id
            ),
                          2,
                          (
                select
                    (SUBSTRING(hout.name, 1, 60)
                     || '-'
                     || SUBSTRING(mp.organization_code, 1, 3)) party
                from
                    (select * from bec_ods.hr_organization_information where is_deleted_flg<>'Y') hoi,
                    (select * from bec_ods.hr_all_organization_units_tl where is_deleted_flg<>'Y') hout,
                    (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y') mp
                where
                        hoi.organization_id = hout.organization_id
                    and hout.organization_id = mp.organization_id
                    and hout.language = 'US'
                    and hoi.org_information1 = 'OPERATING_UNIT'
                    and hoi.org_information2 = 'Y'
                    and hoi.org_information_context = 'CLASS'
                    and hoi.organization_id = moqd.owning_organization_id
                    and moqd.organization_id <> moqd.owning_organization_id
            )),
                   null) owing_party,
		MAX(moqd.last_update_date)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) org_last_trx_date,
		MAX(moqd.last_update_date)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.subinventory_code,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) subinv_last_trx_date,
		MAX(moqd.last_update_date)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.subinventory_code,
		moqd.locator_id,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) loc_last_trx_date,
		MIN(moqd.last_update_date)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) org_first_trx_date,
		MIN(moqd.last_update_date)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.subinventory_code,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) subinv_first_trx_date,
		MIN(moqd.last_update_date)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.subinventory_code,
		moqd.locator_id,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) loc_first_trx_date,
		moqd.primary_transaction_quantity on_hand,
		SUM(moqd.primary_transaction_quantity)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) org_sum_on_hand,
		SUM(moqd.primary_transaction_quantity)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.subinventory_code,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) subinv_sum_on_hand,
		SUM(moqd.primary_transaction_quantity)
            over(partition by moqd.organization_id,
		moqd.inventory_item_id,
		moqd.subinventory_code,
		moqd.locator_id,
		moqd.is_consigned,
		moqd.owning_tp_type,
		decode(moqd.is_consigned, 1, moqd.owning_organization_id, null)) loc_sum_on_hand,
		moqd.primary_transaction_quantity * nvl(decode(cql.inventory_item_id, null, cic.item_cost, cql.item_cost),
                                                    0) net_value
                                                    ,
		msib.primary_uom_code uom_code,
		trunc(nvl(moqd.orig_date_received, moqd.creation_date)) aging_cal,
		moqd.onhand_quantities_id,
		moqd.orig_date_received,
		msib.INVENTORY_ASSET_FLAG INCLUDE_EXPENSE_ITEMS,
		msiv.asset_inventory include_expense_sub_inventory
	from
		(select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg<>'Y') moqd,
		(select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msib,
		(select * from bec_ods.mtl_secondary_inventories where is_deleted_flg<>'Y') msiv,
		(select * from bec_ods.mtl_item_categories where is_deleted_flg<>'Y') mic,
		(select * from bec_ods.mtl_categories_b where is_deleted_flg<>'Y') mc,
		(select * from bec_ods.mtl_item_locations where is_deleted_flg<>'Y') mil,
		(select * from bec_ods.cst_cost_groups where is_deleted_flg<>'Y') ccg,
		(select * from bec_ods.cst_quantity_layers where is_deleted_flg<>'Y') cql,
		(select * from bec_ods.cst_item_costs where is_deleted_flg<>'Y') cic,
		(select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y') mp
	where
		moqd.organization_id = msib.organization_id
		and mp.organization_id = moqd.organization_id
		and cic.inventory_item_id = moqd.inventory_item_id
		and cql.inventory_item_id (+) = cic.inventory_item_id
		and cql.organization_id (+) = cic.organization_id
		and mp.cost_organization_id = cic.organization_id
		and mp.primary_cost_method = cic.cost_type_id
		and decode(cql.inventory_item_id,
                       null,
                       decode(mp.primary_cost_method,
                              1,
                              1,
                              nvl(mp.default_cost_group_id, 1)),
                       cql.cost_group_id) = decode(mp.primary_cost_method,
                                                   1,
                                                   1,
                                                   nvl(moqd.cost_group_id, mp.default_cost_group_id))
			and moqd.inventory_item_id = msib.inventory_item_id
			and msiv.organization_id = moqd.organization_id
			and msiv.secondary_inventory_name = moqd.subinventory_code
			and moqd.cost_group_id = ccg.cost_group_id
			and msib.organization_id = mic.organization_id
			and msib.inventory_item_id = mic.inventory_item_id
			and mc.category_id = mic.category_id
			and moqd.organization_id = mil.organization_id (+)
			and moqd.locator_id = mil.inventory_location_id (+)
			and mic.category_set_id = 1
    )
group by
	organization_id,
	inventory_item_id,
	subinventory_code,
	locator_code,
	locator_id,
	item_code,
	item_category,
	item_desc,
	uom_code,
	owing_party,
	inventory_asset_flag,
	asset_inventory,
	aging_cal,
	onhand_quantities_id,
	orig_date_received,
	include_expense_items,
	include_expense_sub_inventory;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inventory_aging'
	and batch_name = 'costing';

commit;