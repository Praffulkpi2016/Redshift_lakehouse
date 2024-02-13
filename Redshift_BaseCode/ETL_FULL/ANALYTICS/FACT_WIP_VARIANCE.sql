/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
drop 
  table if exists bec_dwh.FACT_WIP_VARIANCE;
commit;
create table bec_dwh.FACT_WIP_VARIANCE diststyle all sortkey (organization_id) as (
  select 
    transaction_id, 
    organization_id, 
    wip_entity_id, 
    wip_entity_name, 
    primary_item_id, 
    line_id, 
    line_code, 
    acct_period_id, 
    trx_type_name, 
    transaction_date, 
    TRANSACTION_QUANTITY, 
    transaction_uom, 
    PRIMARY_QUANTITY, 
    primary_uom, 
    operation_seq_num, 
    CURRENCY_CODE, 
    CURRENCY_CONVERSION_DATE, 
    CURRENCY_CONVERSION_TYPE, 
    CURRENCY_CONVERSION_RATE, 
    department_id, 
    reason_name, 
    reference, 
    INVENTORY_ITEM_ID, 
    REVISION, 
    SUBINVENTORY_CODE, 
    resource_seq_num, 
    reference_account, 
    resource_id, 
    repetitive_schedule_id, 
    LINE_TYPE_NAME, 
    transaction_value, 
    base_transaction_value, 
    contra_set_id, 
    basis, 
    COST_ELEMENT, 
    ACTIVITY, 
    rate_or_amount, 
    gl_batch_id, 
    overhead_basis_factor, 
    basis_resource_id, 
    TRANSACTION_SOURCE, 
    UNIT_COST, 
    last_update_date, 
    last_updated_by, 
    creation_date, 
    created_by, 
    last_update_login, 
    request_id, 
    program_application_id, 
    program_id, 
    program_update_date, 
    asset_number, 
    asset_group_id, 
    rebuild_item_id, 
    rebuild_serial_number,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || transaction_id transaction_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || wip_entity_id wip_entity_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || department_id department_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || line_id line_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || resource_id resource_id_KEY,
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
    )|| '-' || nvl(transaction_id, 0)|| '-' || nvl(wip_entity_id, 0)|| '-' || nvl(reference_account, 0)|| '-' || nvl(COST_ELEMENT, 'NA')|| '-' || nvl(resource_id, 0)|| '-' || nvl(base_transaction_value, 0) as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM 
    (
      WITH wip_entitity as (
        select 
          we.organization_id, 
          we.wip_entity_name, 
          we.primary_item_id, 
          we.entity_type, 
          wdj.wip_entity_id, 
          wdj.rebuild_serial_number, 
          wdj.rebuild_item_id rebuild_item_id, 
          wdj.asset_group_id asset_group_id, 
          wdj.asset_number asset_number 
        from 
          (select * from bec_ods.wip_entities where is_deleted_flg <> 'Y') we, 
          (select * from bec_ods.wip_discrete_jobs where is_deleted_flg <> 'Y') wdj 
        where 
          we.wip_entity_id = wdj.wip_entity_id 
          and we.organization_id = wdj.organization_id
      ), 
      cst_activity as(
        select 
          activity_id, 
          activity 
        from 
          bec_ods.cst_activities 
		where is_deleted_flg <> 'Y'
      ), 
      trx_reasons as (
        select 
          reason_id, 
          reason_name 
        from 
          bec_ods.mtl_transaction_reasons 
		where is_deleted_flg <> 'Y'
      ), 
      csi as (
        SELECT 
          instance_number, 
          serial_number, 
          inventory_item_id 
        FROM 
          bec_ods.csi_item_instances
        where is_deleted_flg <> 'Y'
      ) (
        select 
          wt.transaction_id, 
          we.organization_id, 
          we.wip_entity_id, 
          we.wip_entity_name wip_entity_name, 
          we.primary_item_id, 
          wt.line_id, 
          NULL line_code, 
          wt.acct_period_id, 
          lu1.meaning trx_type_name, 
          wt.transaction_date, 
          decode(
            wt.transaction_type, 11, wta.primary_quantity, 
            12, wta.primary_quantity, 14, wta.primary_quantity, 
            wt.transaction_quantity
          ) TRANSACTION_QUANTITY, 
          wt.transaction_uom, 
          decode(
            wt.transaction_type, 
            11, 
            wta.primary_quantity, 
            12, 
            wta.primary_quantity, 
            14, 
            wta.primary_quantity, 
            decode(
              wta.base_transaction_value, 
              0, 
              wta.primary_quantity, 
              sign(wta.base_transaction_value) * abs(wta.primary_quantity)
            )
          ) PRIMARY_QUANTITY, 
          wt.primary_uom, 
          wt.operation_seq_num, 
          nvl(
            wta.currency_code, wt.currency_code
          ) CURRENCY_CODE, 
          nvl(
            wta.currency_conversion_date, wt.currency_conversion_date
          ) CURRENCY_CONVERSION_DATE, 
          nvl(
            wta.currency_conversion_type, wt.currency_conversion_type
          ) CURRENCY_CONVERSION_TYPE, 
          nvl(
            wta.currency_conversion_rate, wt.currency_conversion_rate
          ) CURRENCY_CONVERSION_RATE, 
          decode(
            we.entity_type, 
            6, 
            nvl(
              wt.charge_department_id, wt.department_id
            ), 
            7, 
            nvl(
              wt.charge_department_id, wt.department_id
            ), 
            wt.department_id
          ) department_id, 
          mtr.reason_name, 
          wt.reference, 
          -1 INVENTORY_ITEM_ID, 
          NULL REVISION, 
          NULL SUBINVENTORY_CODE, 
          wt.resource_seq_num, 
          wta.reference_account, 
          wta.resource_id, 
          wta.repetitive_schedule_id, 
          lu2.meaning LINE_TYPE_NAME, 
          wta.transaction_value, 
          wta.base_transaction_value, 
          wta.contra_set_id, 
          lu3.meaning basis, 
          decode(
            wta.cost_element_id, 1, 'Material', 
            2, 'Material Overhead', 3, 'Resource', 
            4, 'Outside Processing', 5, 'Overhead', 
            ' '
          ) COST_ELEMENT, 
          ca.activity ACTIVITY, 
          wta.rate_or_amount, 
          wta.gl_batch_id, 
          wta.overhead_basis_factor, 
          wta.basis_resource_id, 
          poh.segment1 TRANSACTION_SOURCE, 
          decode(
            wta.primary_quantity, 
            NULL, 
            NULL, 
            0, 
            NULL, 
            decode(
              wta.rate_or_amount, 
              NULL, 
              abs(
                wta.base_transaction_value / wta.primary_quantity
              ), 
              abs(wta.rate_or_amount)
            )
          ) UNIT_COST, 
          --wt.rowid,
          wt.last_update_date, 
          wt.last_updated_by, 
          wt.creation_date, 
          wt.created_by, 
          wt.last_update_login, 
          wt.request_id, 
          wt.program_application_id, 
          wt.program_id, 
          wt.program_update_date, 
          cii1.instance_number asset_number, 
          we.asset_group_id, 
          we.rebuild_item_id, 
          cii2.instance_number rebuild_serial_number 
        from 
          (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lu3, 
          (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lu2, 
          (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lu1, 
          (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh, 
          (select * from bec_ods.wip_transaction_accounts where is_deleted_flg <> 'Y') wta, 
          (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') wt, 
          wip_entitity we, 
          trx_reasons mtr, 
          cst_activity ca, 
          csi cii1, 
          csi cii2 
        where 
          wta.transaction_id = wt.transaction_id 
          AND we.wip_entity_id = wta.wip_entity_id 
          AND we.organization_id = wt.organization_id 
          AND lu1.lookup_type (+) = 'WIP_TRANSACTION_TYPE' 
          AND lu1.lookup_code (+) = wt.transaction_type 
          AND mtr.reason_id (+) = wt.reason_id 
          AND poh.po_header_id (+) = wt.po_header_id 
          AND lu2.lookup_type = 'CST_ACCOUNTING_LINE_TYPE' 
          AND lu2.lookup_code = wta.accounting_line_type 
          AND lu3.lookup_code (+) = wta.basis_type 
          AND lu3.lookup_type (+) = 'CST_BASIS' 
          AND ca.activity_id (+) = wta.activity_id 
          AND cii1.serial_number(+) = we.asset_number 
          AND cii1.inventory_item_id(+) = we.asset_group_id 
          AND cii2.serial_number(+) = we.rebuild_serial_number 
          AND cii2.inventory_item_id(+) = we.rebuild_item_id 
          AND lu2.meaning = 'WIP valuation' --and we.organization_id = 106 and wip_entity_name = '2301070436-POR-144522'
        UNION 
        select 
          mmt.transaction_id, 
          we.organization_id, 
          we.wip_entity_id, 
          we.wip_entity_name, 
          we.primary_item_id, 
          mmt.repetitive_line_id LINE_ID, 
          NULL line_code, 
          mmt.acct_period_id, 
          mtt.transaction_type_name TRANSACTION_TYPE_NAME, 
          mmt.transaction_date, 
          decode(
            mmt.transaction_action_id, 40, mta.primary_quantity, 
            41, mta.primary_quantity, 43, mta.primary_quantity, 
            - mmt.transaction_quantity
          ) TRANSACTION_QUANTITY, 
          mmt.transaction_uom, 
          mta.primary_quantity, 
          msi.primary_uom_code, 
          mmt.operation_seq_num, 
          nvl(
            mta.currency_code, mmt.currency_code
          ) CURRENCY_CODE, 
          nvl(
            mta.currency_conversion_date, mmt.currency_conversion_date
          ) CURRENCY_CONVERSION_DATE, 
          nvl(
            mta.currency_conversion_type, mmt.currency_conversion_type
          ) CURRENCY_CONVERSION_TYPE, 
          nvl(
            mta.currency_conversion_rate, mmt.currency_conversion_rate
          ) CURRENCY_CONVERSION_RATE, 
          mmt.department_id, 
          mtr.reason_name, 
          mmt.transaction_reference REFERENCE, 
          mmt.inventory_item_id INVENTORY_ITEM_ID, 
          mmt.revision, 
          mmt.subinventory_code, 
          NULL RESOURCE_SEQ_NUM, 
          mta.reference_account reference_account, 
          mta.resource_id, 
          mta.repetitive_schedule_id, 
          lu2.meaning LINE_TYPE_NAME, 
          mta.transaction_value, 
          mta.base_transaction_value, 
          mta.contra_set_id, 
          '' BASIS, 
          decode(
            mta.cost_element_id, 1, 'Material', 
            2, 'Material Overhead', 3, 'Resource', 
            4, 'Outside Processing', 5, 'Overhead'
          ) COST_ELEMENT, 
          ca.activity, 
          mta.rate_or_amount, 
          mta.gl_batch_id, 
          -1 OVERHEAD_BASIS_FACTOR, 
          -1 BASIS_RESOURCE_ID, 
          NULL TRANSACTION_SOURCE, 
          decode(
            mmt.primary_quantity, 
            NULL, 
            NULL, 
            0, 
            NULL, 
            decode(
              mta.rate_or_amount, 
              NULL, 
              abs(
                mta.base_transaction_value / decode(
                  mmt.transaction_action_id, 40, mta.primary_quantity, 
                  41, mta.primary_quantity, 43, mta.primary_quantity, 
                  mmt.primary_quantity
                )
              ), 
              abs(mta.rate_or_amount)
            )
          ) UNIT_COST, 
          --mmt.rowid,
          mmt.last_update_date, 
          mmt.last_updated_by, 
          mmt.creation_date, 
          mmt.created_by, 
          mmt.last_update_login, 
          mmt.request_id, 
          mmt.program_application_id, 
          mmt.program_id, 
          mmt.program_update_date, 
          cii1.instance_number asset_number, 
          we.asset_group_id, 
          we.rebuild_item_id, 
          cii2.instance_number rebuild_serial_number 
        from 
          (select * from bec_ods.mtl_transaction_types where is_deleted_flg <> 'Y') mtt, 
          (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
          (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lu2, 
          (select * from bec_ods.mtl_transaction_accounts where is_deleted_flg <> 'Y') mta, 
          (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt, 
          wip_entitity we, 
          trx_reasons mtr, 
          cst_activity ca, 
          csi cii1, 
          csi cii2 
        where 
          mta.transaction_source_id = we.wip_entity_id 
          AND mmt.organization_id = we.organization_id 
          AND mtt.transaction_type_id = mmt.transaction_type_id 
          AND mtr.reason_id (+) = mmt.reason_id 
          AND mta.transaction_source_type_id = 5 
          AND mmt.transaction_source_type_id = 5 
          AND mta.transaction_id = mmt.transaction_id 
          AND lu2.lookup_type = 'CST_ACCOUNTING_LINE_TYPE' 
          AND lu2.lookup_code = mta.accounting_line_type 
          AND msi.inventory_item_id = mmt.inventory_item_id 
          AND msi.organization_id = mta.organization_id 
          AND ca.activity_id (+) = mta.activity_id 
          AND cii1.serial_number(+) = we.asset_number 
          AND cii1.inventory_item_id(+) = we.asset_group_id 
          AND cii2.serial_number(+) = we.rebuild_serial_number 
          AND cii2.inventory_item_id(+) = we.rebuild_item_id 
          AND lu2.meaning = 'WIP valuation'
      )
    )
);
commit;
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_wip_variance' 
  and batch_name = 'wip';
commit;