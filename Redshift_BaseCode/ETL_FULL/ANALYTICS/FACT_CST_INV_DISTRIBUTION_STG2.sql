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

drop table if exists BEC_DWH.FACT_CST_INV_DISTRIBUTION_STG2;

create table bec_dwh.fact_cst_inv_distribution_stg2
	distkey(transaction_id)
	sortkey (transaction_id,inventory_item_id,organization_id)
	as 
(
SELECT distinct
  transaction_source_type_id,
  transaction_id,
  reference_account,
  transaction_date,
  creation_date,
  last_update_date,
  inventory_item_id,
  organization_id,
  subinventory_code,
  primary_quantity,
  serial_number,
  cost_element_id,
  transaction_cost,
  unit_cost,
  trx_value,
  transaction_source_id,
  mmt_transaction_source_id,
  trx_source_line_id,
  created_by,
  rcv_transaction_id,
  source_line_id,
  transaction_organization_id,
  transaction_type_id,
  accounting_line_type,
  transaction_reference,
  'N' as Update_flg,
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
  ) || '-' || nvl(transaction_id, 0)
	|| '-' || nvl(REFERENCE_ACCOUNT, 0)
	|| '-' || nvl(COST_ELEMENT_ID, 0)
	|| '-' || nvl(accounting_line_type, 0)
	|| '-' || nvl(primary_quantity, 0)
	|| '-' || nvl(unit_cost, 0)
	as dw_load_id,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date
FROM
(
WITH CTE_mtl_unit_transactions as (
	Select 
		transaction_id,
		serial_number
	From 
		(select * from bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y')
	Group by 
		transaction_id,
		serial_number
)
	(
	SELECT
		mmt.transaction_source_type_id,
		mmt.transaction_id,
		mtrx.reference_account,
		mtrx.transaction_date,
		mmt.creation_date,
		mmt.last_update_date,
		mtrx.inventory_item_id,
		mtrx.organization_id,
		mtrx.subinventory_code,
		(mtrx.primary_quantity / ABS (mtrx.primary_quantity)
                   ) primary_quantity, 
		mut.serial_number,
		mtrx.cost_element_id ,
		mmt.transaction_cost,
		DECODE (mtrx.TRANSACTION_TYPE_ID ,
		 24,
		mmt.transaction_cost,
		mtrx.unit_cost
                          ) unit_cost,
		DECODE (mtrx.TRANSACTION_TYPE_ID,
		24,
		( mmt.transaction_cost * ( mtrx.primary_quantity / ABS(mtrx.primary_quantity))),
		( mtrx.unit_cost * (mtrx.primary_quantity / ABS(mtrx.primary_quantity)))) trx_value,
		mtrx.transaction_source_id,
		mmt.transaction_source_type_id mmt_transaction_source_id,  --use this decode statement for next toget source value
		mmt.trx_source_line_id,
		mmt.created_by,
		mmt.rcv_transaction_id,
		mmt.source_line_id,
		mtrx.transaction_organization_id,
		mtrx.TRANSACTION_TYPE_ID,
		mtrx.ACCOUNTING_LINE_TYPE,
		mtrx.transaction_reference
	FROM
		bec_dwh.fact_cst_inv_distribution_stg1 mtrx,
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
		CTE_mtl_unit_transactions mut
	WHERE
	mmt.transaction_id(+) = mtrx.transaction_id
		AND mmt.transaction_id = mut.transaction_id(+)
		AND mut.serial_number IS NOT NULL
UNION ALL
	SELECT
		mmt.transaction_source_type_id,
		mmt.transaction_id,
		mtrx.reference_account,
		mtrx.transaction_date,
		mmt.creation_date,
		mmt.last_update_date,
		mtrx.inventory_item_id,
		mtrx.organization_id,
		mtrx.subinventory_code,
		mtrx.primary_quantity primary_quantity,
		mut.serial_number,
		mtrx.cost_element_id ,
		mmt.transaction_cost,
		DECODE (mtrx.TRANSACTION_TYPE_ID,
		24,
		mmt.transaction_cost,
		mtrx.unit_cost
                          ) unit_cost,
		DECODE (mtrx.TRANSACTION_TYPE_ID,
		24,
		mmt.transaction_cost
                            * mtrx.primary_quantity,
		mtrx.unit_cost * mtrx.primary_quantity
                          ) trx_value,
		mtrx.transaction_source_id,
		mmt.transaction_source_type_id mmt_transaction_source_id,
		mmt.trx_source_line_id,
		mmt.created_by,
		mmt.rcv_transaction_id,
		mmt.source_line_id,
		mtrx.transaction_organization_id,
		mtrx.TRANSACTION_TYPE_ID,
		mtrx.ACCOUNTING_LINE_TYPE,
		mtrx.transaction_reference
	FROM
		bec_dwh.fact_cst_inv_distribution_stg1 mtrx,
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
		CTE_mtl_unit_transactions mut
	WHERE
		 mmt.transaction_id(+) = mtrx.transaction_id
		AND mmt.transaction_id = mut.transaction_id(+)
		AND mut.serial_number IS NULL )
)
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cst_inv_distribution_stg2' 
  and batch_name = 'inv';
commit;