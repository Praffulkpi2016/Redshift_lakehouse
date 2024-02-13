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

drop table if exists BEC_DWH.FACT_CST_INV_DISTRIBUTION_STG1;

create table bec_dwh.fact_cst_inv_distribution_stg1
	distkey(transaction_id)
	sortkey (transaction_id,inventory_item_id,organization_id)
	as 
(
SELECT distinct
  transaction_organization_id,
  transaction_id,
  organization_id,
  inventory_item_id,
  revision,
  subinventory_code,
  locator_id,
  transaction_type_id,
  transaction_source_type_id,
  transaction_source_id,
  transaction_source_name,
  transaction_date,
  transaction_quantity,
  transaction_uom,
  primary_quantity,
  operation_seq_num,
  currency_code,
  currency_conversion_date,
  currency_conversion_type,
  currency_conversion_rate,
  department_id,
  reason_id,
  transaction_reference,
  reference_account,
  accounting_line_type,
  transaction_value,
  base_transaction_value,
  basis_type,
  cost_element_id,
  activity_id,
  rate_or_amount,
  gl_batch_id,
  resource_id,
  unit_cost,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  mta_transaction_date,
  parent_transaction_id,
  logical_trx_type_code,
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
SELECT DISTINCT
	mmt.organization_id TRANSACTION_ORGANIZATION_ID,
	mta.transaction_id TRANSACTION_ID,
	MTA.ORGANIZATION_ID ORGANIZATION_ID,
	MMT.INVENTORY_ITEM_ID INVENTORY_ITEM_ID,
	MMT.REVISION REVISION,
	MMT.SUBINVENTORY_CODE SUBINVENTORY_CODE,
	MMT.LOCATOR_ID LOCATOR_ID,
	MMT.TRANSACTION_TYPE_ID TRANSACTION_TYPE_ID,
	MTA.TRANSACTION_SOURCE_TYPE_ID TRANSACTION_SOURCE_TYPE_ID,
	MMT.TRANSACTION_SOURCE_ID TRANSACTION_SOURCE_ID,
	MMT.TRANSACTION_SOURCE_NAME TRANSACTION_SOURCE_NAME,
	MMT.TRANSACTION_DATE TRANSACTION_DATE,
	MMT.TRANSACTION_QUANTITY TRANSACTION_QUANTITY,
	MMT.TRANSACTION_UOM TRANSACTION_UOM,
	decode (MTA.ACCOUNTING_LINE_TYPE,
	1,
	MTA.PRIMARY_QUANTITY,
	14,
	MTA.PRIMARY_QUANTITY,
	3,
	MTA.PRIMARY_QUANTITY,
	decode ( sign (nvl(MTA.BASE_TRANSACTION_VALUE, 0) ) ,
	-1,
	-1 * abs (MTA.PRIMARY_QUANTITY) ,
	MTA.Primary_quantity) ) PRIMARY_QUANTITY,
	MMT.OPERATION_SEQ_NUM OPERATION_SEQ_NUM,
	NVL(MTA.CURRENCY_CODE , MMT.CURRENCY_CODE) CURRENCY_CODE,
	NVL(MTA.CURRENCY_CONVERSION_DATE , MMT.CURRENCY_CONVERSION_DATE) CURRENCY_CONVERSION_DATE,
	MTA.CURRENCY_CONVERSION_TYPE CURRENCY_CONVERSION_TYPE,
	NVL(MTA.CURRENCY_CONVERSION_RATE , MMT.CURRENCY_CONVERSION_RATE) CURRENCY_CONVERSION_RATE,
	 MMT.DEPARTMENT_ID DEPARTMENT_ID,
	 MMT.REASON_ID REASON_ID,
	MMT.TRANSACTION_REFERENCE TRANSACTION_REFERENCE,
	MTA.REFERENCE_ACCOUNT REFERENCE_ACCOUNT,
	MTA.ACCOUNTING_LINE_TYPE ACCOUNTING_LINE_TYPE,
	MTA.TRANSACTION_VALUE TRANSACTION_VALUE,
	MTA.BASE_TRANSACTION_VALUE BASE_TRANSACTION_VALUE,
	 MTA.BASIS_TYPE BASIS_TYPE,
	MTA.COST_ELEMENT_ID COST_ELEMENT_ID,
	 MTA.ACTIVITY_ID ACTIVITY_ID,
	MTA.RATE_OR_AMOUNT RATE_OR_AMOUNT,
	MTA.GL_BATCH_ID GL_BATCH_ID,
	MTA.RESOURCE_ID RESOURCE_ID,
	DECODE ( MMT.TRANSACTION_TYPE_ID,
	24,
	NULL,
	80,
	NULL,
	DECODE ( MTA.PRIMARY_QUANTITY,
	0,
	NULL,
	NULL,
	NULL,
	MTA.RATE_OR_AMOUNT ) ) UNIT_COST,
	MTA.LAST_UPDATE_DATE,
	MTA.LAST_UPDATED_BY,
	MTA.CREATION_DATE,
	MTA.CREATED_BY,
	MTA.LAST_UPDATE_LOGIN,
	MTA.REQUEST_ID ,
	MTA.PROGRAM_APPLICATION_ID,
	MTA.PROGRAM_ID,
	MTA.PROGRAM_UPDATE_DATE,
	MTA.TRANSACTION_DATE MTA_TRANSACTION_DATE,
	MMT.PARENT_TRANSACTION_ID,
	MMT.LOGICAL_TRX_TYPE_CODE
FROM
	(select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') MTA ,
	(select * from bec_ods.MTL_MATERIAL_TRANSACTIONS where is_deleted_flg <> 'Y') MMT ,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP1
WHERE
	mta.transaction_id = mmt.transaction_id
	AND ( mmt.transaction_action_id NOT IN (2, 28, 5, 3)
		OR ( mmt.transaction_action_id IN (2, 28, 5)
			AND mmt.primary_quantity < 0
			AND ( ( (mmt.transaction_type_id != 68
				OR mta.accounting_line_type != 13)
			AND mmt.primary_quantity = mta.primary_quantity )
			OR ( mp.primary_cost_method <> 1
				AND mmt.transaction_type_id = 68
				AND mta.accounting_line_type = 13
				AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
					AND mmt.primary_quantity = decode(sign(mmt.primary_quantity),-1, mmt.primary_quantity, NULL))
					OR (mmt.cost_group_id = mmt.transfer_cost_group_id
						AND mmt.primary_quantity = -1 * mta.primary_quantity) ) )
			OR ( mp.primary_cost_method = 1
				AND mmt.transaction_type_id = 68
				AND mta.accounting_line_type = 13
				AND ( (mmt.cost_group_id <> mmt.transfer_cost_group_id
					AND mmt.project_id = mta.transaction_source_id )
				OR (mmt.cost_group_id = mmt.transfer_cost_group_id
					AND mmt.primary_quantity =-1 * mta.primary_quantity) ) ) ) )
		OR ( mmt.transaction_action_id = 3
			AND mp.primary_cost_method = 1
			AND mp1.primary_cost_method = 1
			AND mmt.organization_id = mta.organization_id )
		OR ( mmt.transaction_action_id = 3
			AND ( mp.primary_cost_method <> 1
				OR mp1.primary_cost_method <> 1 ) ) )
	AND MP.ORGANIZATION_ID = MMT.ORGANIZATION_ID
	AND MP1.ORGANIZATION_ID (+) = MMT.TRANSFER_ORGANIZATION_ID
	AND mta.inventory_item_id = mmt.inventory_item_id
	AND mta.transaction_date BETWEEN TRUNC(mmt.transaction_date) AND TRUNC(mmt.transaction_date)+ 0.99999
	AND mmt.transaction_date > getdate() - 400 
UNION ALL
SELECT DISTINCT
	mmt.organization_id TRANSACTION_ORGANIZATION_ID,
	mta.transaction_id TRANSACTION_ID,
	MTA.ORGANIZATION_ID ORGANIZATION_ID,
	MMT.INVENTORY_ITEM_ID INVENTORY_ITEM_ID,
	MMT.REVISION REVISION,
	MMT.SUBINVENTORY_CODE ,
	MMT.LOCATOR_ID ,
	MMT.TRANSACTION_TYPE_ID ,
	MMT.TRANSACTION_SOURCE_TYPE_ID , 
	MMT.TRANSACTION_SOURCE_ID ,
	MMT.TRANSACTION_SOURCE_NAME ,
	MMT.TRANSACTION_DATE ,
	MMT.TRANSACTION_QUANTITY ,
	MMT.TRANSACTION_UOM ,
	decode (MTA.ACCOUNTING_LINE_TYPE,
	1,
	MTA.PRIMARY_QUANTITY,
	14,
	MTA.PRIMARY_QUANTITY,
	3,
	MTA.PRIMARY_QUANTITY,
	decode ( sign ( nvl ( MTA.BASE_TRANSACTION_VALUE,
	0) ) ,
	-1,
	-1 * abs (MTA.PRIMARY_QUANTITY) ,
	MTA.PRIMARY_QUANTITY ) ) PRIMARY_QUANTITY,
	MMT.OPERATION_SEQ_NUM OPERATION_SEQ_NUM,
	NVL(MTA.CURRENCY_CODE , MMT.CURRENCY_CODE) CURRENCY_CODE,
	NVL(MTA.CURRENCY_CONVERSION_DATE , MMT.CURRENCY_CONVERSION_DATE) CURRENCY_CONVERSION_DATE,
	/*GLT.USER_CONVERSION_TYPE*/ MTA.CURRENCY_CONVERSION_TYPE CURRENCY_CONVERSION_TYPE,
	NVL(MTA.CURRENCY_CONVERSION_RATE , MMT.CURRENCY_CONVERSION_RATE) CURRENCY_CONVERSION_RATE,
	MMT.DEPARTMENT_ID,
	MMT.REASON_ID,
	MMT.TRANSACTION_REFERENCE ,
	MTA.REFERENCE_ACCOUNT ,
	MTA.ACCOUNTING_LINE_TYPE ,
	MTA.TRANSACTION_VALUE ,
	MTA.BASE_TRANSACTION_VALUE ,
	MTA.BASIS_TYPE,
	MTA.COST_ELEMENT_ID ,
	MTA.ACTIVITY_ID,
	MTA.RATE_OR_AMOUNT ,
	MTA.GL_BATCH_ID ,
	MTA.RESOURCE_ID,
	DECODE ( MMT.TRANSACTION_TYPE_ID,
	24,
	NULL,
	80,
	NULL,
	DECODE ( MTA.PRIMARY_QUANTITY,
	0,
	NULL,
	NULL,
	NULL,
	MTA.RATE_OR_AMOUNT ) ) UNIT_COST,
	MTA.LAST_UPDATE_DATE,
	MTA.LAST_UPDATED_BY,
	MTA.CREATION_DATE,
	MTA.CREATED_BY,
	MTA.LAST_UPDATE_LOGIN,
	MTA.REQUEST_ID ,
	MTA.PROGRAM_APPLICATION_ID,
	MTA.PROGRAM_ID,
	MTA.PROGRAM_UPDATE_DATE,
	MTA.TRANSACTION_DATE MTA_TRANSACTION_DATE,
	MMT.PARENT_TRANSACTION_ID,
	MMT.LOGICAL_TRX_TYPE_CODE
FROM
	(select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') MTA ,
	(select * from bec_ods.MTL_MATERIAL_TRANSACTIONS where is_deleted_flg <> 'Y') MMT ,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP1
WHERE
	mta.transaction_id = mmt.transfer_transaction_id
	AND ( (mmt.transaction_action_id IN (2, 28, 5)
		AND mmt.primary_quantity > 0
		AND ( ( (mmt.transaction_type_id != 68
			OR mta.accounting_line_type != 13)
		AND mmt.primary_quantity = mta.primary_quantity )
		OR ( mp.primary_cost_method <> 1
			AND mmt.transaction_type_id = 68
			AND mta.accounting_line_type = 13
			AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
				AND mmt.primary_quantity = decode(sign(mmt.primary_quantity),-1, mmt.primary_quantity, NULL))
				OR (mmt.cost_group_id = mmt.transfer_cost_group_id
					AND mmt.primary_quantity = -1 * mta.primary_quantity)) )
		OR ( mp.primary_cost_method = 1
			AND mmt.transaction_type_id = 68
			AND mta.accounting_line_type = 13
			AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
				AND mmt.project_id = mta.transaction_source_id)
			OR (mmt.cost_group_id = mmt.transfer_cost_group_id
				AND mmt.primary_quantity = -1 * mta.primary_quantity) ) ) ) )
	OR ( mmt.transaction_action_id = 3
		AND mp.primary_cost_method = 1
		AND mp1.primary_cost_method = 1
		AND mmt.organization_id = mta.organization_id ) )
	AND MP.ORGANIZATION_ID = MMT.ORGANIZATION_ID
	AND MP1.ORGANIZATION_ID (+) = MMT.TRANSFER_ORGANIZATION_ID
	AND mta.inventory_item_id = mmt.inventory_item_id
	AND mta.transaction_date BETWEEN TRUNC(mmt.transaction_date) AND TRUNC(mmt.transaction_date)+ 0.99999
	AND mmt.transaction_date > getdate() - 400 
)
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cst_inv_distribution_stg1' 
  and batch_name = 'inv';
commit;