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
--delete records from temp table
TRUNCATE bec_dwh.fact_cst_inv_distribution_stg_tmp;
--Insert records into temp table
INSERT INTO bec_dwh.fact_cst_inv_distribution_stg_tmp 
(
select distinct transaction_id,inventory_item_id,organization_id 
from bec_ods.mtl_transaction_accounts mta
where 1=1
and nvl(mta.kca_seq_date,'2020-01-01 12:00:00.000') > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_cst_inv_distribution_stg' and batch_name = 'inv')
);
--delete records from fact table
delete from bec_dwh.fact_cst_inv_distribution_stg
WHERE exists (select 1 from bec_dwh.fact_cst_inv_distribution_stg_tmp tmp
              where tmp.transaction_id = fact_cst_inv_distribution_stg.transaction_id
			  and tmp.inventory_item_id = fact_cst_inv_distribution_stg.inventory_item_id
			  and tmp.organization_id = fact_cst_inv_distribution_stg.organization_id
             );
-- Insert records into fact table
insert into bec_dwh.fact_cst_inv_distribution_stg 
(
SELECT distinct
  REFERENCE_ACCOUNT,
  transaction_organization_id,
  transaction_id,
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  TRANSACTION_SOURCE_ID,
  COST_ELEMENT_ID,
  transaction_type_id,	
  accounting_line_type,
  transaction_reference,
  transaction_date,
  subinventory_code,
  trx_source_line_id,
  transaction_source_type_id,
  rcv_transaction_id,
  source_line_id,
  created_by,
  creation_date,
  last_update_date,
  transaction_cost,
  primary_quantity,
  unit_cost,
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
(SELECT
    MTA.REFERENCE_ACCOUNT ,
	mmt.organization_id  transaction_organization_id,
	mta.transaction_id ,
	MTA.ORGANIZATION_ID ,
	MMT.INVENTORY_ITEM_ID ,
	MMT.TRANSACTION_SOURCE_ID ,
	MTA.COST_ELEMENT_ID ,
	mmt.transaction_type_id,	
    mta.accounting_line_type,  --Join with lookup 'CST_ACCOUNTING_LINE_TYPE'
	mmt.transaction_reference,
	mta.transaction_date,
	--Description join with msi transaction_organization_id  & inventory_item_id
	mmt.subinventory_code,
	mmt.trx_source_line_id ,
	mmt.transaction_source_type_id,
	mmt.rcv_transaction_id,
    mmt.source_line_id,
	mmt.created_by,
	mmt.creation_date,
    mmt.last_update_date,
	mmt.transaction_cost,
	decode(mta.accounting_line_type,
               1,
               mta.primary_quantity,
               14,
               mta.primary_quantity,
               3,
               mta.primary_quantity,
               decode(sign(nvl(mta.base_transaction_value, 0)),
                      - 1,
                      - 1 * abs(mta.primary_quantity),
                      mta.primary_quantity)) primary_quantity,
	decode(mmt.transaction_type_id,
               24,
               NULL,
               80,
               NULL,
               decode(mta.primary_quantity, 0, NULL, NULL, NULL,mta.rate_or_amount)) unit_cost
FROM
	(select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') MTA ,
	(select * from bec_ods.MTL_MATERIAL_TRANSACTIONS where is_deleted_flg <> 'Y') MMT ,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP1,
	bec_dwh.fact_cst_inv_distribution_stg_tmp tmp
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
	AND mta.transaction_id = tmp.transaction_id
	and mta.inventory_item_id = tmp.inventory_item_id
	and mta.organization_id = tmp.organization_id
UNION ALL
SELECT
    MTA.REFERENCE_ACCOUNT ,
	mmt.organization_id  transaction_organization_id,
	mta.transaction_id ,
	MTA.ORGANIZATION_ID ,
	MMT.INVENTORY_ITEM_ID ,
	MMT.TRANSACTION_SOURCE_ID ,
	MTA.COST_ELEMENT_ID ,
	mmt.transaction_type_id,	
    mta.accounting_line_type,  --Join with lookup 'CST_ACCOUNTING_LINE_TYPE'
	mmt.transaction_reference,
	mta.transaction_date,
	--Description join with msi transaction_organization_id  & inventory_item_id
	mmt.subinventory_code,
	mmt.trx_source_line_id ,
	mmt.transaction_source_type_id,
	mmt.rcv_transaction_id,
    mmt.source_line_id,
	mmt.created_by,
	mmt.creation_date,
    mmt.last_update_date,
	mmt.transaction_cost,
	decode(mta.accounting_line_type,
               1,
               mta.primary_quantity,
               14,
               mta.primary_quantity,
               3,
               mta.primary_quantity,
               decode(sign(nvl(mta.base_transaction_value, 0)),
                      - 1,
                      - 1 * abs(mta.primary_quantity),
                      mta.primary_quantity)) primary_quantity,
	decode(mmt.transaction_type_id,
               24,
               NULL,
               80,
               NULL,
               decode(mta.primary_quantity, 0, NULL, NULL, NULL,mta.rate_or_amount)) unit_cost
FROM
	(select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') MTA ,
	(select * from bec_ods.MTL_MATERIAL_TRANSACTIONS where is_deleted_flg <> 'Y') MMT ,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP,
	(select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP1,
	bec_dwh.fact_cst_inv_distribution_stg_tmp tmp
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
	AND mta.transaction_id = tmp.transaction_id
	and mta.inventory_item_id = tmp.inventory_item_id
	and mta.organization_id = tmp.organization_id
)
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cst_inv_distribution_stg' 
  and batch_name = 'inv';
commit;