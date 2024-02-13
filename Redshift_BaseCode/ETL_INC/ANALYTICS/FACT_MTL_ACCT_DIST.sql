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
 --delete records
delete from bec_dwh.FACT_MTL_ACCT_DIST
where exists 
(
select 1 
from bec_ods.mtl_transaction_accounts ods
where 1=1
and ods.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_mtl_acct_dist' and batch_name = 'costing')
and fact_mtl_acct_dist.transaction_id=ods.transaction_id 
and fact_mtl_acct_dist.inventory_item_id = ods.inventory_item_id
and fact_mtl_acct_dist.organization_id = ods.organization_id
);
commit;

--insert records
Insert into bec_dwh.fact_mtl_acct_dist
(
SELECT            
        Transaction_id,
        organization_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(organization_id,0) as organization_id_key,
		inv_sub_ledger_id, 	
        inventory_item_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(inventory_item_id,0) as inventory_item_id_key, 
		BOM_department_id, 
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(BOM_department_id,0) as BOM_department_id_key,
		bom_resource_id ,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(bom_resource_id,0) as bom_resource_id_key,
		 item,
		 item_description,
        revision,
        subinventory_code,
        locator_id,
        transaction_type_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(transaction_type_id,0) as transaction_type_id_key,
        transaction_type_name,
        transaction_source_type_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || nvl(transaction_source_type_id,0) as transaction_source_type_id_key,
        transaction_source_type_name,
        transaction_source_id,
        transaction_source_name,
        transaction_date,
        transaction_quantity,
        transaction_uom,
		PRIMARY_QUANTITY,
        primary_uom, 
        operation_seq_num,
        CURRENCY_CODE,
        CURRENCY_CONVERSION_DATE,
        CURRENCY_CONVERSION_TYPE,
        CURRENCY_CONVERSION_RATE,
        department_code,
        reason_name,
        transaction_reference,
        reference_account,
        accounting_line_type,
        LINE_TYPE_NAME,
        transaction_value,
        base_transaction_value,
        BASIS_TYPE_NAME,
        cost_element_id,
        activity,
        rate_or_amount,
        gl_batch_id,
        resource_code,
        UNIT_COST,
        parent_transaction_id,
        organization_code,
        LOGICAL_TRX_TYPE,
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
				|| '-' || nvl(transaction_id, 0)				
				|| '-' || nvl(INV_SUB_LEDGER_ID, 0)
				|| '-' || nvl(cost_element_id, 0)
				|| '-' || nvl(reference_account, 0)
				|| '-' || nvl(resource_code, 'NA')
						as dw_load_id,
			getdate() as dw_insert_date,
			getdate() as dw_update_date
             FROM (
		SELECT
        mta.transaction_id,
        mta.organization_id, 
		mta.inv_sub_ledger_id,
        mmt.inventory_item_id,
		mmt.department_id BOM_department_id,
		mta.resource_id bom_resource_id,
		msi.segment1 item,
        REPLACE (REPLACE (REPLACE (REPLACE (msi.description, '&', ' AND '),'<',' '),'>',' '),'ï¿½','DEG ') item_description,
        mmt.revision,
        NVL (REPLACE (REPLACE (REPLACE (REPLACE (mmt.subinventory_code, '&', '^%$'),'<',' '),'>',' '),'ï¿½','DEG '),'.')subinventory_code,
        mmt.locator_id,
        mmt.transaction_type_id,
        mtt.transaction_type_name,
        mta.transaction_source_type_id,
        mtst.transaction_source_type_name,
        mmt.transaction_source_id,
        mmt.transaction_source_name,
        mmt.transaction_date,
        mmt.transaction_quantity,
        mmt.transaction_uom,
        decode(mta.accounting_line_type, 1, mta.primary_quantity, 14, mta.primary_quantity,3, mta.primary_quantity, 
		decode(sign(nvl(mta.base_transaction_value, 0)), - 1, - 1 * abs(mta.primary_quantity), mta.primary_quantity)) PRIMARY_QUANTITY,
        msi.primary_uom_code primary_uom, 
        mmt.operation_seq_num,
        nvl(mta.currency_code, mmt.currency_code) CURRENCY_CODE,
        nvl(mta.currency_conversion_date, mmt.currency_conversion_date) CURRENCY_CONVERSION_DATE,
        glt.user_conversion_type CURRENCY_CONVERSION_TYPE,
        nvl(mta.currency_conversion_rate, mmt.currency_conversion_rate) CURRENCY_CONVERSION_RATE,
        bd.department_code,
        mtr.reason_name,
        mmt.transaction_reference,
        mta.reference_account,
        mta.accounting_line_type,
        lu1.meaning LINE_TYPE_NAME,
        mta.transaction_value,
        mta.base_transaction_value,
        lu2.meaning BASIS_TYPE_NAME,
        mta.cost_element_id,
        ca.activity,
        (NVL (ABS (mta.rate_or_amount),(mta.base_transaction_value / NVL (mta.primary_quantity, 1))))rate_or_amount,
        mta.gl_batch_id,
        br.resource_code,
        decode(mmt.transaction_type_id, 24, NULL, 80, NULL,decode(mta.primary_quantity, 0, NULL, NULL, NULL,mta.rate_or_amount)) UNIT_COST,
        mmt.parent_transaction_id,
        mp2.organization_code,
        lu3.meaning LOGICAL_TRX_TYPE
    FROM
        (select * from bec_ods.mtl_transaction_accounts where is_deleted_flg<>'Y'
		and kca_seq_date > (select (executebegints-prune_days) 
		from bec_etl_ctrl.batch_dw_info 
		where dw_table_name ='fact_mtl_acct_dist' and batch_name = 'costing'))  mta,
        (select * from bec_ods.mtl_material_transactions where is_deleted_flg<>'Y') mmt,
        (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y')            mp,
        (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y')            mp1,
        (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y')            mp2,
        (select * from bec_ods.cst_activities where is_deleted_flg<>'Y')            ca,
        (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y')        msi,
        (select * from bec_ods.bom_departments where is_deleted_flg<>'Y')           bd,
        (select * from bec_ods.bom_resources where is_deleted_flg<>'Y')             br,
        (select * from bec_ods.mtl_transaction_reasons where is_deleted_flg<>'Y')   mtr,
        (select * from bec_ods.mtl_txn_source_types where is_deleted_flg<>'Y')      mtst,
        (select * from bec_ods.mtl_transaction_types where is_deleted_flg<>'Y')     mtt,
        (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg<>'Y')               lu1,
        (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg<>'Y')               lu2,
        (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg<>'Y')               lu3,
        (select * from bec_ods.gl_daily_conversion_types where is_deleted_flg<>'Y') glt
    WHERE 1=1
        AND  mta.transaction_id = mmt.transaction_id
        AND ( mmt.transaction_action_id NOT IN ( 2, 28, 5, 3 )
              OR ( mmt.transaction_action_id IN ( 2, 28, 5 )
                   AND mmt.primary_quantity < 0
                   AND ( ( ( mmt.transaction_type_id != 68
                             OR mta.accounting_line_type != 13 )
                           AND mmt.primary_quantity = mta.primary_quantity )
                         OR ( mp.primary_cost_method <> 1
                              AND mmt.transaction_type_id = 68
                              AND mta.accounting_line_type = 13
                              AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
                                      AND mmt.primary_quantity = decode(sign(mmt.primary_quantity), - 1, mmt.primary_quantity, NULL) )
                                    OR ( mmt.cost_group_id = mmt.transfer_cost_group_id
                                         AND mmt.primary_quantity = - 1 * mta.primary_quantity ) ) )
                         OR ( mp.primary_cost_method = 1
                              AND mmt.transaction_type_id = 68
                              AND mta.accounting_line_type = 13
                              AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
                                      AND mmt.project_id = mta.transaction_source_id )
                                    OR ( mmt.cost_group_id = mmt.transfer_cost_group_id
                                         AND mmt.primary_quantity = - 1 * mta.primary_quantity ) ) ) ) )
              OR ( mmt.transaction_action_id = 3
                   AND mp.primary_cost_method = 1
                   AND mp1.primary_cost_method = 1
                   AND mmt.organization_id = mta.organization_id )
              OR ( mmt.transaction_action_id = 3
                   AND ( mp.primary_cost_method <> 1
                         OR mp1.primary_cost_method <> 1 ) ) )
        AND msi.inventory_item_id = mmt.inventory_item_id
        AND msi.organization_id = mmt.organization_id
        AND mp.organization_id = mmt.organization_id
        AND mp1.organization_id (+) = mmt.transfer_organization_id
        AND mp2.organization_id = mta.organization_id
        AND mtt.transaction_type_id = mmt.transaction_type_id
        AND mtst.transaction_source_type_id = mta.transaction_source_type_id
        AND bd.department_id (+) = mmt.department_id
        AND mtr.reason_id (+) = mmt.reason_id
        AND br.resource_id (+) = mta.resource_id
        AND lu1.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
        AND lu1.lookup_code = mta.accounting_line_type
        AND lu2.lookup_code (+) = mta.basis_type
        AND lu2.lookup_type (+) = 'CST_BASIS_SHORT'
        AND lu3.lookup_type (+) = 'MTL_LOGICAL_TRANSACTION_CODE'
        AND lu3.lookup_code (+) = mmt.logical_trx_type_code
        AND glt.conversion_type (+) = mta.currency_conversion_type
        AND ca.activity_id (+) = mta.activity_id
        AND mta.inventory_item_id = mmt.inventory_item_id
    UNION ALL
    SELECT
        mta.transaction_id,
        mta.organization_id,
		mta.inv_sub_ledger_id,		
        mmt.inventory_item_id,
		mmt.department_id BOM_department_id,
		mta.resource_id bom_resource_id,
		msi.segment1 item,
        REPLACE (REPLACE (REPLACE (REPLACE (msi.description, '&', ' AND '),'<',' '),'>',' '),'ï¿½','DEG ') item_description,
        mmt.revision,
        NVL (REPLACE (REPLACE (REPLACE (REPLACE (mmt.subinventory_code, '&', '^%$'),'<',' '),'>',' '),'ï¿½','DEG '),'.')subinventory_code,
        mmt.locator_id,
        mmt.transaction_type_id,
        mtt.transaction_type_name,
        mmt.transaction_source_type_id,
        mtst.transaction_source_type_name,
        mmt.transaction_source_id,
        mmt.transaction_source_name,
        mmt.transaction_date,
        mmt.transaction_quantity,
        mmt.transaction_uom,
        decode(mta.accounting_line_type, 1, mta.primary_quantity, 14, mta.primary_quantity,
               3, mta.primary_quantity, decode(sign(nvl(mta.base_transaction_value, 0)), - 1, - 1 * abs(mta.primary_quantity), mta.primary_quantity)) PRIMARY_QUANTITY,
        msi.primary_uom_code primary_uom,
        mmt.operation_seq_num,
        nvl(mta.currency_code, mmt.currency_code) CURRENCY_CODE,
        nvl(mta.currency_conversion_date, mmt.currency_conversion_date) CURRENCY_CONVERSION_DATE, 
        glt.user_conversion_type CURRENCY_CONVERSION_TYPE,
        nvl(mta.currency_conversion_rate, mmt.currency_conversion_rate) CURRENCY_CONVERSION_RATE,
        bd.department_code,
        mtr.reason_name,
        mmt.transaction_reference,
        mta.reference_account,
        mta.accounting_line_type,
        lu1.meaning LINE_TYPE_NAME,
        mta.transaction_value,
        mta.base_transaction_value,
        lu2.meaning BASIS_TYPE_NAME,
        mta.cost_element_id,
        ca.activity,
        (NVL (ABS (mta.rate_or_amount),(mta.base_transaction_value / NVL (mta.primary_quantity, 1))))rate_or_amount,
        mta.gl_batch_id,
        br.resource_code,
        decode(mmt.transaction_type_id, 24, NULL, 80, NULL,
               decode(mta.primary_quantity, 0, NULL, NULL, NULL, /* Commented and replaced for bug 8543247 - decode(MTA.ACCOUNTING_LINE_TYPE, 1, MTA.BASE_TRANSACTION_VALUE/MTA.PRIMARY_QUANTITY, 14, MTA.BASE_TRANSACTION_VALUE/MTA.PRIMARY_QUANTITY, 3, MTA.BASE_TRANSACTION_VALUE/MTA.PRIMARY_QUANTITY, ABS ( MTA.BASE_TRANSACTION_VALUE / MTA.PRIMARY_QUANTITY ) )*/
                      mta.rate_or_amount)) UNIT_COST,
        mmt.parent_transaction_id,
        mp2.organization_code,
        lu3.meaning LOGICAL_TRX_TYPE
    FROM		
        (select * from bec_ods.mtl_transaction_accounts where is_deleted_flg<>'Y'
		and kca_seq_date > (select (executebegints-prune_days) 
		from bec_etl_ctrl.batch_dw_info 
		where dw_table_name ='fact_mtl_acct_dist' and batch_name = 'costing'))  mta,
        (select * from bec_ods.mtl_material_transactions where is_deleted_flg<>'Y') mmt,
        (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y')            mp,
        (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y')            mp1,
        (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y')            mp2,
        (select * from bec_ods.cst_activities where is_deleted_flg<>'Y')            ca,
        (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y')        msi,
        (select * from bec_ods.bom_departments where is_deleted_flg<>'Y')           bd,
        (select * from bec_ods.bom_resources where is_deleted_flg<>'Y')             br,
        (select * from bec_ods.mtl_transaction_reasons where is_deleted_flg<>'Y')   mtr,
        (select * from bec_ods.mtl_txn_source_types where is_deleted_flg<>'Y')      mtst,
        (select * from bec_ods.mtl_transaction_types where is_deleted_flg<>'Y')     mtt,
        (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg<>'Y')               lu1,
        (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg<>'Y')               lu2,
        (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg<>'Y')               lu3,
        (select * from bec_ods.gl_daily_conversion_types where is_deleted_flg<>'Y') glt
    WHERE 1=1
        AND   mta.transaction_id = mmt.transfer_transaction_id
        AND ( ( mmt.transaction_action_id IN ( 2, 28, 5 )
                AND mmt.primary_quantity > 0
                AND ( ( ( mmt.transaction_type_id != 68
                          OR mta.accounting_line_type != 13 )
                        AND mmt.primary_quantity = mta.primary_quantity )
                      OR ( mp.primary_cost_method <> 1
                           AND mmt.transaction_type_id = 68
                           AND mta.accounting_line_type = 13
                           AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
                                   AND mmt.primary_quantity = decode(sign(mmt.primary_quantity), - 1, mmt.primary_quantity, NULL) )
                                 OR ( mmt.cost_group_id = mmt.transfer_cost_group_id
                                      AND mmt.primary_quantity = - 1 * mta.primary_quantity ) ) )
                      OR ( mp.primary_cost_method = 1
                           AND mmt.transaction_type_id = 68
                           AND mta.accounting_line_type = 13
                           AND ( ( mmt.cost_group_id <> mmt.transfer_cost_group_id
                                   AND mmt.project_id = mta.transaction_source_id )
                                 OR ( mmt.cost_group_id = mmt.transfer_cost_group_id
                                      AND mmt.primary_quantity = - 1 * mta.primary_quantity ) ) ) ) )
              OR ( mmt.transaction_action_id = 3
                   AND mp.primary_cost_method = 1
                   AND mp1.primary_cost_method = 1
                   AND mmt.organization_id = mta.organization_id ) )
        AND msi.inventory_item_id = mmt.inventory_item_id
        AND msi.organization_id = mmt.organization_id
        AND mp.organization_id = mmt.organization_id
        AND mp1.organization_id (+) = mmt.transfer_organization_id
        AND mp2.organization_id = mta.organization_id
        AND mtt.transaction_type_id = mmt.transaction_type_id
        AND mtst.transaction_source_type_id = mmt.transaction_source_type_id
        AND bd.department_id (+) = mmt.department_id
        AND mtr.reason_id (+) = mmt.reason_id
        AND br.resource_id (+) = mta.resource_id
        AND lu1.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
        AND lu1.lookup_code = mta.accounting_line_type
        AND lu2.lookup_code (+) = mta.basis_type
        AND lu2.lookup_type (+) = 'CST_BASIS_SHORT'
        AND lu3.lookup_type (+) = 'MTL_LOGICAL_TRANSACTION_CODE'
        AND lu3.lookup_code (+) = mmt.logical_trx_type_code
        AND glt.conversion_type (+) = mta.currency_conversion_type
        AND ca.activity_id (+) = mta.activity_id
        AND mta.inventory_item_id = mmt.inventory_item_id )) ; --3years data --5,19,92008 records took around 15 mins
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_mtl_acct_dist'
	and batch_name = 'costing';

commit;		 