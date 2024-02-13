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
	-- delete records 
	delete from bec_dwh.DIM_MTL_TRANSACTION_ACCOUNTS 
	where (nvl(transaction_id,0),
		nvl(meaning,'NA'),
		nvl(account,'NA')
	)
	in
	(
		select 
		nvl(ods.transaction_id,0) as transaction_id
		,nvl(ods.meaning,'NA') as meaning
		,nvl(ods.account,'NA')	as account
		from bec_dwh.DIM_MTL_TRANSACTION_ACCOUNTS dw,
		(
			SELECT
			c.transaction_id,
			lkp.meaning,
			c.accounting_line_type,
			SUM (c.primary_quantity) primary_quantity,
			gcc.segment1
			|| '.'
			|| gcc.segment2
			|| '.'
			|| gcc.segment3 account,
			SUM (c.base_transaction_value) trans_value
			FROM
			bec_ods.mtl_transaction_accounts   c,
			bec_ods.gl_code_combinations   gcc,
			bec_ods.fnd_lookup_values   lkp
			WHERE
			1 = 1
			AND c.reference_account = gcc.code_combination_id
			AND lkp.lookup_code = c.accounting_line_type
			AND lkp.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
			AND lkp.meaning IN ( 'Intransit Inventory', 'Profit in inventory', 'Account')
			AND c.primary_quantity > 0
			--  and TRANSACTION_id in (8388683, 8264632)
			and (c.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
			where dw_table_name ='dim_mtl_transaction_accounts' and batch_name = 'inv')
			
			or 
			gcc.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
			where dw_table_name ='dim_mtl_transaction_accounts' and batch_name = 'inv')
			or c.is_deleted_flg = 'Y'
			or gcc.is_deleted_flg = 'Y'
			or lkp.is_deleted_flg = 'Y'
			)
			GROUP BY
			c.transaction_id,
			gcc.segment1 || '.' || gcc.segment2 || '.'
			|| gcc.segment3,
			lkp.meaning,
		c.accounting_line_type)ods
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
		|| nvl(ods.transaction_id,0) 
		||'-'||nvl(ods.meaning,'NA')
		||'-'||nvl(ods.account,'NA')
	);
	commit;
	
	-- Insert Records
	
	insert into bec_dwh.DIM_MTL_TRANSACTION_ACCOUNTS 
	(
		transaction_id
		,meaning
		,accounting_line_type
		,primary_quantity
		,account
		,trans_value
		,is_deleted_flg
		,source_app_id
		,dw_load_id
		,dw_insert_date
		,dw_update_date
	)
	(
		select 
		transaction_id,
		meaning,
		accounting_line_type,
		primary_quantity,
		account,
		trans_value,
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
		|| nvl(transaction_id,0) 
		||'-'||nvl(meaning,'NA')
		||'-'||nvl(account,'NA')	   AS dw_load_id,
		getdate()           AS dw_insert_date,
		getdate()           AS dw_update_date
		from 
		(
			SELECT
			c.transaction_id,
			lkp.meaning,
			c.accounting_line_type,
			SUM (c.primary_quantity) primary_quantity,
			gcc.segment1
			|| '.'
			|| gcc.segment2
			|| '.'
			|| gcc.segment3 account,
			SUM (c.base_transaction_value) trans_value
			FROM
			(select * from bec_ods.mtl_transaction_accounts where is_deleted_flg <> 'Y') c,
			(select * from bec_ods.gl_code_combinations where is_deleted_flg <> 'Y') gcc,
			(select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lkp
			WHERE
			1 = 1
			AND c.reference_account = gcc.code_combination_id
			AND lkp.lookup_code = c.accounting_line_type
			AND lkp.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
			AND lkp.meaning IN ( 'Intransit Inventory', 'Profit in inventory', 'Account')
			AND c.primary_quantity > 0
			--  and TRANSACTION_id in (8388683, 8264632)
			and (c.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
			where dw_table_name ='dim_mtl_transaction_accounts' and batch_name = 'inv')
			OR 
			gcc.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
			where dw_table_name ='dim_mtl_transaction_accounts' and batch_name = 'inv')
			
			)
			GROUP BY
			c.transaction_id,
			gcc.segment1 || '.' || gcc.segment2 || '.'
			|| gcc.segment3,
			lkp.meaning,
		c.accounting_line_type)
	);
	commit;
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name  = 'dim_mtl_transaction_accounts'
and batch_name = 'inv';

commit;