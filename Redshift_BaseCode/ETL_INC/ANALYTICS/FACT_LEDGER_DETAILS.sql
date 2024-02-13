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
	
	delete
	from
	bec_dwh.FACT_LEDGER_DETAILS
	where
	nvl(LEDGER_ID, 0) in (
		select
		nvl(ods.LEDGER_ID, 0) as LEDGER_ID
		from
		bec_dwh.FACT_LEDGER_DETAILS dw,
		(
			select
			distinct
			GL.LEDGER_ID,
			DECODE(GL.LEDGER_CATEGORY_CODE, 'PRIMARY', GL.NAME,
			(select NAME from   bec_ods.GL_LEDGERS  GL_LEDGERS where LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID )) PRIMARY_LEDGER_NAME,
			DECODE(GL.LEDGER_CATEGORY_CODE, 'SECONDARY', GL.NAME) SECONDARY_LEDGER_NAME,
			GL.DESCRIPTION LEDGER_DESC,
			GL.LEDGER_CATEGORY_CODE LEDGER_CATEGORY,
			GL.CHART_OF_ACCOUNTS_ID,
			GL.CURRENCY_CODE,
			GL.PERIOD_SET_NAME,
			GL.SLA_ACCOUNTING_METHOD_CODE,
			GL.RET_EARN_CODE_COMBINATION_ID RETAINED_EARNINGS_ACCT_ID,
			GL.SLA_LEDGER_CUR_BAL_SUS_CCID SUSPENSE_ACCT_ID,
			GL.ROUNDING_CODE_COMBINATION_ID ROUNDING_ACCT_ID,
			GL.CUM_TRANS_CODE_COMBINATION_ID ADJ_ACCT_ID,
			GL.ENABLE_RECONCILIATION_FLAG RECONCILATION_FLAG
			from
			bec_ods.GL_LEDGERS   GL,
			(
				select
				GLR.TARGET_LEDGER_ID,
				GLR.TARGET_LEDGER_CATEGORY_CODE,
				GLR.SOURCE_LEDGER_ID,
				GL1.LEDGER_ID LEDGER_ID1,
				GLCD.is_deleted_flg is_deleted_flg1,
				GL1.is_deleted_flg is_deleted_flg2,
				GLR.is_deleted_flg is_deleted_flg3 
				from
				bec_ods.GL_LEDGER_CONFIG_DETAILS   GLCD,
				bec_ods.GL_LEDGERS   GL1,
				bec_ods.GL_LEDGER_RELATIONSHIPS   GLR
				where
				1 = 1
				and GLCD.SETUP_STEP_CODE = 'NONE'
				and GLCD.OBJECT_TYPE_CODE = GLR.TARGET_LEDGER_CATEGORY_CODE
				--IN ( 'PRIMARY', 'SECONDARY' )
				and GL1.CONFIGURATION_ID = GLCD.CONFIGURATION_ID
				and GL1.LEDGER_CATEGORY_CODE in ( 'PRIMARY', 'SECONDARY' )
				and GL1.LEDGER_ID = GLCD.OBJECT_ID
				and GLR.TARGET_LEDGER_ID = GLCD.OBJECT_ID
				and GLR.RELATIONSHIP_ENABLED_FLAG = 'Y'
				and ( ( GLR.TARGET_LEDGER_CATEGORY_CODE in ('PRIMARY') )
					or ( GLR.TARGET_LEDGER_CATEGORY_CODE in ('SECONDARY')
					and RELATIONSHIP_TYPE_CODE not in ('NONE') ) )
					and GLR.TARGET_LEDGER_CATEGORY_CODE = GLCD.OBJECT_TYPE_CODE
			) LEDGER_DETAILS
			where
			((GL.LEDGER_ID = LEDGER_DETAILS.TARGET_LEDGER_ID)
			or (GL.LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID))
			and GL.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_ledger_details'
			and batch_name = 'gl')   
			or GL.is_deleted_flg = 'Y'
			or LEDGER_DETAILS.is_deleted_flg1 = 'Y'
			or LEDGER_DETAILS.is_deleted_flg2 = 'Y'
			or LEDGER_DETAILS.is_deleted_flg3 = 'Y'
 			order by
			GL.LEDGER_ID
		) ods
		where
		dw.dw_load_id = 
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS'
	) || '-' || nvl(ods.LEDGER_ID, 0)
);

commit;
-- Insert records


insert
into
bec_dwh.FACT_LEDGER_DETAILS  
(
	ledger_id,
	ledger_id_key,
	primary_ledger_name,
	secondary_ledger_name,
	ledger_desc,
	ledger_category,
	chart_of_accounts_id,
	chart_of_accounts_id_key,
	currency_code,
	period_set_name,
	sla_accounting_method_code,
	retained_earnings_acct_id,
	suspense_acct_id,
	rounding_acct_id,
	adj_acct_id,
	reconcilation_flag,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
	distinct
    GL.LEDGER_ID,
	(
		select
		system_id
		from
		bec_etl_ctrl.etlsourceappid
		where
	source_system = 'EBS')|| '-' || GL.LEDGER_ID as LEDGER_ID_KEY,
	DECODE(GL.LEDGER_CATEGORY_CODE, 'PRIMARY', GL.NAME,
	(select NAME from (select * from bec_ods.GL_LEDGERS where is_deleted_flg<>'Y')GL_LEDGERS where LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID )) PRIMARY_LEDGER_NAME,
	DECODE(GL.LEDGER_CATEGORY_CODE, 'SECONDARY', GL.NAME) SECONDARY_LEDGER_NAME,
	GL.DESCRIPTION LEDGER_DESC,
	GL.LEDGER_CATEGORY_CODE LEDGER_CATEGORY,
	GL.CHART_OF_ACCOUNTS_ID,
	(
		select
		system_id
		from
		bec_etl_ctrl.etlsourceappid
		where
	source_system = 'EBS')|| '-' || GL.CHART_OF_ACCOUNTS_ID as CHART_OF_ACCOUNTS_ID_KEY,
	GL.CURRENCY_CODE,
	GL.PERIOD_SET_NAME,
	GL.SLA_ACCOUNTING_METHOD_CODE,
	GL.RET_EARN_CODE_COMBINATION_ID RETAINED_EARNINGS_ACCT_ID,
	GL.SLA_LEDGER_CUR_BAL_SUS_CCID SUSPENSE_ACCT_ID,
	GL.ROUNDING_CODE_COMBINATION_ID ROUNDING_ACCT_ID,
	GL.CUM_TRANS_CODE_COMBINATION_ID ADJ_ACCT_ID,
	GL.ENABLE_RECONCILIATION_FLAG RECONCILATION_FLAG,
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
	source_system = 'EBS')
	|| '-' || nvl(GL.LEDGER_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	from
	(select * from bec_ods.GL_LEDGERS where is_deleted_flg<>'Y') GL,
	(
		select
		GLR.TARGET_LEDGER_ID,
		GLR.TARGET_LEDGER_CATEGORY_CODE,
		GLR.SOURCE_LEDGER_ID,
		GL1.LEDGER_ID LEDGER_ID1
		from
		(select * from bec_ods.GL_LEDGER_CONFIG_DETAILS where is_deleted_flg<>'Y') GLCD,
		(select * from bec_ods.GL_LEDGERS where is_deleted_flg<>'Y') GL1,
		(select * from bec_ods.GL_LEDGER_RELATIONSHIPS where is_deleted_flg<>'Y') GLR
		where
		1 = 1
		and GLCD.SETUP_STEP_CODE = 'NONE'
		and GLCD.OBJECT_TYPE_CODE = GLR.TARGET_LEDGER_CATEGORY_CODE
		--IN ( 'PRIMARY', 'SECONDARY' )
		and GL1.CONFIGURATION_ID = GLCD.CONFIGURATION_ID
		and GL1.LEDGER_CATEGORY_CODE in ( 'PRIMARY', 'SECONDARY' )
		and GL1.LEDGER_ID = GLCD.OBJECT_ID
		and GLR.TARGET_LEDGER_ID = GLCD.OBJECT_ID
		and GLR.RELATIONSHIP_ENABLED_FLAG = 'Y'
		and ( ( GLR.TARGET_LEDGER_CATEGORY_CODE in ('PRIMARY') )
			or ( GLR.TARGET_LEDGER_CATEGORY_CODE in ('SECONDARY')
			and RELATIONSHIP_TYPE_CODE not in ('NONE') ) )
			and GLR.TARGET_LEDGER_CATEGORY_CODE = GLCD.OBJECT_TYPE_CODE
	) LEDGER_DETAILS
	where
	((GL.LEDGER_ID = LEDGER_DETAILS.TARGET_LEDGER_ID)
	or (GL.LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID))
	and GL.kca_seq_date > (
		select
		(executebegints-prune_days)
		from
		bec_etl_ctrl.batch_dw_info
		where
		dw_table_name = 'fact_ledger_details'
	and batch_name = 'gl')
	order by
	GL.LEDGER_ID
);
end;
	
	update
	bec_etl_ctrl.batch_dw_info
	set
	last_refresh_date = getdate()
	where
	dw_table_name = 'fact_ledger_details'
	and batch_name = 'gl';
	
	commit;		