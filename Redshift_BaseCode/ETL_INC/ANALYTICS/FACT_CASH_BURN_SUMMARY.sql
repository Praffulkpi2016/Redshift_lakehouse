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
	delete from 
	bec_dwh.FACT_CASH_BURN_SUMMARY 
	where 
	(
		NVL(CHECK_ID, 0),NVL(INVOICE_ID, 0),NVL(AE_LINE_NUM, 0)
	,NVL(AE_HEADER_ID, 0),NVL(INVOICE_PAYMENT_ID, 0)
	) in (
		select 
		nvl(ods.CHECK_ID, 0) as CHECK_ID, 
		nvl(ods.INVOICE_ID, 0) as INVOICE_ID, 
		nvl(ods.AE_LINE_NUM, 0) as AE_LINE_NUM, 
		nvl(ods.AE_HEADER_ID, 0) as AE_HEADER_ID, 
		nvl(ods.INVOICE_PAYMENT_ID, 0) as INVOICE_PAYMENT_ID
		from 
		bec_dwh.fact_cash_burn_summary dw, 
		(
			SELECT 
			DISTINCT ac.check_number,
			ac.check_date,
			ac.vendor_name,
			aip.amount Amount,
			ac.check_id,
			aip.invoice_id,
			ac.status_lookup_code,
			xel.code_combination_id,
			xel.ae_line_num, 
			xel.ae_header_id , 
			xte.entity_id,
			xeh.event_id,
			aip.INVOICE_PAYMENT_ID
			
			FROM    BEC_ODS.AP_CHECKS_ALL   ac,
			BEC_ODS.AP_INVOICE_PAYMENTS_ALL    AIP,
			--GL_CODE_COMBINATIONS_KFV GCC,
			BEC_ODS.xla_ae_lines    xel,
			BEC_ODS.xla_ae_headers    xeh,
			BEC_ODS.xla_transaction_entities    xte
			WHERE     1 = 1
	 		AND AIP.check_id = AC.check_id
			AND aip.set_of_books_id = xel.ledger_id
			AND xel.application_id = xeh.application_id
			AND xte.application_id = xeh.application_id
			-- AND xel.code_combination_id = GCC.code_combination_id
			AND xel.ae_header_id = xeh.ae_header_id
			AND xte.source_id_int_1 = aip.check_id
			and aip.ACCOUNTING_EVENT_ID = xeh.event_id 
			AND xte.entity_id = xeh.entity_id
			AND xte.entity_code = 'AP_PAYMENTS'
			AND XEL.ACCOUNTING_CLASS_CODE IN ('CASH', 'CASH_CLEARING')
			--AND AC.ORG_ID = 85
			AND xte.application_id = 200
			AND (
				ac.kca_seq_date >= (
					select 
					(executebegints - prune_days) 
					from 
					bec_etl_ctrl.batch_dw_info 
					where 
					dw_table_name = 'fact_cash_burn_summary' 
					and batch_name = 'ap'
				) 		   
				OR		aip.kca_seq_date >= (
					select 
					(executebegints - prune_days) 
					from 
					bec_etl_ctrl.batch_dw_info 
					where 
					dw_table_name = 'fact_cash_burn_summary' 
					and batch_name = 'ap'
				)
				OR		xeh.kca_seq_date >= (
					select 
					(executebegints - prune_days) 
					from 
					bec_etl_ctrl.batch_dw_info 
					where 
					dw_table_name = 'fact_cash_burn_summary' 
					and batch_name = 'ap'
				)					
				OR  	xte.kca_seq_date >= (
					select 
					(executebegints - prune_days) 
					from 
					bec_etl_ctrl.batch_dw_info 
					where 
					dw_table_name = 'fact_cash_burn_summary' 
					and batch_name = 'ap'
				)
				OR  	xel.kca_seq_date >= (
					select 
					(executebegints - prune_days) 
					from 
					bec_etl_ctrl.batch_dw_info 
					where 
					dw_table_name = 'fact_cash_burn_summary' 
					and batch_name = 'ap'
				)
				
				or ac.is_deleted_flg = 'Y'
				or aip.is_deleted_flg = 'Y'
				or xeh.is_deleted_flg = 'Y'
				or xte.is_deleted_flg = 'Y'
				or xel.is_deleted_flg = 'Y'
				
			)		
		) ods 
		where 
		dw.dw_load_id = (
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
		source_system = 'EBS'
		) || '-' || nvl(ODS.CHECK_ID, 0) || '-' || nvl(ODS.invoice_id, 0)|| '-' || nvl(ODS.ae_line_num, 0)
	|| '-' || nvl(ODS.ae_header_id, 0)|| '-' || nvl(ODS.INVOICE_PAYMENT_ID, 0)
	);
	commit;
	-- Insert records
	insert into bec_dwh.fact_cash_burn_summary (
		ORG_ID,
		check_number,
		check_date,
		vendor_name,
		amount,
		check_id,
		invoice_id,
		status_lookup_code,
		code_combination_id,
		ae_line_num,
		ae_header_id,
		entity_id,
		event_id,
		invoice_payment_id,
		check_id_key,
		invoice_id_key,
		ae_header_id_key,
		code_combination_id_key,
		is_deleted_flg,
		source_app_id,
		dw_load_id,
		dw_insert_date,
	dw_update_date
	) (
		SELECT 
		DISTINCT AC.ORG_ID,ac.check_number,
		ac.check_date,
		ac.vendor_name,
		aip.amount Amount,
		ac.check_id,
		aip.invoice_id,
		ac.status_lookup_code,
		xel.code_combination_id,
		xel.ae_line_num, 
		xel.ae_header_id , 
		xte.entity_id,
		xeh.event_id,
		aip.INVOICE_PAYMENT_ID,
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || ac.check_id as CHECK_ID_KEY, 
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || aip.invoice_id as INVOICE_ID_KEY, 
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || xel.ae_header_id as AE_HEADER_ID_KEY, 	
		(
			select 
			system_id 
			from 
			bec_etl_ctrl.etlsourceappid 
			where 
			source_system = 'EBS'
		)|| '-' || xel.code_combination_id as CODE_COMBINATION_ID_KEY,  
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
		) || '-' || nvl(AC.CHECK_ID, 0) || '-' || nvl(AIP.invoice_id, 0)|| '-' || nvl(xel.ae_line_num, 0)|| '-' || nvl(xel.ae_header_id, 0)|| '-' || nvl(aip.INVOICE_PAYMENT_ID, 0)   as dw_load_id,  
		getdate() as dw_insert_date, 
		getdate() as dw_update_date 
		FROM  (SELECT * FROM BEC_ODS.AP_CHECKS_ALL WHERE IS_DELETED_FLG <> 'Y') ac,
		(SELECT * FROM BEC_ODS.AP_INVOICE_PAYMENTS_ALL  WHERE IS_DELETED_FLG <> 'Y') AIP,
		--GL_CODE_COMBINATIONS_KFV GCC,
		(SELECT * FROM BEC_ODS.xla_ae_lines  WHERE IS_DELETED_FLG <> 'Y') xel,
		(SELECT * FROM BEC_ODS.xla_ae_headers  WHERE IS_DELETED_FLG <> 'Y') xeh,
		(SELECT * FROM BEC_ODS.xla_transaction_entities  WHERE IS_DELETED_FLG <> 'Y') xte
		WHERE     1 = 1
		AND AIP.check_id = AC.check_id
		AND aip.set_of_books_id = xel.ledger_id
		AND xel.application_id = xeh.application_id
		AND xte.application_id = xeh.application_id
		-- AND xel.code_combination_id = GCC.code_combination_id
		AND xel.ae_header_id = xeh.ae_header_id
		AND xte.source_id_int_1 = aip.check_id
		and aip.ACCOUNTING_EVENT_ID = xeh.event_id 
		AND xte.entity_id = xeh.entity_id
		AND xte.entity_code = 'AP_PAYMENTS'
		AND XEL.ACCOUNTING_CLASS_CODE IN ('CASH', 'CASH_CLEARING')
		--AND AC.ORG_ID = 85
		AND xte.application_id = 200
		AND (
			ac.kca_seq_date >= (
				select 
				(executebegints - prune_days) 
				from 
				bec_etl_ctrl.batch_dw_info 
				where 
				dw_table_name = 'fact_cash_burn_summary' 
				and batch_name = 'ap'
			) 		   
			OR		aip.kca_seq_date >= (
				select 
				(executebegints - prune_days) 
				from 
				bec_etl_ctrl.batch_dw_info 
				where 
				dw_table_name = 'fact_cash_burn_summary' 
				and batch_name = 'ap'
			)
			OR		xeh.kca_seq_date >= (
				select 
				(executebegints - prune_days) 
				from 
				bec_etl_ctrl.batch_dw_info 
				where 
				dw_table_name = 'fact_cash_burn_summary' 
				and batch_name = 'ap'
			)					
			OR  	xte.kca_seq_date >= (
				select 
				(executebegints - prune_days) 
				from 
				bec_etl_ctrl.batch_dw_info 
				where 
				dw_table_name = 'fact_cash_burn_summary' 
				and batch_name = 'ap'
			)
			OR  	xel.kca_seq_date >= (
				select 
				(executebegints - prune_days) 
				from 
				bec_etl_ctrl.batch_dw_info 
				where 
				dw_table_name = 'fact_cash_burn_summary' 
				and batch_name = 'ap'
			)
		)
	);
	
	commit;
	
end;

update 
bec_etl_ctrl.batch_dw_info 
set 
last_refresh_date = getdate() 
where 
dw_table_name = 'fact_cash_burn_summary' 
and batch_name = 'ap';
commit;
