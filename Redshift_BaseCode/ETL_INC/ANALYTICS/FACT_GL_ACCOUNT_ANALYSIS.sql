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
	bec_dwh.FACT_GL_ACCOUNT_ANALYSIS 
	where 
	(
		nvl(LINE_NUMBER, 0), 
		nvl(HEADER_ID, 0), 
		nvl(LEDGER_CURRENCY, 'NA'), 
		nvl(TRANSLATED_FLAG, 'NA'), 
	nvl(TEMP_LINE_NUM, 0)
	) in (
		select 
		nvl(ods.LINE_NUMBER, 0) as LINE_NUMBER, 
		nvl(ods.HEADER_ID, 0) as HEADER_ID, 
		nvl(ods.LEDGER_CURRENCY, 'NA') as LEDGER_CURRENCY, 
		nvl(ods.TRANSLATED_FLAG, 'NA') as TRANSLATED_FLAG, 
		nvl(ods.TEMP_LINE_NUM, 0) as TEMP_LINE_NUM 
		from 
		bec_dwh.FACT_GL_ACCOUNT_ANALYSIS dw, 
		(
			SELECT 
			A.GL_DATE, 
			A.created_by_id, 
			A.creation_date, 
			A.last_update_date, 
			A.GL_TRANSFER_DATE, 
			A.reference_date, 
			A.COMPLETED_DATE, 
			A.TRANSACTION_NUMBER, 
			A.TRANSACTION_DATE, 
			A.ACCOUNTING_SEQUENCE_NAME, 
			A.ACCOUNTING_SEQUENCE_VERSION, 
			A.ACCOUNTING_SEQUENCE_NUMBER, 
			A.REPORTING_SEQUENCE_NAME, 
			A.REPORTING_SEQUENCE_VERSION, 
			A.REPORTING_SEQUENCE_NUMBER, 
			A.DOCUMENT_CATEGORY, 
			A.DOCUMENT_SEQUENCE_NAME, 
			A.DOCUMENT_SEQUENCE_NUMBER, 
			A.APPLICATION_ID, 
			A.APPLICATION_NAME, 
			A.HEADER_ID, 
			A.HEADER_DESCRIPTION, 
			A.FUND_STATUS, 
			A.je_category, 
			A.JE_SOURCE_NAME, 
			A.EVENT_ID, 
			A.EVENT_DATE, 
			A.EVENT_NUMBER, 
			A.EVENT_CLASS_CODE, 
			A.EVENT_CLASS_NAME, 
			A.EVENT_TYPE_CODE, 
			A.EVENT_TYPE_NAME, 
			A.GL_BATCH_NAME, 
			A.POSTED_DATE, 
			A.je_header_id, 
			A.GL_JE_NAME, 
			A.REVERSAL_PERIOD, 
			A.REVERSAL_STATUS, 
			A.EXTERNAL_REFERENCE, 
			A.GL_LINE_NUMBER, 
			A.effective_date, 
			A.LINE_NUMBER, 
			A.ORIG_LINE_NUMBER, 
			A.ACCOUNTING_CLASS_CODE, 
			A.ACCOUNTING_CLASS_NAME, 
			A.LINE_DESCRIPTION, 
			A.ENTERED_CURRENCY, 
			A.CONVERSION_RATE, 
			A.CONVERSION_RATE_DATE, 
			A.CONVERSION_RATE_TYPE_CODE, 
			A.CONVERSION_RATE_TYPE, 
			A.ENTERED_DR, 
			A.ENTERED_CR, 
			A.UNROUNDED_ACCOUNTED_DR, 
			A.UNROUNDED_ACCOUNTED_CR, 
			A.ACCOUNTED_DR, 
			A.ACCOUNTED_CR, 
			A.STATISTICAL_AMOUNT, 
			A.RECONCILIATION_REFERENCE, 
			A.PARTY_TYPE_CODE, 
			A.PARTY_TYPE, 
			A.LEDGER_ID, 
			A.LEDGER_SHORT_NAME, 
			A.LEDGER_DESCRIPTION, 
			A.LEDGER_NAME, 
			A.LEDGER_CURRENCY, 
			A.PERIOD_YEAR, 
			A.PERIOD_NUMBER, 
			A.PERIOD_NAME, 
			A.PERIOD_START_DATE, 
			A.PERIOD_END_DATE, 
			A.BALANCE_TYPE_CODE, 
			A.BALANCE_TYPE, 
			A.BUDGET_NAME, 
			A.ENCUMBRANCE_TYPE, 
			A.BEGIN_BALANCE_DR, 
			A.BEGIN_BALANCE_CR, 
			A.PERIOD_NET_DR, 
			A.PERIOD_NET_CR, 
			A.CODE_COMBINATION_ID, 
			A.ACCOUNTING_CODE_COMBINATION, 
			A.CODE_COMBINATION_DESCRIPTION, 
			A.CONTROL_ACCOUNT_FLAG, 
			A.CONTROL_ACCOUNT, 
			A.BALANCING_SEGMENT, 
			A.NATURAL_ACCOUNT_SEGMENT, 
			A.COST_CENTER_SEGMENT, 
			A.MANAGEMENT_SEGMENT, 
			A.INTERCOMPANY_SEGMENT, 
			A.BALANCING_SEGMENT_DESC, 
			A.NATURAL_ACCOUNT_DESC, 
			A.COST_CENTER_DESC, 
			A.MANAGEMENT_SEGMENT_DESC, 
			A.INTERCOMPANY_SEGMENT_DESC, 
			A.segment1, 
			A.segment2, 
			A.segment3, 
			A.segment4, 
			A.segment5, 
			A.segment6, 
			A.segment7, 
			A.segment8, 
			A.segment9, 
			A.segment10, 
			A.BEGIN_RUNNING_TOTAL_CR, 
			A.BEGIN_RUNNING_TOTAL_DR, 
			A.END_RUNNING_TOTAL_CR, 
			A.END_RUNNING_TOTAL_DR, 
			A.LEGAL_ENTITY_ID, 
			A.LEGAL_ENTITY_NAME, 
			A.LE_ADDRESS_LINE_1, 
			A.LE_ADDRESS_LINE_2, 
			A.LE_ADDRESS_LINE_3, 
			A.LE_CITY, 
			A.LE_REGION_1, 
			A.LE_REGION_2, 
			A.LE_REGION_3, 
			A.LE_POSTAL_CODE, 
			A.LE_COUNTRY, 
			A.LE_REGISTRATION_NUMBER, 
			A.LE_REGISTRATION_EFFECTIVE_FROM, 
			A.LE_BR_DAILY_INSCRIPTION_NUMBER, 
			A.LE_BR_DAILY_INSCRIPTION_DATE, 
			A.LE_BR_DAILY_ENTITY, 
			A.LE_BR_DAILY_LOCATION, 
			A.LE_BR_DIRECTOR_NUMBER, 
			A.LE_BR_ACCOUNTANT_NUMBER, 
			A.LE_BR_ACCOUNTANT_NAME, 
			A.BE_PROJECT_NO, 
			A.BE_PROJECT_DESC, 
			A.BE_TASK_NO, 
			A.BE_TASK_DESC, 
			A.approver_name, 
			A.PO_NUMBER, 
			A.invoice_num, 
			A.INVOICE_ID, 
			A.vendor_id, 
			A.vendor_site_id, 
			A.TRANSLATED_FLAG, 
			A.TEMP_LINE_NUM, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.je_category as je_category_KEY, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.je_header_id as je_header_id_KEY, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.LEDGER_ID as LEDGER_ID_KEY, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.LEGAL_ENTITY_ID as LEGAL_ENTITY_ID_KEY, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.INVOICE_ID as INVOICE_ID_KEY, 
			(
				select 
				system_id 
				from 
				bec_etl_ctrl.etlsourceappid 
				where 
				source_system = 'EBS'
			)|| '-' || A.vendor_id as vendor_id_KEY -- audit columns
			, 
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
			) || '-' || nvl(A.LINE_NUMBER, 0) || '-' || nvl(A.HEADER_ID, 0) || '-' || nvl(A.LEDGER_CURRENCY, 'NA') || '-' || nvl(A.TRANSLATED_FLAG, 'NA') || '-' || nvl(A.TEMP_LINE_NUM, 0) as dw_load_id, 
			getdate() as dw_insert_date, 
			getdate() as dw_update_date 
			from 
			(
				(
					with CTE_PO_AP as (
						SELECT 
						pa1.segment1, 
						pa1.name, 
						pa1.description BE_PROJECT_DESC, 
						pt1.task_number, 
						pt1.description BE_TASK_DESC, 
						PHA.SEGMENT1 as PO_NUMBER, 
						AIA.invoice_num, 
						AIA.INVOICE_ID, 
						AIA.vendor_id, 
						AIA.vendor_site_id, 
						apd1.invoice_distribution_id 
						From 
						bec_ods.ap_invoice_distributions_all  apd1, 
						bec_ods.po_distributions_all   pda, 
						bec_ods.AP_INVOICES_ALL   AIA, 
						bec_ods.po_headers_all   pha, 
						bec_ods.pa_projects_all  pa1, 
						bec_ods.pa_tasks   pt1 
						WHERE 
						1 = 1 
						AND pt1.task_id (+) = apd1.task_id 
						AND apd1.INVOICE_ID = AIA.INVOICE_ID 
						AND pa1.project_id (+)= apd1.project_id 
						AND PDA.PO_DISTRIBUTION_ID (+) = apd1.PO_DISTRIBUTION_ID 
						AND PHA.PO_HEADER_ID(+) = PDA.PO_HEADER_ID 
						AND (
							apd1.kca_seq_date >= (
								select 
								(executebegints - prune_days) 
								from 
								bec_etl_ctrl.batch_dw_info 
								where 
								dw_table_name = 'fact_gl_account_analysis' 
								and batch_name = 'gl'
							) 
							OR AIA.kca_seq_date >= (
								select 
								(executebegints - prune_days) 
								from 
								bec_etl_ctrl.batch_dw_info 
								where 
								dw_table_name = 'fact_gl_account_analysis' 
								and batch_name = 'gl'
							)
							or apd1.is_deleted_flg = 'Y'
							or pda.is_deleted_flg = 'Y'
							or AIA.is_deleted_flg = 'Y'
							or pha.is_deleted_flg = 'Y'
							or pa1.is_deleted_flg = 'Y'
							or pt1.is_deleted_flg = 'Y'
							
						)
					) 
					SELECT 
					XLA.GL_DATE, 
					OTT.created_by_id, 
					XLA.creation_date, 
					XLA.last_update_date, 
					XLA.GL_TRANSFER_DATE, 
					XLA.reference_date, 
					XLA.COMPLETED_DATE, 
					XLA.TRANSACTION_NUMBER, 
					XLA.TRANSACTION_DATE, 
					XLA.ACCOUNTING_SEQUENCE_NAME, 
					XLA.ACCOUNTING_SEQUENCE_VERSION, 
					XLA.ACCOUNTING_SEQUENCE_NUMBER, 
					XLA.REPORTING_SEQUENCE_NAME, 
					XLA.REPORTING_SEQUENCE_VERSION, 
					XLA.REPORTING_SEQUENCE_NUMBER, 
					NULL DOCUMENT_CATEGORY, 
					XLA.DOCUMENT_SEQUENCE_NAME, 
					XLA.DOCUMENT_SEQUENCE_NUMBER, 
					XLA.APPLICATION_ID, 
					NULL APPLICATION_NAME, 
					XLA.HEADER_ID, 
					XLA.HEADER_DESCRIPTION, 
					XLA.FUND_STATUS, 
					XLA.je_category_name as je_category, 
					OTT.JE_SOURCE_NAME, 
					XLA.EVENT_ID, 
					XLA.EVENT_DATE, 
					XLA.EVENT_NUMBER, 
					XLA.EVENT_CLASS_CODE, 
					XLA.EVENT_CLASS_NAME, 
					XLA.EVENT_TYPE_CODE, 
					XLA.EVENT_TYPE_NAME, 
					OTT.GL_BATCH_NAME, 
					OTT.POSTED_DATE, 
					OTT.je_header_id, 
					OTT.GL_JE_NAME, 
					OTT.REVERSAL_PERIOD, 
					OTT.REVERSAL_STATUS, 
					OTT.EXTERNAL_REFERENCE, 
					OTT.GL_LINE_NUMBER, 
					OTT.effective_date, 
					XLA.LINE_NUMBER, 
					XLA.ORIG_LINE_NUMBER, 
					XLA.ACCOUNTING_CLASS_CODE, 
					XLA.ACCOUNTING_CLASS_NAME, 
					XLA.LINE_DESCRIPTION, 
					XLA.ENTERED_CURRENCY, 
					XLA.CONVERSION_RATE, 
					XLA.CONVERSION_RATE_DATE, 
					XLA.CONVERSION_RATE_TYPE_CODE, 
					XLA.CONVERSION_RATE_TYPE, 
					XLA.ENTERED_DR, 
					XLA.ENTERED_CR, 
					XLA.UNROUNDED_ACCOUNTED_DR, 
					XLA.UNROUNDED_ACCOUNTED_CR, 
					XLA.ACCOUNTED_DR, 
					XLA.ACCOUNTED_CR, 
					XLA.STATISTICAL_AMOUNT, 
					XLA.RECONCILIATION_REFERENCE, 
					XLA.PARTY_TYPE_CODE, 
					NULL PARTY_TYPE, 
					OTT.LEDGER_ID, 
					OTT.LEDGER_SHORT_NAME, 
					OTT.LEDGER_DESCRIPTION, 
					OTT.LEDGER_NAME, 
					OTT.LEDGER_CURRENCY, 
					OTT.PERIOD_YEAR, 
					OTT.PERIOD_NUMBER, 
					OTT.PERIOD_NAME, 
					OTT.PERIOD_START_DATE, 
					OTT.PERIOD_END_DATE, 
					OTT.BALANCE_TYPE_CODE, 
					OTT.BALANCE_TYPE, 
					OTT.BUDGET_NAME, 
					OTT.ENCUMBRANCE_TYPE, 
					OTT.BEGIN_BALANCE_DR, 
					OTT.BEGIN_BALANCE_CR, 
					OTT.PERIOD_NET_DR, 
					OTT.PERIOD_NET_CR, 
					OTT.CODE_COMBINATION_ID, 
					OTT.ACCOUNTING_CODE_COMBINATION, 
					OTT.CODE_COMBINATION_DESCRIPTION, 
					OTT.CONTROL_ACCOUNT_FLAG, 
					OTT.CONTROL_ACCOUNT, 
					OTT.BALANCING_SEGMENT, 
					OTT.NATURAL_ACCOUNT_SEGMENT, 
					OTT.COST_CENTER_SEGMENT, 
					OTT.MANAGEMENT_SEGMENT, 
					OTT.INTERCOMPANY_SEGMENT, 
					OTT.BALANCING_SEGMENT_DESC, 
					OTT.NATURAL_ACCOUNT_DESC, 
					OTT.COST_CENTER_DESC, 
					OTT.MANAGEMENT_SEGMENT_DESC, 
					OTT.INTERCOMPANY_SEGMENT_DESC, 
					OTT.segment1 SEGMENT1, 
					OTT.segment2 SEGMENT2, 
					OTT.segment3 SEGMENT3, 
					OTT.segment4 SEGMENT4, 
					OTT.segment5 SEGMENT5, 
					OTT.segment6 SEGMENT6, 
					OTT.segment7 SEGMENT7, 
					OTT.segment8 SEGMENT8, 
					OTT.segment9 SEGMENT9, 
					OTT.segment10 SEGMENT10, 
					OTT.BEGIN_RUNNING_TOTAL_CR, 
					OTT.BEGIN_RUNNING_TOTAL_DR, 
					OTT.END_RUNNING_TOTAL_CR, 
					OTT.END_RUNNING_TOTAL_DR, 
					OTT.LEGAL_ENTITY_ID, 
					OTT.LEGAL_ENTITY_NAME, 
					OTT.LE_ADDRESS_LINE_1, 
					OTT.LE_ADDRESS_LINE_2, 
					OTT.LE_ADDRESS_LINE_3, 
					OTT.LE_CITY, 
					OTT.LE_REGION_1, 
					OTT.LE_REGION_2, 
					OTT.LE_REGION_3, 
					OTT.LE_POSTAL_CODE, 
					OTT.LE_COUNTRY, 
					OTT.LE_REGISTRATION_NUMBER, 
					OTT.LE_REGISTRATION_EFFECTIVE_FROM, 
					OTT.LE_BR_DAILY_INSCRIPTION_NUMBER, 
					OTT.LE_BR_DAILY_INSCRIPTION_DATE, 
					OTT.LE_BR_DAILY_ENTITY, 
					OTT.LE_BR_DAILY_LOCATION, 
					OTT.LE_BR_DIRECTOR_NUMBER, 
					OTT.LE_BR_ACCOUNTANT_NUMBER, 
					OTT.LE_BR_ACCOUNTANT_NAME, 
					(
						CASE WHEN XLA.event_class_code = 'SALES_ORDER' 
							AND OTT.user_je_source_name = 'Cost Management' 
							AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS' THEN NVL(XLA.order_Number, ' ') ELSE POAP.segment1 || ' - ' || POAP.name END
						) BE_PROJECT_NO, 
						POAP.BE_PROJECT_DESC, 
						(
							CASE WHEN XLA.entity_code = 'TRANSACTIONS' 
								AND OTT.user_je_source_name = 'Receivables' THEN NVL(XLA.REC_address1, '') WHEN XLA.event_class_code = 'ADJUSTMENT' 
									AND OTT.user_je_source_name = 'Receivables' THEN NVL(XLA.ADJ_address1, '') WHEN XLA.event_class_code = 'SALES_ORDER' 
										AND OTT.user_je_source_name = 'Cost Management' 
										AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS' THEN NVL(XLA.address1, ' ') ELSE POAP.task_number || ' ' END
									) BE_TASK_NO, 
									POAP.BE_TASK_DESC, 
									OTT.approver_name, 
									(
										NVL(POAP.PO_NUMBER, ' ')
									) PO_NUMBER, 
									POAP.invoice_num, 
									POAP.INVOICE_ID, 
									POAP.vendor_id, 
									POAP.vendor_site_id, 
									OTT.TRANSLATED_FLAG, 
									XLA.TEMP_LINE_NUM 
									FROM 
									bec_dwh.FACT_GL_XLA_STG XLA, 
									bec_dwh.FACT_GL_JOURNAL_STG OTT, 
									CTE_PO_AP POAP 
									Where 
									1 = 1 
									AND OTT.gl_sl_link_id = XLA.gl_sl_link_id 
									AND OTT.gl_sl_link_table = XLA.gl_sl_link_table 
									AND XLA.balance_type_code = OTT.balance_type_code 
									AND NVL(XLA.budget_version_id,-19999) = NVL(OTT.budget_version_id,-19999) 
									AND NVL(XLA.encumbrance_type_id,-19999)= NVL(OTT.encumbrance_type_id,-19999) 
									AND POAP.invoice_distribution_id (+)= XLA.alloc_to_dist_id_num_1
								) 
								UNION ALL 
								(
									with gjl_gjh as(
										SELECT 
										gjh.default_effective_date, 
										gjh.creation_date, 
										gjh.last_update_date, 
										gjh.reference_date, 
										gjh.posting_acct_seq_value, 
										gjh.close_acct_seq_value, 
										gjh.je_header_id as header_id, 
										gjh.description as HEADER_DESCRIPTION, 
										gjh.je_category, 
										gjh.je_header_id, 
										gjh.NAME, 
										gjh.ACCRUAL_REV_PERIOD_NAME, 
										gjh.ACCRUAL_REV_STATUS, 
										gjh.external_reference, 
										gjh.currency_code, 
										gjh.currency_conversion_rate, 
										gjh.currency_conversion_date, 
										gjh.currency_conversion_type, 
										gjh.actual_flag, 
										gjh.je_from_sla_flag, 
										gjh.budget_version_id, 
										gjh.encumbrance_type_id, 
										gjh.je_batch_id, 
										gjh.posting_acct_seq_version_id, 
										gjh.close_acct_seq_version_id, 
										gjh.je_source, 
										gjl.ledger_id, 
										gjl.code_combination_id, 
										gjl.period_name, 
										gjl.created_by, 
										gjl.je_line_num GL_LINE_NUMBER, 
										gjl.effective_date, 
										gjl.je_line_num LINE_NUMBER, 
										gjl.je_line_num ORIG_LINE_NUMBER, 
										gjl.description as LINE_DESCRIPTION, 
										gjl.entered_dr, 
										gjl.entered_cr, 
										gjl.accounted_dr, 
										gjl.accounted_cr, 
										gjl.stat_amount, 
										gjl.jgzz_recon_ref_11i, 
										gjl.attribute1, 
										gjl.attribute2 
										FROM 
										bec_ods.gl_je_lines   gjl, 
										bec_ods.gl_je_headers   gjh 
										WHERE 
										gjh.je_header_id = gjl.je_header_id 
										and gjl.last_update_date >= to_date(
										'2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS') 
										and gjh.last_update_date >= to_date(
											'2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS'
										) 
										and (
											gjl.kca_seq_date >= (
												select 
												(executebegints - prune_days) 
												from 
												bec_etl_ctrl.batch_dw_info 
												where 
												dw_table_name = 'fact_gl_account_analysis' 
												and batch_name = 'gl'
											) 
											OR gjh.kca_seq_date >= (
												select 
												(executebegints - prune_days) 
												from 
												bec_etl_ctrl.batch_dw_info 
												where 
												dw_table_name = 'fact_gl_account_analysis' 
												and batch_name = 'gl'
											)
											
											or gjl.is_deleted_flg = 'Y'
											or gjh.is_deleted_flg = 'Y'
										) 
										and NVL(gjh.je_from_sla_flag, 'N') = 'N'
									), 
									-------------------------------------------------------------------------------------------------------------  
									gjst as (
										SELECT 
										gjst.user_je_source_name user_je_source_name, 
										gjst.je_source_name je_source_name, 
										gjst.language "language"  
 										FROM 
										bec_ods.gl_je_sources_tl gjst 
										WHERE 
										gjst.language = 'US'

									), 
									-------------------------------------------------------------------------------------------------------------
									gdct as (
										SELECT 
										gdct.user_conversion_type, 
										gdct.conversion_type 
 										FROM 
										bec_ods.gl_daily_conversion_types  gdct
									), 
									-------------------------------------------------------------------------------------------------------------
									gjb_papf as (
										SELECT 
										gjb.je_batch_id, 
										gjb.NAME, 
										gjb.status, 
										gjb.APPROVER_EMPLOYEE_ID, 
										gjb.creation_date, 
										gjb.posted_date, 
										papf.person_id, 
										papf.full_name, 
										papf.effective_start_date, 
										papf.effective_end_date  

										FROM 
										bec_ods.gl_je_batches   gjb, 
										bec_ods.per_all_people_f   papf 
										WHERE 
										gjb.APPROVER_EMPLOYEE_ID = papf.person_id(+) 
										AND gjb.creation_date BETWEEN papf.effective_start_date (+) 
										and NVL(
											papf.effective_end_date (+), 
											GETDATE()+ 1
										) 
										and gjb.status = 'P' 
										and (gjb.kca_seq_date >= (
											select 
											(executebegints - prune_days) 
											from 
											bec_etl_ctrl.batch_dw_info 
											where 
											dw_table_name = 'fact_gl_account_analysis' 
											and batch_name = 'gl'
										) or gjb.is_deleted_flg = 'Y' OR papf.is_deleted_flg = 'Y' )
									) -------------------------------------------------------------------------------------------------------------
									--Main Query Starts Here
									-------------------------------------------------------------------------------------------------------------
									SELECT 
									gjl_gjh.default_effective_date GL_DATE, 
									gjl_gjh.created_by created_by_id, 
									gjl_gjh.CREATION_DATE, 
									gjl_gjh.LAST_UPDATE_DATE, 
									NULL :: TIMESTAMP GL_TRANSFER_DATE, 
									gjl_gjh.REFERENCE_DATE, 
									NULL :: TIMESTAMP COMPLETED_DATE, 
									NULL TRANSACTION_NUMBER, 
									NULL :: TIMESTAMP TRANSACTION_DATE, 
									fsv1.header_name ACCOUNTING_SEQUENCE_NAME, 
									fsv1.version_name ACCOUNTING_SEQUENCE_VERSION, 
									gjl_gjh.posting_acct_seq_value ACCOUNTING_SEQUENCE_NUMBER, 
									fsv2.header_name REPORTING_SEQUENCE_NAME, 
									fsv2.version_name REPORTING_SEQUENCE_VERSION, 
									gjl_gjh.close_acct_seq_value REPORTING_SEQUENCE_NUMBER, 
									NULL DOCUMENT_CATEGORY, 
									NULL DOCUMENT_SEQUENCE_NAME, 
									NULL DOCUMENT_SEQUENCE_NUMBER, 
									NULL APPLICATION_ID, 
									NULL APPLICATION_NAME --1
									, 
									gjl_gjh.HEADER_ID, 
									gjl_gjh.HEADER_DESCRIPTION, 
									NULL FUND_STATUS, 
									gjl_gjh.je_category, 
									gjst.user_je_source_name JE_SOURCE_NAME, 
									NULL EVENT_ID, 
									NULL :: TIMESTAMP EVENT_DATE, 
									NULL EVENT_NUMBER, 
									NULL EVENT_CLASS_CODE, 
									NULL EVENT_CLASS_NAME, 
									NULL EVENT_TYPE_CODE, 
									NULL EVENT_TYPE_NAME, 
									gjb_papf.NAME GL_BATCH_NAME, 
									gjb_papf.POSTED_DATE, 
									gjl_gjh.je_header_id, 
									gjl_gjh.NAME GL_JE_NAME, 
									gjl_gjh.ACCRUAL_REV_PERIOD_NAME REVERSAL_PERIOD, 
									gjl_gjh.ACCRUAL_REV_STATUS REVERSAL_STATUS, 
									gjl_gjh.external_reference EXTERNAL_REFERENCE, 
									gjl_gjh.GL_LINE_NUMBER, 
									gjl_gjh.effective_date, 
									gjl_gjh.LINE_NUMBER, 
									gjl_gjh.ORIG_LINE_NUMBER, 
									NULL ACCOUNTING_CLASS_CODE, 
									NULL ACCOUNTING_CLASS_NAME, 
									gjl_gjh.LINE_DESCRIPTION, 
									gjl_gjh.currency_code ENTERED_CURRENCY, 
									gjl_gjh.currency_conversion_rate CONVERSION_RATE, 
									gjl_gjh.currency_conversion_date CONVERSION_RATE_DATE, 
									gjl_gjh.currency_conversion_type CONVERSION_RATE_TYPE_CODE, 
									gdct.user_conversion_type CONVERSION_RATE_TYPE, 
									gjl_gjh.entered_dr ENTERED_DR, 
									gjl_gjh.entered_cr ENTERED_CR, 
									NULL UNROUNDED_ACCOUNTED_DR, 
									NULL UNROUNDED_ACCOUNTED_CR, 
									gjl_gjh.accounted_dr ACCOUNTED_DR, 
									gjl_gjh.accounted_cr ACCOUNTED_CR, 
									gjl_gjh.stat_amount STATISTICAL_AMOUNT, 
									gjl_gjh.jgzz_recon_ref_11i RECONCILIATION_REFERENCE, 
									NULL PARTY_TYPE_CODE, 
									NULL PARTY_TYPE, 
									glbgt.ledger_id LEDGER_ID, 
									glbgt.ledger_short_name LEDGER_SHORT_NAME, 
									glbgt.ledger_description LEDGER_DESCRIPTION, 
									glbgt.ledger_name LEDGER_NAME, 
									glbgt.ledger_currency LEDGER_CURRENCY, 
									glbgt.period_year PERIOD_YEAR, 
									glbgt.period_number PERIOD_NUMBER, 
									glbgt.period_name PERIOD_NAME, 
									glbgt.period_start_date, 
									glbgt.period_end_date, 
									glbgt.balance_type_code BALANCE_TYPE_CODE, 
									glbgt.balance_type BALANCE_TYPE, 
									glbgt.budget_name BUDGET_NAME, 
									glbgt.encumbrance_type ENCUMBRANCE_TYPE, 
									glbgt.begin_balance_dr BEGIN_BALANCE_DR, 
									glbgt.begin_balance_cr BEGIN_BALANCE_CR, 
									glbgt.period_net_dr PERIOD_NET_DR, 
									glbgt.period_net_cr PERIOD_NET_CR, 
									glbgt.code_combination_id CODE_COMBINATION_ID, 
									'NA' :: VARCHAR(2) ACCOUNTING_CODE_COMBINATION, 
									'NA' :: VARCHAR(2) CODE_COMBINATION_DESCRIPTION, 
									glbgt.control_account_flag CONTROL_ACCOUNT_FLAG, 
									glbgt.control_account CONTROL_ACCOUNT, 
									'NA' :: VARCHAR(2) BALANCING_SEGMENT, 
									'NA' :: VARCHAR(2) NATURAL_ACCOUNT_SEGMENT, 
									'NA' :: VARCHAR(2) COST_CENTER_SEGMENT, 
									'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT, 
									'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT, 
									'NA' :: VARCHAR(2) BALANCING_SEGMENT_DESC, 
									'NA' :: VARCHAR(2) NATURAL_ACCOUNT_DESC, 
									'NA' :: VARCHAR(2) COST_CENTER_DESC, 
									'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT_DESC, 
									'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT_DESC, 
									glbgt.segment1 SEGMENT1, 
									glbgt.segment2 SEGMENT2, 
									glbgt.segment3 SEGMENT3, 
									glbgt.segment4 SEGMENT4, 
									glbgt.segment5 SEGMENT5, 
									glbgt.segment6 SEGMENT6, 
									glbgt.segment7 SEGMENT7, 
									glbgt.segment8 SEGMENT8, 
									glbgt.segment9 SEGMENT9, 
									glbgt.segment10 SEGMENT10, 
									'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_CR, 
									'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_DR, 
									'NA' :: VARCHAR(2) END_RUNNING_TOTAL_CR, 
									'NA' :: VARCHAR(2) END_RUNNING_TOTAL_DR, 
									'NA' :: VARCHAR(2) LEGAL_ENTITY_ID, 
									'NA' :: VARCHAR(2) LEGAL_ENTITY_NAME, 
									'NA' :: VARCHAR(2) LE_ADDRESS_LINE_1, 
									'NA' :: VARCHAR(2) LE_ADDRESS_LINE_2, 
									'NA' :: VARCHAR(2) LE_ADDRESS_LINE_3, 
									'NA' :: VARCHAR(2) LE_CITY, 
									'NA' :: VARCHAR(2) LE_REGION_1, 
									'NA' :: VARCHAR(2) LE_REGION_2, 
									'NA' :: VARCHAR(2) LE_REGION_3, 
									'NA' :: VARCHAR(2) LE_POSTAL_CODE, 
									'NA' :: VARCHAR(2) LE_COUNTRY, 
									'NA' :: VARCHAR(2) LE_REGISTRATION_NUMBER, 
									'NA' :: VARCHAR(2) LE_REGISTRATION_EFFECTIVE_FROM, 
									'NA' :: VARCHAR(2) LE_BR_DAILY_INSCRIPTION_NUMBER, 
									NULL :: TIMESTAMP LE_BR_DAILY_INSCRIPTION_DATE, 
									'NA' :: VARCHAR(2) LE_BR_DAILY_ENTITY, 
									'NA' :: VARCHAR(2) LE_BR_DAILY_LOCATION, 
									'NA' :: VARCHAR(2) LE_BR_DIRECTOR_NUMBER, 
									'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NUMBE, 
									'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NAME, 
									gjl_gjh.attribute1 BE_PROJECT_NO, 
									NULL BE_PROJECT_DESC, 
									gjl_gjh.attribute2 BE_TASK_NO, 
									null BE_TASK_DESC, 
									gjb_papf.full_name approver_name, 
									NULL PO_NUMBER, 
									NULL invoice_num, 
									NULL INVOICE_ID, 
									NULL vendor_id, 
									NULL vendor_site_id, 
									glbgt.TRANSLATED_FLAG, 
									0 TEMP_LINE_NUM 
									FROM 
									bec_dwh.FACT_XLA_REPORT_BALANCES_STG glbgt, 
									gjl_gjh, 
									bec_ods.fun_seq_versions   fsv1, 
									bec_ods.fun_seq_versions   fsv2, 
									gjst, 
									gdct, 
									gjb_papf 
									WHERE 
									gjl_gjh.ledger_id = glbgt.ledger_id 
									AND gjl_gjh.code_combination_id = glbgt.code_combination_id 
									AND (
										gjl_gjh.effective_date BETWEEN glbgt.period_start_date 
										AND glbgt.period_end_date
									) 
									AND gjl_gjh.period_name = glbgt.period_name 
									AND gjl_gjh.actual_flag = glbgt.balance_type_code 
									AND decode(
										gjl_gjh.currency_code, 'STAT', gjl_gjh.currency_code, 
										glbgt.ledger_currency
									) = glbgt.ledger_currency 
									AND NVL(
									gjl_gjh.budget_version_id,-19999
									)= NVL(glbgt.budget_version_id,-19999) 
									AND NVL(
									gjl_gjh.encumbrance_type_id,-19999
									)= NVL(
										glbgt.encumbrance_type_id,-19999
									) 
									AND gjb_papf.je_batch_id = gjl_gjh.je_batch_id --AND   fdu.user_id                      = gjl.created_by --change
									AND fsv1.seq_version_id(+) = gjl_gjh.posting_acct_seq_version_id 
									AND fsv2.seq_version_id(+) = gjl_gjh.close_acct_seq_version_id --       AND   --gjct.je_category_name            = gjh.je_category
									AND gjst.je_source_name = gjl_gjh.je_source 
									AND gdct.conversion_type(+) = gjl_gjh.currency_conversion_type 
									 
									
								) 
								UNION ALL 
								(
									SELECT 
									gjh.default_effective_date as GL_DATE, 
									gjl.created_by created_by_id, 
									gjh.creation_date as CREATION_DATE, 
									gjh.last_update_date as LAST_UPDATE_DATE, 
									NULL :: TIMESTAMP as GL_TRANSFER_DATE, 
									gjh.reference_date as REFERENCE_DATE, 
									NULL :: TIMESTAMP as COMPLETED_DATE, 
									NULL TRANSACTION_NUMBER, 
									NULL :: TIMESTAMP as TRANSACTION_DATE, 
									fsv1.header_name ACCOUNTING_SEQUENCE_NAME, 
									fsv1.version_name ACCOUNTING_SEQUENCE_VERSION, 
									gjh.posting_acct_seq_value ACCOUNTING_SEQUENCE_NUMBER, 
									fsv2.header_name REPORTING_SEQUENCE_NAME, 
									fsv2.version_name REPORTING_SEQUENCE_VERSION, 
									gjh.close_acct_seq_value REPORTING_SEQUENCE_NUMBER, 
									NULL DOCUMENT_CATEGORY, 
									NULL DOCUMENT_SEQUENCE_NAME, 
									NULL DOCUMENT_SEQUENCE_NUMBER, 
									NULL APPLICATION_ID, 
									NULL APPLICATION_NAME, 
									NULL HEADER_ID, 
									gjh.description HEADER_DESCRIPTION, 
									NULL FUND_STATUS, 
									gjh.je_category, 
									gjst.user_je_source_name JE_SOURCE_NAME, 
									NULL EVENT_ID, 
									NULL :: TIMESTAMP as EVENT_DATE, 
									NULL EVENT_NUMBER, 
									NULL EVENT_CLASS_CODE, 
									NULL EVENT_CLASS_NAME, 
									NULL EVENT_TYPE_CODE, 
									NULL EVENT_TYPE_NAME, 
									gjb.NAME GL_BATCH_NAME, 
									gjb.posted_date, 
									gjh.je_header_id, 
									gjh.NAME GL_JE_NAME, 
									gjh.ACCRUAL_REV_PERIOD_NAME REVERSAL_PERIOD, 
									gjh.ACCRUAL_REV_STATUS REVERSAL_STATUS, 
									gjh.external_reference EXTERNAL_REFERENCE, 
									gjl.je_line_num GL_LINE_NUMBER, 
									gjl.effective_date, 
									gjl.je_line_num LINE_NUMBER, 
									gjl.je_line_num ORIG_LINE_NUMBER, 
									NULL ACCOUNTING_CLASS_CODE, 
									NULL ACCOUNTING_CLASS_NAME, 
									gjl.description LINE_DESCRIPTION, 
									gjh.currency_code ENTERED_CURRENCY, 
									gjh.currency_conversion_rate CONVERSION_RATE, 
									gjh.currency_conversion_date as CONVERSION_RATE_DATE, 
									gjh.currency_conversion_type CONVERSION_RATE_TYPE_CODE, 
									gdct.user_conversion_type CONVERSION_RATE_TYPE, 
									gjl.entered_dr ENTERED_DR, 
									gjl.entered_cr ENTERED_CR, 
									NULL UNROUNDED_ACCOUNTED_DR, 
									NULL UNROUNDED_ACCOUNTED_CR, 
									gjl.accounted_dr ACCOUNTED_DR, 
									gjl.accounted_cr ACCOUNTED_CR, 
									gjl.stat_amount STATISTICAL_AMOUNT, 
									gjl.jgzz_recon_ref_11i RECONCILIATION_REFERENCE, 
									NULL PARTY_TYPE_CODE, 
									NULL PARTY_TYPE, 
									glbgt.ledger_id LEDGER_ID, 
									glbgt.ledger_short_name LEDGER_SHORT_NAME, 
									glbgt.ledger_description LEDGER_DESCRIPTION, 
									glbgt.ledger_name LEDGER_NAME, 
									glbgt.ledger_currency LEDGER_CURRENCY, 
									glbgt.period_year PERIOD_YEAR, 
									glbgt.period_number PERIOD_NUMBER, 
									glbgt.period_name PERIOD_NAME, 
									glbgt.period_start_date, 
									glbgt.period_end_date, 
									glbgt.balance_type_code BALANCE_TYPE_CODE, 
									glbgt.balance_type BALANCE_TYPE, 
									glbgt.budget_name BUDGET_NAME, 
									glbgt.encumbrance_type ENCUMBRANCE_TYPE, 
									glbgt.begin_balance_dr BEGIN_BALANCE_DR, 
									glbgt.begin_balance_cr BEGIN_BALANCE_CR, 
									glbgt.period_net_dr PERIOD_NET_DR, 
									glbgt.period_net_cr PERIOD_NET_CR, 
									glbgt.code_combination_id CODE_COMBINATION_ID, 
									'NA' :: VARCHAR(2) ACCOUNTING_CODE_COMBINATION, 
									'NA' :: VARCHAR(2) CODE_COMBINATION_DESCRIPTION, 
									glbgt.control_account_flag CONTROL_ACCOUNT_FLAG, 
									glbgt.control_account CONTROL_ACCOUNT, 
									'NA' :: VARCHAR(2) BALANCING_SEGMENT, 
									'NA' :: VARCHAR(2) NATURAL_ACCOUNT_SEGMENT, 
									'NA' :: VARCHAR(2) COST_CENTER_SEGMENT, 
									'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT, 
									'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT, 
									'NA' :: VARCHAR(2) BALANCING_SEGMENT_DESC, 
									'NA' :: VARCHAR(2) NATURAL_ACCOUNT_DESC, 
									'NA' :: VARCHAR(2) COST_CENTER_DESC, 
									'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT_DESC, 
									'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT_DESC, 
									glbgt.segment1 SEGMENT1, 
									glbgt.segment2 SEGMENT2, 
									glbgt.segment3 SEGMENT3, 
									glbgt.segment4 SEGMENT4, 
									glbgt.segment5 SEGMENT5, 
									glbgt.segment6 SEGMENT6, 
									glbgt.segment7 SEGMENT7, 
									glbgt.segment8 SEGMENT8, 
									glbgt.segment9 SEGMENT9, 
									glbgt.segment10 SEGMENT10, 
									'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_CR, 
									'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_DR, 
									'NA' :: VARCHAR(2) END_RUNNING_TOTAL_CR, 
									'NA' :: VARCHAR(2) END_RUNNING_TOTAL_DR, 
									'NA' :: VARCHAR(2) LEGAL_ENTITY_ID, 
									'NA' :: VARCHAR(2) LEGAL_ENTITY_NAME, 
									'NA' :: VARCHAR(2) LE_ADDRESS_LINE_1, 
									'NA' :: VARCHAR(2) LE_ADDRESS_LINE_2, 
									'NA' :: VARCHAR(2) LE_ADDRESS_LINE_3, 
									'NA' :: VARCHAR(2) LE_CITY, 
									'NA' :: VARCHAR(2) LE_REGION_1, 
									'NA' :: VARCHAR(2) LE_REGION_2, 
									'NA' :: VARCHAR(2) LE_REGION_3, 
									'NA' :: VARCHAR(2) LE_POSTAL_CODE, 
									'NA' :: VARCHAR(2) LE_COUNTRY, 
									'NA' :: VARCHAR(2) LE_REGISTRATION_NUMBER, 
									'NA' :: VARCHAR(2) LE_REGISTRATION_EFFECTIVE_FROM, 
									'NA' :: VARCHAR(2) LE_BR_DAILY_INSCRIPTION_NUMBER, 
									NULL :: TIMESTAMP LE_BR_DAILY_INSCRIPTION_DATE, 
									'NA' :: VARCHAR(2) LE_BR_DAILY_ENTITY, 
									'NA' :: VARCHAR(2) LE_BR_DAILY_LOCATION, 
									'NA' :: VARCHAR(2) LE_BR_DIRECTOR_NUMBER, 
									'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NUMBER, 
									'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NAME, 
									gjl.attribute1 BE_PROJECT_NO, 
									NULL BE_PROJECT_DESC, 
									gjl.attribute2 BE_TASK_NO, 
									NULL BE_TASK_DESC, 
									PAPF.FULL_NAME APPROVER_NAME, 
									NULL PO_NUMBER, 
									NULL invoice_num, 
									NULL INVOICE_ID, 
									NULL vendor_id, 
									NULL vendor_site_id, 
									glbgt.TRANSLATED_FLAG, 
									0 TEMP_LINE_NUM 
									FROM 
									bec_dwh.FACT_XLA_REPORT_BALANCES_STG glbgt, 
									bec_ods.fnd_new_messages   fnm, 
									bec_ods.fun_seq_versions   fsv1, 
									bec_ods.fun_seq_versions  fsv2, 
									bec_ods.gl_je_sources_tl   gjst, 
									bec_ods.gl_daily_conversion_types   gdct, 
									bec_ods.gl_je_lines   gjl, 
									bec_ods.gl_je_headers   gjh, 
									bec_ods.gl_je_batches   gjb, 
									bec_ods.PER_ALL_PEOPLE_F   PAPF 
									WHERE 
									gjl.ledger_id = glbgt.ledger_id 
									AND gjl.code_combination_id = glbgt.code_combination_id 
									AND gjl.effective_date BETWEEN glbgt.period_start_date 
									AND glbgt.period_end_date 
									AND gjl.period_name = glbgt.period_name 
									AND gjh.je_header_id = gjl.je_header_id 
									AND gjh.actual_flag = glbgt.balance_type_code 
									AND decode(
										gjh.currency_code, 'STAT', gjh.currency_code, 
										glbgt.ledger_currency
									) = glbgt.ledger_currency --added bug 6686541
									AND NVL(gjh.je_from_sla_flag, 'N') = 'U' 
									AND fnm.application_id = 101 
									AND fnm.language_code = 'US' 
									AND fnm.message_name in (
										'PPOS0220', 'PPOS0221', 'PPOS0222', 
										'PPOS0243', 'PPOS0222_G', 'PPOSO275'
									) 
									AND gjl.description = fnm.message_text 
									AND NVL(gjh.budget_version_id,-19999) = NVL(glbgt.budget_version_id,-19999) 
									AND NVL(gjh.encumbrance_type_id,-19999) = NVL(
										glbgt.encumbrance_type_id,-19999
									) 
									AND gjb.je_batch_id = gjh.je_batch_id 
									AND gjb.status = 'P' 
									AND GJB.APPROVER_EMPLOYEE_ID = PAPF.PERSON_ID(+) 
									AND gjb.creation_date BETWEEN papf.effective_start_date (+) 
									and NVL(
										papf.effective_end_date (+), 
										SYSDATE + 1
									) --       AND   fdu.user_id                      = gjl.created_by  --change
									AND fsv1.seq_version_id(+) = gjh.posting_acct_seq_version_id 
									AND fsv2.seq_version_id(+) = gjh.close_acct_seq_version_id --AND   gjct.je_category_name            = gjh.je_category
									AND gjst.je_source_name = gjh.je_source 
									AND gjst.language = 'US' 
									AND gdct.conversion_type(+) = gjh.currency_conversion_type 
									AND not exists (
										select 
										'x' 
										from 
										bec_ods.gl_import_references   gir 
										where 
										gir.je_header_id = gjl.je_header_id 
										and gir.je_line_num = gjl.je_line_num 
									)  
									AND (
										gjl.kca_seq_date >= (
											select 
											(executebegints - prune_days) 
											from 
											bec_etl_ctrl.batch_dw_info 
											where 
											dw_table_name = 'fact_gl_account_analysis' 
											and batch_name = 'gl'
										) 
										OR gjh.kca_seq_date >= (
											select 
											(executebegints - prune_days) 
											from 
											bec_etl_ctrl.batch_dw_info 
											where 
											dw_table_name = 'fact_gl_account_analysis' 
											and batch_name = 'gl'
										) 
										OR gjb.kca_seq_date >= (
											select 
											(executebegints - prune_days) 
											from 
											bec_etl_ctrl.batch_dw_info 
											where 
											dw_table_name = 'fact_gl_account_analysis' 
											and batch_name = 'gl'
										)
										
										or fnm.is_deleted_flg = 'Y'
										or fsv1.is_deleted_flg = 'Y'
										or fsv2.is_deleted_flg = 'Y'
										or gjst.is_deleted_flg = 'Y'
										or gdct.is_deleted_flg = 'Y'
										or gjb.is_deleted_flg = 'Y'
										or gjl.is_deleted_flg = 'Y'
										or gjh.is_deleted_flg = 'Y'
										or gjb.is_deleted_flg = 'Y'
										OR PAPF.is_deleted_flg = 'Y' 
									)
								)
							) A
						) ods 
						where 
						dw.dw_load_id = (
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
						source_system = 'EBS'
						) || '-' || nvl(ods.LINE_NUMBER, 0) || '-' || nvl(ods.HEADER_ID, 0) || '-' || nvl(ods.LEDGER_CURRENCY, 'NA') || '-' || nvl(ods.TRANSLATED_FLAG, 'NA') || '-' || nvl(ods.TEMP_LINE_NUM, 0)
					);
					commit;
					-- Insert records
					insert into bec_dwh.FACT_GL_ACCOUNT_ANALYSIS (
						GL_DATE, 
						created_by_id, 
						creation_date, 
						last_update_date, 
						GL_TRANSFER_DATE, 
						reference_date, 
						COMPLETED_DATE, 
						TRANSACTION_NUMBER, 
						TRANSACTION_DATE, 
						ACCOUNTING_SEQUENCE_NAME, 
						ACCOUNTING_SEQUENCE_VERSION, 
						ACCOUNTING_SEQUENCE_NUMBER, 
						REPORTING_SEQUENCE_NAME, 
						REPORTING_SEQUENCE_VERSION, 
						REPORTING_SEQUENCE_NUMBER, 
						DOCUMENT_CATEGORY, 
						DOCUMENT_SEQUENCE_NAME, 
						DOCUMENT_SEQUENCE_NUMBER, 
						APPLICATION_ID, 
						APPLICATION_NAME, 
						HEADER_ID, 
						HEADER_DESCRIPTION, 
						FUND_STATUS, 
						je_category, 
						JE_SOURCE_NAME, 
						EVENT_ID, 
						EVENT_DATE, 
						EVENT_NUMBER, 
						EVENT_CLASS_CODE, 
						EVENT_CLASS_NAME, 
						EVENT_TYPE_CODE, 
						EVENT_TYPE_NAME, 
						GL_BATCH_NAME, 
						POSTED_DATE, 
						je_header_id, 
						GL_JE_NAME, 
						REVERSAL_PERIOD, 
						REVERSAL_STATUS, 
						EXTERNAL_REFERENCE, 
						GL_LINE_NUMBER, 
						effective_date, 
						LINE_NUMBER, 
						ORIG_LINE_NUMBER, 
						ACCOUNTING_CLASS_CODE, 
						ACCOUNTING_CLASS_NAME, 
						LINE_DESCRIPTION, 
						ENTERED_CURRENCY, 
						CONVERSION_RATE, 
						CONVERSION_RATE_DATE, 
						CONVERSION_RATE_TYPE_CODE, 
						CONVERSION_RATE_TYPE, 
						ENTERED_DR, 
						ENTERED_CR, 
						UNROUNDED_ACCOUNTED_DR, 
						UNROUNDED_ACCOUNTED_CR, 
						ACCOUNTED_DR, 
						ACCOUNTED_CR, 
						STATISTICAL_AMOUNT, 
						RECONCILIATION_REFERENCE, 
						PARTY_TYPE_CODE, 
						PARTY_TYPE, 
						LEDGER_ID, 
						LEDGER_SHORT_NAME, 
						LEDGER_DESCRIPTION, 
						LEDGER_NAME, 
						LEDGER_CURRENCY, 
						PERIOD_YEAR, 
						PERIOD_NUMBER, 
						PERIOD_NAME, 
						PERIOD_START_DATE, 
						PERIOD_END_DATE, 
						BALANCE_TYPE_CODE, 
						BALANCE_TYPE, 
						BUDGET_NAME, 
						ENCUMBRANCE_TYPE, 
						BEGIN_BALANCE_DR, 
						BEGIN_BALANCE_CR, 
						PERIOD_NET_DR, 
						PERIOD_NET_CR, 
						CODE_COMBINATION_ID, 
						ACCOUNTING_CODE_COMBINATION, 
						CODE_COMBINATION_DESCRIPTION, 
						CONTROL_ACCOUNT_FLAG, 
						CONTROL_ACCOUNT, 
						BALANCING_SEGMENT, 
						NATURAL_ACCOUNT_SEGMENT, 
						COST_CENTER_SEGMENT, 
						MANAGEMENT_SEGMENT, 
						INTERCOMPANY_SEGMENT, 
						BALANCING_SEGMENT_DESC, 
						NATURAL_ACCOUNT_DESC, 
						COST_CENTER_DESC, 
						MANAGEMENT_SEGMENT_DESC, 
						INTERCOMPANY_SEGMENT_DESC, 
						segment1, 
						segment2, 
						segment3, 
						segment4, 
						segment5, 
						segment6, 
						segment7, 
						segment8, 
						segment9, 
						segment10, 
						BEGIN_RUNNING_TOTAL_CR, 
						BEGIN_RUNNING_TOTAL_DR, 
						END_RUNNING_TOTAL_CR, 
						END_RUNNING_TOTAL_DR, 
						LEGAL_ENTITY_ID, 
						LEGAL_ENTITY_NAME, 
						LE_ADDRESS_LINE_1, 
						LE_ADDRESS_LINE_2, 
						LE_ADDRESS_LINE_3, 
						LE_CITY, 
						LE_REGION_1, 
						LE_REGION_2, 
						LE_REGION_3, 
						LE_POSTAL_CODE, 
						LE_COUNTRY, 
						LE_REGISTRATION_NUMBER, 
						LE_REGISTRATION_EFFECTIVE_FROM, 
						LE_BR_DAILY_INSCRIPTION_NUMBER, 
						LE_BR_DAILY_INSCRIPTION_DATE, 
						LE_BR_DAILY_ENTITY, 
						LE_BR_DAILY_LOCATION, 
						LE_BR_DIRECTOR_NUMBER, 
						LE_BR_ACCOUNTANT_NUMBER, 
						LE_BR_ACCOUNTANT_NAME, 
						BE_PROJECT_NO, 
						BE_PROJECT_DESC, 
						BE_TASK_NO, 
						BE_TASK_DESC, 
						approver_name, 
						PO_NUMBER, 
						invoice_num, 
						INVOICE_ID, 
						vendor_id, 
						vendor_site_id, 
						TRANSLATED_FLAG, 
						TEMP_LINE_NUM, 
						je_category_KEY, 
						je_header_id_KEY, 
						LEDGER_ID_KEY, 
						CODE_COMBINATION_ID_KEY, 
						LEGAL_ENTITY_ID_KEY, 
						INVOICE_ID_KEY, 
						vendor_id_KEY,
						Update_flg,						
						is_deleted_flg, 
						source_app_id, 
						dw_load_id, 
						dw_insert_date, 
					dw_update_date
					) (
						SELECT distinct
						A.GL_DATE, 
						A.created_by_id, 
						A.creation_date, 
						A.last_update_date, 
						A.GL_TRANSFER_DATE, 
						A.reference_date, 
						A.COMPLETED_DATE, 
						A.TRANSACTION_NUMBER, 
						A.TRANSACTION_DATE, 
						A.ACCOUNTING_SEQUENCE_NAME, 
						A.ACCOUNTING_SEQUENCE_VERSION, 
						A.ACCOUNTING_SEQUENCE_NUMBER, 
						A.REPORTING_SEQUENCE_NAME, 
						A.REPORTING_SEQUENCE_VERSION, 
						A.REPORTING_SEQUENCE_NUMBER, 
						A.DOCUMENT_CATEGORY, 
						A.DOCUMENT_SEQUENCE_NAME, 
						A.DOCUMENT_SEQUENCE_NUMBER, 
						A.APPLICATION_ID, 
						A.APPLICATION_NAME, 
						A.HEADER_ID, 
						A.HEADER_DESCRIPTION, 
						A.FUND_STATUS, 
						A.je_category, 
						A.JE_SOURCE_NAME, 
						A.EVENT_ID, 
						A.EVENT_DATE, 
						A.EVENT_NUMBER, 
						A.EVENT_CLASS_CODE, 
						A.EVENT_CLASS_NAME, 
						A.EVENT_TYPE_CODE, 
						A.EVENT_TYPE_NAME, 
						A.GL_BATCH_NAME, 
						A.POSTED_DATE, 
						A.je_header_id, 
						A.GL_JE_NAME, 
						A.REVERSAL_PERIOD, 
						A.REVERSAL_STATUS, 
						A.EXTERNAL_REFERENCE, 
						A.GL_LINE_NUMBER, 
						A.effective_date, 
						A.LINE_NUMBER, 
						A.ORIG_LINE_NUMBER, 
						A.ACCOUNTING_CLASS_CODE, 
						A.ACCOUNTING_CLASS_NAME, 
						A.LINE_DESCRIPTION, 
						A.ENTERED_CURRENCY, 
						A.CONVERSION_RATE, 
						A.CONVERSION_RATE_DATE, 
						A.CONVERSION_RATE_TYPE_CODE, 
						A.CONVERSION_RATE_TYPE, 
						A.ENTERED_DR, 
						A.ENTERED_CR, 
						A.UNROUNDED_ACCOUNTED_DR, 
						A.UNROUNDED_ACCOUNTED_CR, 
						A.ACCOUNTED_DR, 
						A.ACCOUNTED_CR, 
						A.STATISTICAL_AMOUNT, 
						A.RECONCILIATION_REFERENCE, 
						A.PARTY_TYPE_CODE, 
						A.PARTY_TYPE, 
						A.LEDGER_ID, 
						A.LEDGER_SHORT_NAME, 
						A.LEDGER_DESCRIPTION, 
						A.LEDGER_NAME, 
						A.LEDGER_CURRENCY, 
						A.PERIOD_YEAR, 
						A.PERIOD_NUMBER, 
						A.PERIOD_NAME, 
						A.PERIOD_START_DATE, 
						A.PERIOD_END_DATE, 
						A.BALANCE_TYPE_CODE, 
						A.BALANCE_TYPE, 
						A.BUDGET_NAME, 
						A.ENCUMBRANCE_TYPE, 
						A.BEGIN_BALANCE_DR, 
						A.BEGIN_BALANCE_CR, 
						A.PERIOD_NET_DR, 
						A.PERIOD_NET_CR, 
						A.CODE_COMBINATION_ID, 
						A.ACCOUNTING_CODE_COMBINATION, 
						A.CODE_COMBINATION_DESCRIPTION, 
						A.CONTROL_ACCOUNT_FLAG, 
						A.CONTROL_ACCOUNT, 
						A.BALANCING_SEGMENT, 
						A.NATURAL_ACCOUNT_SEGMENT, 
						A.COST_CENTER_SEGMENT, 
						A.MANAGEMENT_SEGMENT, 
						A.INTERCOMPANY_SEGMENT, 
						A.BALANCING_SEGMENT_DESC, 
						A.NATURAL_ACCOUNT_DESC, 
						A.COST_CENTER_DESC, 
						A.MANAGEMENT_SEGMENT_DESC, 
						A.INTERCOMPANY_SEGMENT_DESC, 
						A.segment1, 
						A.segment2, 
						A.segment3, 
						A.segment4, 
						A.segment5, 
						A.segment6, 
						A.segment7, 
						A.segment8, 
						A.segment9, 
						A.segment10, 
						A.BEGIN_RUNNING_TOTAL_CR, 
						A.BEGIN_RUNNING_TOTAL_DR, 
						A.END_RUNNING_TOTAL_CR, 
						A.END_RUNNING_TOTAL_DR, 
						A.LEGAL_ENTITY_ID, 
						A.LEGAL_ENTITY_NAME, 
						A.LE_ADDRESS_LINE_1, 
						A.LE_ADDRESS_LINE_2, 
						A.LE_ADDRESS_LINE_3, 
						A.LE_CITY, 
						A.LE_REGION_1, 
						A.LE_REGION_2, 
						A.LE_REGION_3, 
						A.LE_POSTAL_CODE, 
						A.LE_COUNTRY, 
						A.LE_REGISTRATION_NUMBER, 
						A.LE_REGISTRATION_EFFECTIVE_FROM, 
						A.LE_BR_DAILY_INSCRIPTION_NUMBER, 
						A.LE_BR_DAILY_INSCRIPTION_DATE, 
						A.LE_BR_DAILY_ENTITY, 
						A.LE_BR_DAILY_LOCATION, 
						A.LE_BR_DIRECTOR_NUMBER, 
						A.LE_BR_ACCOUNTANT_NUMBER, 
						A.LE_BR_ACCOUNTANT_NAME, 
						A.BE_PROJECT_NO, 
						A.BE_PROJECT_DESC, 
						A.BE_TASK_NO, 
						A.BE_TASK_DESC, 
						A.approver_name, 
						A.PO_NUMBER, 
						A.invoice_num, 
						A.INVOICE_ID, 
						A.vendor_id, 
						A.vendor_site_id, 
						A.TRANSLATED_FLAG, 
						A.TEMP_LINE_NUM, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.je_category as je_category_KEY, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.je_header_id as je_header_id_KEY, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.LEDGER_ID as LEDGER_ID_KEY, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.LEGAL_ENTITY_ID as LEGAL_ENTITY_ID_KEY, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.INVOICE_ID as INVOICE_ID_KEY, 
						(
							select 
							system_id 
							from 
							bec_etl_ctrl.etlsourceappid 
							where 
							source_system = 'EBS'
						)|| '-' || A.vendor_id as vendor_id_KEY -- audit columns
						, 
						'Y' as Update_flg,
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
						) || '-' || nvl(A.LINE_NUMBER, 0) || '-' || nvl(A.HEADER_ID, 0) || '-' || nvl(A.LEDGER_CURRENCY, 'NA') || '-' || nvl(A.TRANSLATED_FLAG, 'NA') || '-' || nvl(A.TEMP_LINE_NUM, 0) as dw_load_id, 
						getdate() as dw_insert_date, 
						getdate() as dw_update_date 
						from 
						(
							(
								with CTE_PO_AP as (
									SELECT 
									pa1.segment1, 
									pa1.name, 
									pa1.description BE_PROJECT_DESC, 
									pt1.task_number, 
									pt1.description BE_TASK_DESC, 
									PHA.SEGMENT1 as PO_NUMBER, 
									AIA.invoice_num, 
									AIA.INVOICE_ID, 
									AIA.vendor_id, 
									AIA.vendor_site_id, 
									apd1.invoice_distribution_id 
									From 
									(select * from bec_ods.ap_invoice_distributions_all where is_deleted_flg <> 'Y') apd1, 
									(select * from bec_ods.po_distributions_all where is_deleted_flg <> 'Y') pda, 
									(select * from bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') AIA, 
									(select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') pha, 
									(select * from bec_ods.pa_projects_all where is_deleted_flg <> 'Y') pa1, 
									(select * from bec_ods.pa_tasks where is_deleted_flg <> 'Y') pt1 
									WHERE 
									1 = 1 
									AND pt1.task_id (+) = apd1.task_id 
									AND apd1.INVOICE_ID = AIA.INVOICE_ID 
									AND pa1.project_id (+)= apd1.project_id 
									AND PDA.PO_DISTRIBUTION_ID (+) = apd1.PO_DISTRIBUTION_ID 
									AND PHA.PO_HEADER_ID(+) = PDA.PO_HEADER_ID 
									AND (
										apd1.kca_seq_date >= (
											select 
											(executebegints - prune_days) 
											from 
											bec_etl_ctrl.batch_dw_info 
											where 
											dw_table_name = 'fact_gl_account_analysis' 
											and batch_name = 'gl'
										) 
										OR AIA.kca_seq_date >= (
											select 
											(executebegints - prune_days) 
											from 
											bec_etl_ctrl.batch_dw_info 
											where 
											dw_table_name = 'fact_gl_account_analysis' 
											and batch_name = 'gl'
										)
									)
								) 
								SELECT 
								XLA.GL_DATE, 
								OTT.created_by_id, 
								XLA.creation_date, 
								XLA.last_update_date, 
								XLA.GL_TRANSFER_DATE, 
								XLA.reference_date, 
								XLA.COMPLETED_DATE, 
								XLA.TRANSACTION_NUMBER, 
								XLA.TRANSACTION_DATE, 
								XLA.ACCOUNTING_SEQUENCE_NAME, 
								XLA.ACCOUNTING_SEQUENCE_VERSION, 
								XLA.ACCOUNTING_SEQUENCE_NUMBER, 
								XLA.REPORTING_SEQUENCE_NAME, 
								XLA.REPORTING_SEQUENCE_VERSION, 
								XLA.REPORTING_SEQUENCE_NUMBER, 
								NULL DOCUMENT_CATEGORY, 
								XLA.DOCUMENT_SEQUENCE_NAME, 
								XLA.DOCUMENT_SEQUENCE_NUMBER, 
								XLA.APPLICATION_ID, 
								NULL APPLICATION_NAME, 
								XLA.HEADER_ID, 
								XLA.HEADER_DESCRIPTION, 
								XLA.FUND_STATUS, 
								XLA.je_category_name as je_category, 
								OTT.JE_SOURCE_NAME, 
								XLA.EVENT_ID, 
								XLA.EVENT_DATE, 
								XLA.EVENT_NUMBER, 
								XLA.EVENT_CLASS_CODE, 
								XLA.EVENT_CLASS_NAME, 
								XLA.EVENT_TYPE_CODE, 
								XLA.EVENT_TYPE_NAME, 
								OTT.GL_BATCH_NAME, 
								OTT.POSTED_DATE, 
								OTT.je_header_id, 
								OTT.GL_JE_NAME, 
								OTT.REVERSAL_PERIOD, 
								OTT.REVERSAL_STATUS, 
								OTT.EXTERNAL_REFERENCE, 
								OTT.GL_LINE_NUMBER, 
								OTT.effective_date, 
								XLA.LINE_NUMBER, 
								XLA.ORIG_LINE_NUMBER, 
								XLA.ACCOUNTING_CLASS_CODE, 
								XLA.ACCOUNTING_CLASS_NAME, 
								XLA.LINE_DESCRIPTION, 
								XLA.ENTERED_CURRENCY, 
								XLA.CONVERSION_RATE, 
								XLA.CONVERSION_RATE_DATE, 
								XLA.CONVERSION_RATE_TYPE_CODE, 
								XLA.CONVERSION_RATE_TYPE, 
								XLA.ENTERED_DR, 
								XLA.ENTERED_CR, 
								XLA.UNROUNDED_ACCOUNTED_DR, 
								XLA.UNROUNDED_ACCOUNTED_CR, 
								XLA.ACCOUNTED_DR, 
								XLA.ACCOUNTED_CR, 
								XLA.STATISTICAL_AMOUNT, 
								XLA.RECONCILIATION_REFERENCE, 
								XLA.PARTY_TYPE_CODE, 
								NULL PARTY_TYPE, 
								OTT.LEDGER_ID, 
								OTT.LEDGER_SHORT_NAME, 
								OTT.LEDGER_DESCRIPTION, 
								OTT.LEDGER_NAME, 
								OTT.LEDGER_CURRENCY, 
								OTT.PERIOD_YEAR, 
								OTT.PERIOD_NUMBER, 
								OTT.PERIOD_NAME, 
								OTT.PERIOD_START_DATE, 
								OTT.PERIOD_END_DATE, 
								OTT.BALANCE_TYPE_CODE, 
								OTT.BALANCE_TYPE, 
								OTT.BUDGET_NAME, 
								OTT.ENCUMBRANCE_TYPE, 
								OTT.BEGIN_BALANCE_DR, 
								OTT.BEGIN_BALANCE_CR, 
								OTT.PERIOD_NET_DR, 
								OTT.PERIOD_NET_CR, 
								OTT.CODE_COMBINATION_ID, 
								OTT.ACCOUNTING_CODE_COMBINATION, 
								OTT.CODE_COMBINATION_DESCRIPTION, 
								OTT.CONTROL_ACCOUNT_FLAG, 
								OTT.CONTROL_ACCOUNT, 
								OTT.BALANCING_SEGMENT, 
								OTT.NATURAL_ACCOUNT_SEGMENT, 
								OTT.COST_CENTER_SEGMENT, 
								OTT.MANAGEMENT_SEGMENT, 
								OTT.INTERCOMPANY_SEGMENT, 
								OTT.BALANCING_SEGMENT_DESC, 
								OTT.NATURAL_ACCOUNT_DESC, 
								OTT.COST_CENTER_DESC, 
								OTT.MANAGEMENT_SEGMENT_DESC, 
								OTT.INTERCOMPANY_SEGMENT_DESC, 
								OTT.segment1 SEGMENT1, 
								OTT.segment2 SEGMENT2, 
								OTT.segment3 SEGMENT3, 
								OTT.segment4 SEGMENT4, 
								OTT.segment5 SEGMENT5, 
								OTT.segment6 SEGMENT6, 
								OTT.segment7 SEGMENT7, 
								OTT.segment8 SEGMENT8, 
								OTT.segment9 SEGMENT9, 
								OTT.segment10 SEGMENT10, 
								OTT.BEGIN_RUNNING_TOTAL_CR, 
								OTT.BEGIN_RUNNING_TOTAL_DR, 
								OTT.END_RUNNING_TOTAL_CR, 
								OTT.END_RUNNING_TOTAL_DR, 
								OTT.LEGAL_ENTITY_ID, 
								OTT.LEGAL_ENTITY_NAME, 
								OTT.LE_ADDRESS_LINE_1, 
								OTT.LE_ADDRESS_LINE_2, 
								OTT.LE_ADDRESS_LINE_3, 
								OTT.LE_CITY, 
								OTT.LE_REGION_1, 
								OTT.LE_REGION_2, 
								OTT.LE_REGION_3, 
								OTT.LE_POSTAL_CODE, 
								OTT.LE_COUNTRY, 
								OTT.LE_REGISTRATION_NUMBER, 
								OTT.LE_REGISTRATION_EFFECTIVE_FROM, 
								OTT.LE_BR_DAILY_INSCRIPTION_NUMBER, 
								OTT.LE_BR_DAILY_INSCRIPTION_DATE, 
								OTT.LE_BR_DAILY_ENTITY, 
								OTT.LE_BR_DAILY_LOCATION, 
								OTT.LE_BR_DIRECTOR_NUMBER, 
								OTT.LE_BR_ACCOUNTANT_NUMBER, 
								OTT.LE_BR_ACCOUNTANT_NAME, 
								(
									CASE WHEN XLA.event_class_code = 'SALES_ORDER' 
										AND OTT.user_je_source_name = 'Cost Management' 
										AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS' THEN NVL(XLA.order_Number, ' ') ELSE POAP.segment1 || ' - ' || POAP.name END
									) BE_PROJECT_NO, 
									POAP.BE_PROJECT_DESC, 
									(
										CASE WHEN XLA.entity_code = 'TRANSACTIONS' 
											AND OTT.user_je_source_name = 'Receivables' THEN NVL(XLA.REC_address1, '') WHEN XLA.event_class_code = 'ADJUSTMENT' 
												AND OTT.user_je_source_name = 'Receivables' THEN NVL(XLA.ADJ_address1, '') WHEN XLA.event_class_code = 'SALES_ORDER' 
													AND OTT.user_je_source_name = 'Cost Management' 
													AND XLA.ent_entity_code = 'MTL_ACCOUNTING_EVENTS' THEN NVL(XLA.address1, ' ') ELSE POAP.task_number || ' ' END
												) BE_TASK_NO, 
												POAP.BE_TASK_DESC, 
												OTT.approver_name, 
												(
													NVL(POAP.PO_NUMBER, ' ')
												) PO_NUMBER, 
												POAP.invoice_num, 
												POAP.INVOICE_ID, 
												POAP.vendor_id, 
												POAP.vendor_site_id, 
												OTT.TRANSLATED_FLAG, 
												XLA.TEMP_LINE_NUM 
												FROM 
												bec_dwh.FACT_GL_XLA_STG XLA, 
												bec_dwh.FACT_GL_JOURNAL_STG OTT, 
												CTE_PO_AP POAP 
												Where 
												1 = 1 
												AND OTT.gl_sl_link_id = XLA.gl_sl_link_id 
												AND OTT.gl_sl_link_table = XLA.gl_sl_link_table 
												AND XLA.balance_type_code = OTT.balance_type_code 
												AND NVL(XLA.budget_version_id,-19999) = NVL(OTT.budget_version_id,-19999) 
												AND NVL(XLA.encumbrance_type_id,-19999)= NVL(OTT.encumbrance_type_id,-19999) 
												AND POAP.invoice_distribution_id (+)= XLA.alloc_to_dist_id_num_1
											) 
											UNION ALL 
											(
												with gjl_gjh as(
													SELECT 
													gjh.default_effective_date, 
													gjh.creation_date, 
													gjh.last_update_date, 
													gjh.reference_date, 
													gjh.posting_acct_seq_value, 
													gjh.close_acct_seq_value, 
													gjh.je_header_id as header_id, 
													gjh.description as HEADER_DESCRIPTION, 
													gjh.je_category, 
													gjh.je_header_id, 
													gjh.NAME, 
													gjh.ACCRUAL_REV_PERIOD_NAME, 
													gjh.ACCRUAL_REV_STATUS, 
													gjh.external_reference, 
													gjh.currency_code, 
													gjh.currency_conversion_rate, 
													gjh.currency_conversion_date, 
													gjh.currency_conversion_type, 
													gjh.actual_flag, 
													gjh.je_from_sla_flag, 
													gjh.budget_version_id, 
													gjh.encumbrance_type_id, 
													gjh.je_batch_id, 
													gjh.posting_acct_seq_version_id, 
													gjh.close_acct_seq_version_id, 
													gjh.je_source, 
													gjl.ledger_id, 
													gjl.code_combination_id, 
													gjl.period_name, 
													gjl.created_by, 
													gjl.je_line_num GL_LINE_NUMBER, 
													gjl.effective_date, 
													gjl.je_line_num LINE_NUMBER, 
													gjl.je_line_num ORIG_LINE_NUMBER, 
													gjl.description as LINE_DESCRIPTION, 
													gjl.entered_dr, 
													gjl.entered_cr, 
													gjl.accounted_dr, 
													gjl.accounted_cr, 
													gjl.stat_amount, 
													gjl.jgzz_recon_ref_11i, 
													gjl.attribute1, 
													gjl.attribute2 
													FROM 
													(select * from bec_ods.gl_je_lines where is_deleted_flg <> 'Y') gjl, 
													(select * from bec_ods.gl_je_headers where is_deleted_flg <> 'Y') gjh 
													WHERE 
													gjh.je_header_id = gjl.je_header_id 
													and gjl.last_update_date >= to_date(
													'2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS') 
													and gjh.last_update_date >= to_date(
														'2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS'
													) 
													and (
														gjl.kca_seq_date >= (
															select 
															(executebegints - prune_days) 
															from 
															bec_etl_ctrl.batch_dw_info 
															where 
															dw_table_name = 'fact_gl_account_analysis' 
															and batch_name = 'gl'
														) 
														OR gjh.kca_seq_date >= (
															select 
															(executebegints - prune_days) 
															from 
															bec_etl_ctrl.batch_dw_info 
															where 
															dw_table_name = 'fact_gl_account_analysis' 
															and batch_name = 'gl'
														)
													)
													and NVL(gjh.je_from_sla_flag, 'N') = 'N'
												), 
												-------------------------------------------------------------------------------------------------------------  
												gjst as (
													SELECT 
													gjst.user_je_source_name user_je_source_name, 
													gjst.je_source_name je_source_name, 
													gjst.language "language" 
													FROM 
													(select * from bec_ods.gl_je_sources_tl where is_deleted_flg <> 'Y') gjst 
													WHERE 
													gjst.language = 'US'
												), 
												-------------------------------------------------------------------------------------------------------------
												gdct as (
													SELECT 
													gdct.user_conversion_type, 
													gdct.conversion_type 
													FROM 
													(select * from bec_ods.gl_daily_conversion_types where is_deleted_flg <> 'Y') gdct
												), 
												-------------------------------------------------------------------------------------------------------------
												gjb_papf as (
													SELECT 
													gjb.je_batch_id, 
													gjb.NAME, 
													gjb.status, 
													gjb.APPROVER_EMPLOYEE_ID, 
													gjb.creation_date, 
													gjb.posted_date, 
													papf.person_id, 
													papf.full_name, 
													papf.effective_start_date, 
													papf.effective_end_date 
													FROM 
													(select * from bec_ods.gl_je_batches where is_deleted_flg <> 'Y') gjb, 
													(select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y') papf 
													WHERE 
													gjb.APPROVER_EMPLOYEE_ID = papf.person_id(+) 
													AND gjb.creation_date BETWEEN papf.effective_start_date (+) 
													and NVL(
														papf.effective_end_date (+), 
														GETDATE()+ 1
													) 
													and gjb.status = 'P' 
													and (gjb.kca_seq_date >= (
														select 
														(executebegints - prune_days) 
														from 
														bec_etl_ctrl.batch_dw_info 
														where 
														dw_table_name = 'fact_gl_account_analysis' 
														and batch_name = 'gl'
													))
												) -------------------------------------------------------------------------------------------------------------
												--Main Query Starts Here
												-------------------------------------------------------------------------------------------------------------
												SELECT 
												gjl_gjh.default_effective_date GL_DATE, 
												gjl_gjh.created_by created_by_id, 
												gjl_gjh.CREATION_DATE, 
												gjl_gjh.LAST_UPDATE_DATE, 
												NULL :: TIMESTAMP GL_TRANSFER_DATE, 
												gjl_gjh.REFERENCE_DATE, 
												NULL :: TIMESTAMP COMPLETED_DATE, 
												NULL TRANSACTION_NUMBER, 
												NULL :: TIMESTAMP TRANSACTION_DATE, 
												fsv1.header_name ACCOUNTING_SEQUENCE_NAME, 
												fsv1.version_name ACCOUNTING_SEQUENCE_VERSION, 
												gjl_gjh.posting_acct_seq_value ACCOUNTING_SEQUENCE_NUMBER, 
												fsv2.header_name REPORTING_SEQUENCE_NAME, 
												fsv2.version_name REPORTING_SEQUENCE_VERSION, 
												gjl_gjh.close_acct_seq_value REPORTING_SEQUENCE_NUMBER, 
												NULL DOCUMENT_CATEGORY, 
												NULL DOCUMENT_SEQUENCE_NAME, 
												NULL DOCUMENT_SEQUENCE_NUMBER, 
												NULL APPLICATION_ID, 
												NULL APPLICATION_NAME --1
												, 
												gjl_gjh.HEADER_ID, 
												gjl_gjh.HEADER_DESCRIPTION, 
												NULL FUND_STATUS, 
												gjl_gjh.je_category, 
												gjst.user_je_source_name JE_SOURCE_NAME, 
												NULL EVENT_ID, 
												NULL :: TIMESTAMP EVENT_DATE, 
												NULL EVENT_NUMBER, 
												NULL EVENT_CLASS_CODE, 
												NULL EVENT_CLASS_NAME, 
												NULL EVENT_TYPE_CODE, 
												NULL EVENT_TYPE_NAME, 
												gjb_papf.NAME GL_BATCH_NAME, 
												gjb_papf.POSTED_DATE, 
												gjl_gjh.je_header_id, 
												gjl_gjh.NAME GL_JE_NAME, 
												gjl_gjh.ACCRUAL_REV_PERIOD_NAME REVERSAL_PERIOD, 
												gjl_gjh.ACCRUAL_REV_STATUS REVERSAL_STATUS, 
												gjl_gjh.external_reference EXTERNAL_REFERENCE, 
												gjl_gjh.GL_LINE_NUMBER, 
												gjl_gjh.effective_date, 
												gjl_gjh.LINE_NUMBER, 
												gjl_gjh.ORIG_LINE_NUMBER, 
												NULL ACCOUNTING_CLASS_CODE, 
												NULL ACCOUNTING_CLASS_NAME, 
												gjl_gjh.LINE_DESCRIPTION, 
												gjl_gjh.currency_code ENTERED_CURRENCY, 
												gjl_gjh.currency_conversion_rate CONVERSION_RATE, 
												gjl_gjh.currency_conversion_date CONVERSION_RATE_DATE, 
												gjl_gjh.currency_conversion_type CONVERSION_RATE_TYPE_CODE, 
												gdct.user_conversion_type CONVERSION_RATE_TYPE, 
												gjl_gjh.entered_dr ENTERED_DR, 
												gjl_gjh.entered_cr ENTERED_CR, 
												NULL UNROUNDED_ACCOUNTED_DR, 
												NULL UNROUNDED_ACCOUNTED_CR, 
												gjl_gjh.accounted_dr ACCOUNTED_DR, 
												gjl_gjh.accounted_cr ACCOUNTED_CR, 
												gjl_gjh.stat_amount STATISTICAL_AMOUNT, 
												gjl_gjh.jgzz_recon_ref_11i RECONCILIATION_REFERENCE, 
												NULL PARTY_TYPE_CODE, 
												NULL PARTY_TYPE, 
												glbgt.ledger_id LEDGER_ID, 
												glbgt.ledger_short_name LEDGER_SHORT_NAME, 
												glbgt.ledger_description LEDGER_DESCRIPTION, 
												glbgt.ledger_name LEDGER_NAME, 
												glbgt.ledger_currency LEDGER_CURRENCY, 
												glbgt.period_year PERIOD_YEAR, 
												glbgt.period_number PERIOD_NUMBER, 
												glbgt.period_name PERIOD_NAME, 
												glbgt.period_start_date, 
												glbgt.period_end_date, 
												glbgt.balance_type_code BALANCE_TYPE_CODE, 
												glbgt.balance_type BALANCE_TYPE, 
												glbgt.budget_name BUDGET_NAME, 
												glbgt.encumbrance_type ENCUMBRANCE_TYPE, 
												glbgt.begin_balance_dr BEGIN_BALANCE_DR, 
												glbgt.begin_balance_cr BEGIN_BALANCE_CR, 
												glbgt.period_net_dr PERIOD_NET_DR, 
												glbgt.period_net_cr PERIOD_NET_CR, 
												glbgt.code_combination_id CODE_COMBINATION_ID, 
												'NA' :: VARCHAR(2) ACCOUNTING_CODE_COMBINATION, 
												'NA' :: VARCHAR(2) CODE_COMBINATION_DESCRIPTION, 
												glbgt.control_account_flag CONTROL_ACCOUNT_FLAG, 
												glbgt.control_account CONTROL_ACCOUNT, 
												'NA' :: VARCHAR(2) BALANCING_SEGMENT, 
												'NA' :: VARCHAR(2) NATURAL_ACCOUNT_SEGMENT, 
												'NA' :: VARCHAR(2) COST_CENTER_SEGMENT, 
												'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT, 
												'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT, 
												'NA' :: VARCHAR(2) BALANCING_SEGMENT_DESC, 
												'NA' :: VARCHAR(2) NATURAL_ACCOUNT_DESC, 
												'NA' :: VARCHAR(2) COST_CENTER_DESC, 
												'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT_DESC, 
												'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT_DESC, 
												glbgt.segment1 SEGMENT1, 
												glbgt.segment2 SEGMENT2, 
												glbgt.segment3 SEGMENT3, 
												glbgt.segment4 SEGMENT4, 
												glbgt.segment5 SEGMENT5, 
												glbgt.segment6 SEGMENT6, 
												glbgt.segment7 SEGMENT7, 
												glbgt.segment8 SEGMENT8, 
												glbgt.segment9 SEGMENT9, 
												glbgt.segment10 SEGMENT10, 
												'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_CR, 
												'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_DR, 
												'NA' :: VARCHAR(2) END_RUNNING_TOTAL_CR, 
												'NA' :: VARCHAR(2) END_RUNNING_TOTAL_DR, 
												'NA' :: VARCHAR(2) LEGAL_ENTITY_ID, 
												'NA' :: VARCHAR(2) LEGAL_ENTITY_NAME, 
												'NA' :: VARCHAR(2) LE_ADDRESS_LINE_1, 
												'NA' :: VARCHAR(2) LE_ADDRESS_LINE_2, 
												'NA' :: VARCHAR(2) LE_ADDRESS_LINE_3, 
												'NA' :: VARCHAR(2) LE_CITY, 
												'NA' :: VARCHAR(2) LE_REGION_1, 
												'NA' :: VARCHAR(2) LE_REGION_2, 
												'NA' :: VARCHAR(2) LE_REGION_3, 
												'NA' :: VARCHAR(2) LE_POSTAL_CODE, 
												'NA' :: VARCHAR(2) LE_COUNTRY, 
												'NA' :: VARCHAR(2) LE_REGISTRATION_NUMBER, 
												'NA' :: VARCHAR(2) LE_REGISTRATION_EFFECTIVE_FROM, 
												'NA' :: VARCHAR(2) LE_BR_DAILY_INSCRIPTION_NUMBER, 
												NULL :: TIMESTAMP LE_BR_DAILY_INSCRIPTION_DATE, 
												'NA' :: VARCHAR(2) LE_BR_DAILY_ENTITY, 
												'NA' :: VARCHAR(2) LE_BR_DAILY_LOCATION, 
												'NA' :: VARCHAR(2) LE_BR_DIRECTOR_NUMBER, 
												'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NUMBER, 
												'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NAME, 
												gjl_gjh.attribute1 BE_PROJECT_NO, 
												NULL BE_PROJECT_DESC, 
												gjl_gjh.attribute2 BE_TASK_NO, 
												null BE_TASK_DESC, 
												gjb_papf.full_name approver_name, 
												NULL PO_NUMBER, 
												NULL invoice_num, 
												NULL INVOICE_ID, 
												NULL vendor_id, 
												NULL vendor_site_id, 
												glbgt.TRANSLATED_FLAG, 
												0 TEMP_LINE_NUM 
												FROM 
												bec_dwh.FACT_XLA_REPORT_BALANCES_STG glbgt, 
												gjl_gjh, 
												(select * from bec_ods.fun_seq_versions where is_deleted_flg <> 'Y') fsv1, 
												(select * from bec_ods.fun_seq_versions where is_deleted_flg <> 'Y') fsv2, 
												gjst, 
												gdct, 
												gjb_papf 
												WHERE 
												gjl_gjh.ledger_id = glbgt.ledger_id 
												AND gjl_gjh.code_combination_id = glbgt.code_combination_id 
												AND (
													gjl_gjh.effective_date BETWEEN glbgt.period_start_date 
													AND glbgt.period_end_date
												) 
												AND gjl_gjh.period_name = glbgt.period_name -- AND   gjh.je_header_id                 = gjl.je_header_id
												AND gjl_gjh.actual_flag = glbgt.balance_type_code 
												AND decode(
													gjl_gjh.currency_code, 'STAT', gjl_gjh.currency_code, 
													glbgt.ledger_currency
												) = glbgt.ledger_currency 
												AND NVL(
												gjl_gjh.budget_version_id,-19999
												)= NVL(glbgt.budget_version_id,-19999) 
												AND NVL(
												gjl_gjh.encumbrance_type_id,-19999
												)= NVL(
													glbgt.encumbrance_type_id,-19999
												) 
												AND gjb_papf.je_batch_id = gjl_gjh.je_batch_id --AND   fdu.user_id                      = gjl.created_by --change
												AND fsv1.seq_version_id(+) = gjl_gjh.posting_acct_seq_version_id 
												AND fsv2.seq_version_id(+) = gjl_gjh.close_acct_seq_version_id --       AND   --gjct.je_category_name            = gjh.je_category
												AND gjst.je_source_name = gjl_gjh.je_source 
												AND gdct.conversion_type(+) = gjl_gjh.currency_conversion_type
											) 
											UNION ALL 
											(
												SELECT 
												gjh.default_effective_date as GL_DATE, 
												gjl.created_by created_by_id, 
												gjh.creation_date as CREATION_DATE, 
												gjh.last_update_date as LAST_UPDATE_DATE, 
												NULL :: TIMESTAMP as GL_TRANSFER_DATE, 
												gjh.reference_date as REFERENCE_DATE, 
												NULL :: TIMESTAMP as COMPLETED_DATE, 
												NULL TRANSACTION_NUMBER, 
												NULL :: TIMESTAMP as TRANSACTION_DATE, 
												fsv1.header_name ACCOUNTING_SEQUENCE_NAME, 
												fsv1.version_name ACCOUNTING_SEQUENCE_VERSION, 
												gjh.posting_acct_seq_value ACCOUNTING_SEQUENCE_NUMBER, 
												fsv2.header_name REPORTING_SEQUENCE_NAME, 
												fsv2.version_name REPORTING_SEQUENCE_VERSION, 
												gjh.close_acct_seq_value REPORTING_SEQUENCE_NUMBER, 
												NULL DOCUMENT_CATEGORY, 
												NULL DOCUMENT_SEQUENCE_NAME, 
												NULL DOCUMENT_SEQUENCE_NUMBER, 
												NULL APPLICATION_ID, 
												NULL APPLICATION_NAME, 
												NULL HEADER_ID, 
												gjh.description HEADER_DESCRIPTION, 
												NULL FUND_STATUS, 
												gjh.je_category, 
												gjst.user_je_source_name JE_SOURCE_NAME, 
												NULL EVENT_ID, 
												NULL :: TIMESTAMP as EVENT_DATE, 
												NULL EVENT_NUMBER, 
												NULL EVENT_CLASS_CODE, 
												NULL EVENT_CLASS_NAME, 
												NULL EVENT_TYPE_CODE, 
												NULL EVENT_TYPE_NAME, 
												gjb.NAME GL_BATCH_NAME, 
												gjb.posted_date, 
												gjh.je_header_id, 
												gjh.NAME GL_JE_NAME, 
												gjh.ACCRUAL_REV_PERIOD_NAME REVERSAL_PERIOD, 
												gjh.ACCRUAL_REV_STATUS REVERSAL_STATUS, 
												gjh.external_reference EXTERNAL_REFERENCE, 
												gjl.je_line_num GL_LINE_NUMBER, 
												gjl.effective_date, 
												gjl.je_line_num LINE_NUMBER, 
												gjl.je_line_num ORIG_LINE_NUMBER, 
												NULL ACCOUNTING_CLASS_CODE, 
												NULL ACCOUNTING_CLASS_NAME, 
												gjl.description LINE_DESCRIPTION, 
												gjh.currency_code ENTERED_CURRENCY, 
												gjh.currency_conversion_rate CONVERSION_RATE, 
												gjh.currency_conversion_date as CONVERSION_RATE_DATE, 
												gjh.currency_conversion_type CONVERSION_RATE_TYPE_CODE, 
												gdct.user_conversion_type CONVERSION_RATE_TYPE, 
												gjl.entered_dr ENTERED_DR, 
												gjl.entered_cr ENTERED_CR, 
												NULL UNROUNDED_ACCOUNTED_DR, 
												NULL UNROUNDED_ACCOUNTED_CR, 
												gjl.accounted_dr ACCOUNTED_DR, 
												gjl.accounted_cr ACCOUNTED_CR, 
												gjl.stat_amount STATISTICAL_AMOUNT, 
												gjl.jgzz_recon_ref_11i RECONCILIATION_REFERENCE, 
												NULL PARTY_TYPE_CODE, 
												NULL PARTY_TYPE, 
												glbgt.ledger_id LEDGER_ID, 
												glbgt.ledger_short_name LEDGER_SHORT_NAME, 
												glbgt.ledger_description LEDGER_DESCRIPTION, 
												glbgt.ledger_name LEDGER_NAME, 
												glbgt.ledger_currency LEDGER_CURRENCY, 
												glbgt.period_year PERIOD_YEAR, 
												glbgt.period_number PERIOD_NUMBER, 
												glbgt.period_name PERIOD_NAME, 
												glbgt.period_start_date, 
												glbgt.period_end_date, 
												glbgt.balance_type_code BALANCE_TYPE_CODE, 
												glbgt.balance_type BALANCE_TYPE, 
												glbgt.budget_name BUDGET_NAME, 
												glbgt.encumbrance_type ENCUMBRANCE_TYPE, 
												glbgt.begin_balance_dr BEGIN_BALANCE_DR, 
												glbgt.begin_balance_cr BEGIN_BALANCE_CR, 
												glbgt.period_net_dr PERIOD_NET_DR, 
												glbgt.period_net_cr PERIOD_NET_CR, 
												glbgt.code_combination_id CODE_COMBINATION_ID, 
												'NA' :: VARCHAR(2) ACCOUNTING_CODE_COMBINATION, 
												'NA' :: VARCHAR(2) CODE_COMBINATION_DESCRIPTION, 
												glbgt.control_account_flag CONTROL_ACCOUNT_FLAG, 
												glbgt.control_account CONTROL_ACCOUNT, 
												'NA' :: VARCHAR(2) BALANCING_SEGMENT, 
												'NA' :: VARCHAR(2) NATURAL_ACCOUNT_SEGMENT, 
												'NA' :: VARCHAR(2) COST_CENTER_SEGMENT, 
												'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT, 
												'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT, 
												'NA' :: VARCHAR(2) BALANCING_SEGMENT_DESC, 
												'NA' :: VARCHAR(2) NATURAL_ACCOUNT_DESC, 
												'NA' :: VARCHAR(2) COST_CENTER_DESC, 
												'NA' :: VARCHAR(2) MANAGEMENT_SEGMENT_DESC, 
												'NA' :: VARCHAR(2) INTERCOMPANY_SEGMENT_DESC, 
												glbgt.segment1 SEGMENT1, 
												glbgt.segment2 SEGMENT2, 
												glbgt.segment3 SEGMENT3, 
												glbgt.segment4 SEGMENT4, 
												glbgt.segment5 SEGMENT5, 
												glbgt.segment6 SEGMENT6, 
												glbgt.segment7 SEGMENT7, 
												glbgt.segment8 SEGMENT8, 
												glbgt.segment9 SEGMENT9, 
												glbgt.segment10 SEGMENT10, 
												'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_CR, 
												'NA' :: VARCHAR(2) BEGIN_RUNNING_TOTAL_DR, 
												'NA' :: VARCHAR(2) END_RUNNING_TOTAL_CR, 
												'NA' :: VARCHAR(2) END_RUNNING_TOTAL_DR, 
												'NA' :: VARCHAR(2) LEGAL_ENTITY_ID, 
												'NA' :: VARCHAR(2) LEGAL_ENTITY_NAME, 
												'NA' :: VARCHAR(2) LE_ADDRESS_LINE_1, 
												'NA' :: VARCHAR(2) LE_ADDRESS_LINE_2, 
												'NA' :: VARCHAR(2) LE_ADDRESS_LINE_3, 
												'NA' :: VARCHAR(2) LE_CITY, 
												'NA' :: VARCHAR(2) LE_REGION_1, 
												'NA' :: VARCHAR(2) LE_REGION_2, 
												'NA' :: VARCHAR(2) LE_REGION_3, 
												'NA' :: VARCHAR(2) LE_POSTAL_CODE, 
												'NA' :: VARCHAR(2) LE_COUNTRY, 
												'NA' :: VARCHAR(2) LE_REGISTRATION_NUMBER, 
												'NA' :: VARCHAR(2) LE_REGISTRATION_EFFECTIVE_FROM, 
												'NA' :: VARCHAR(2) LE_BR_DAILY_INSCRIPTION_NUMBER, 
												NULL :: TIMESTAMP LE_BR_DAILY_INSCRIPTION_DATE, 
												'NA' :: VARCHAR(2) LE_BR_DAILY_ENTITY, 
												'NA' :: VARCHAR(2) LE_BR_DAILY_LOCATION, 
												'NA' :: VARCHAR(2) LE_BR_DIRECTOR_NUMBER, 
												'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NUMBER, 
												'NA' :: VARCHAR(2) LE_BR_ACCOUNTANT_NAME, 
												gjl.attribute1 BE_PROJECT_NO, 
												NULL BE_PROJECT_DESC, 
												gjl.attribute2 BE_TASK_NO, 
												NULL BE_TASK_DESC, 
												PAPF.FULL_NAME APPROVER_NAME, 
												NULL PO_NUMBER, 
												NULL invoice_num, 
												NULL INVOICE_ID, 
												NULL vendor_id, 
												NULL vendor_site_id, 
												glbgt.TRANSLATED_FLAG, 
												0 TEMP_LINE_NUM 
												FROM 
												bec_dwh.FACT_XLA_REPORT_BALANCES_STG glbgt, 
												(select * from bec_ods.fnd_new_messages where is_deleted_flg <> 'Y') fnm, 
												(select * from bec_ods.fun_seq_versions where is_deleted_flg <> 'Y') fsv1, 
												(select * from bec_ods.fun_seq_versions where is_deleted_flg <> 'Y') fsv2, 
												(select * from bec_ods.gl_je_sources_tl where is_deleted_flg <> 'Y') gjst, 
												(select * from bec_ods.gl_daily_conversion_types where is_deleted_flg <> 'Y') gdct, 
												(select * from bec_ods.gl_je_lines where is_deleted_flg <> 'Y') gjl, 
												(select * from bec_ods.gl_je_headers where is_deleted_flg <> 'Y') gjh, 
												(select * from bec_ods.gl_je_batches where is_deleted_flg <> 'Y') gjb, 
												(select * from bec_ods.PER_ALL_PEOPLE_F where is_deleted_flg <> 'Y') PAPF 
												WHERE 
												gjl.ledger_id = glbgt.ledger_id 
												AND gjl.code_combination_id = glbgt.code_combination_id 
												AND gjl.effective_date BETWEEN glbgt.period_start_date 
												AND glbgt.period_end_date 
												AND gjl.period_name = glbgt.period_name 
												AND gjh.je_header_id = gjl.je_header_id 
												AND gjh.actual_flag = glbgt.balance_type_code 
												AND decode(
													gjh.currency_code, 'STAT', gjh.currency_code, 
													glbgt.ledger_currency
												) = glbgt.ledger_currency --added bug 6686541
												AND NVL(gjh.je_from_sla_flag, 'N') = 'U' 
												AND fnm.application_id = 101 
												AND fnm.language_code = 'US' 
												AND fnm.message_name in (
													'PPOS0220', 'PPOS0221', 'PPOS0222', 
													'PPOS0243', 'PPOS0222_G', 'PPOSO275'
												) 
												AND gjl.description = fnm.message_text 
												AND NVL(gjh.budget_version_id,-19999) = NVL(glbgt.budget_version_id,-19999) 
												AND NVL(gjh.encumbrance_type_id,-19999) = NVL(
													glbgt.encumbrance_type_id,-19999
												) 
												AND gjb.je_batch_id = gjh.je_batch_id 
												AND gjb.status = 'P' 
												AND GJB.APPROVER_EMPLOYEE_ID = PAPF.PERSON_ID(+) 
												AND gjb.creation_date BETWEEN papf.effective_start_date (+) 
												and NVL(
													papf.effective_end_date (+), 
													SYSDATE + 1
												) --       AND   fdu.user_id                      = gjl.created_by  --change
												AND fsv1.seq_version_id(+) = gjh.posting_acct_seq_version_id 
												AND fsv2.seq_version_id(+) = gjh.close_acct_seq_version_id --AND   gjct.je_category_name            = gjh.je_category
												AND gjst.je_source_name = gjh.je_source 
												AND gjst.language = 'US' 
												AND gdct.conversion_type(+) = gjh.currency_conversion_type 
												AND not exists (
													select 
													'x' 
													from 
													(select * from bec_ods.gl_import_references where is_deleted_flg <> 'Y') gir 
													where 
													gir.je_header_id = gjl.je_header_id 
													and gir.je_line_num = gjl.je_line_num
												) 
												AND (
													gjl.kca_seq_date >= (
														select 
														(executebegints - prune_days) 
														from 
														bec_etl_ctrl.batch_dw_info 
														where 
														dw_table_name = 'fact_gl_account_analysis' 
														and batch_name = 'gl'
													) 
													OR gjh.kca_seq_date >= (
														select 
														(executebegints - prune_days) 
														from 
														bec_etl_ctrl.batch_dw_info 
														where 
														dw_table_name = 'fact_gl_account_analysis' 
														and batch_name = 'gl'
													) 
													OR gjb.kca_seq_date >= (
														select 
														(executebegints - prune_days) 
														from 
														bec_etl_ctrl.batch_dw_info 
														where 
														dw_table_name = 'fact_gl_account_analysis' 
														and batch_name = 'gl'
													)
												)
											)
										) A
									);
									
									commit;
									
								end;
								
								update 
								bec_etl_ctrl.batch_dw_info 
								set 
								last_refresh_date = getdate() 
								where 
								dw_table_name = 'fact_gl_account_analysis' 
								and batch_name = 'gl';
							commit;
														