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
BEGIN;
	-- Delete Records
	DELETE
	FROM
	bec_dwh.FACT_AP_CASH_BURN
	WHERE
	(
		(nvl(INVOICE_ID, 0),nvl(dist_code_combination_id, 0),nvl(PO_DISTRIBUTION_ID, 0),nvl(INVOICE_PAYMENT_ID, 0),nvl(project_id,0),nvl(task_id,0))
	) IN 
	(
		SELECT
		nvl(ODS.INVOICE_ID,0) as INVOICE_ID ,
		nvl(ODS.dist_code_combination_id,0) as dist_code_combination_id ,
		nvl(ODS.PO_DISTRIBUTION_ID,0) as PO_DISTRIBUTION_ID ,
		nvl(ODS.INVOICE_PAYMENT_ID,0) as INVOICE_PAYMENT_ID ,
		nvl(ODS.project_id,0) as project_id ,
		nvl(ODS.task_id,0) as task_id  
		
		FROM
		bec_dwh.FACT_AP_CASH_BURN dw,
		( 
			SELECT  
			aip.invoice_id,
			aip.INVOICE_PAYMENT_ID,
			inv.PO_DISTRIBUTION_ID,
			inv.dist_code_combination_id,
			inv.project_id,
			inv.task_id
			FROM bec_ods.AP_CHECKS_ALL ac,
			bec_ods.AP_INVOICE_PAYMENTS_ALL AIP,
			-- GL_CODE_COMBINATIONS_KFV GCC,
			(  SELECT AIA.INVOICE_ID,
				AID.dist_code_combination_id,
				SUM (AID.AMOUNT) distribution_amount,
				AID.project_id,
				AID.TASK_ID,
				AID.PO_DISTRIBUTION_ID,
				AIA.is_deleted_flg,
				AILA.is_deleted_flg is_deleted_flg1,
				AID.is_deleted_flg is_deleted_flg2
                FROM bec_ods.AP_INVOICES_ALL AIA,
				bec_ods.AP_INVOICE_LINES_ALL AILA,
				bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
				WHERE     AIA.INVOICE_ID = AID.INVOICE_ID
				AND AIA.INVOICE_ID = AILA.INVOICE_ID
				AND AILA.LINE_NUMBER = AID.INVOICE_LINE_NUMBER
				GROUP BY AIA.INVOICE_ID,
				AID.dist_code_combination_id,
				AID.project_id,
				AID.TASK_ID,
				AID.PO_DISTRIBUTION_ID,
				AIA.is_deleted_flg,
				AILA.is_deleted_flg,
				AID.is_deleted_flg
				) INV
			WHERE     1 = 1
			--   AND ac.org_id = 85
			AND AIP.INVOICE_ID = INV.INVOICE_ID
			AND aip.check_id = ac.check_id
			
			AND (aip.kca_seq_date > (
				SELECT
				(executebegints - prune_days)
				FROM
				bec_etl_ctrl.batch_dw_info
				WHERE
				dw_table_name = 'fact_ap_cash_burn'
			AND batch_name = 'ap') 
			or ac.is_deleted_flg = 'Y'
			or AIP.is_deleted_flg = 'Y'
			or INV.is_deleted_flg = 'Y'
			or INV.is_deleted_flg1 = 'Y'
			or INV.is_deleted_flg2 = 'Y'
			)	
		) ODS
		WHERE
		dw.dw_load_id = 
		(
			SELECT
			system_id
			FROM
			bec_etl_ctrl.etlsourceappid
			WHERE
		source_system = 'EBS')    
		|| '-' || nvl(ods.INVOICE_ID, 0)  
		|| '-' || nvl(ods.dist_code_combination_id, 0)
		|| '-' || nvl(ods.PO_DISTRIBUTION_ID, 0)
		|| '-' || nvl(ods.INVOICE_PAYMENT_ID, 0)
		|| '-' || nvl(ods.project_id, 0)
		|| '-' || nvl(ods.task_id, 0)
	);
	COMMIT;
	
	-- Insert records
	
	INSERT
	INTO
	bec_dwh.FACT_AP_CASH_BURN
	(
		org_id,
		check_number,
		vendor_name,
		check_date,
		Amount,   
		check_id,
		invoice_id,
		INVOICE_PAYMENT_ID,
		status_lookup_code,
		/*  gcc.segment1 company_code,
			gcc.segment2 deptartment,
			gcc.segment3 core_account,
			gcc.segment4 intercompany,
			gcc.segment5 budget_id,
		gcc.segment6 LOCATION,*/
		distribution_amount,
		PO_DISTRIBUTION_ID,
		dist_code_combination_id,
		project_id,
		task_id ,
		check_id_key,
		invoice_id_key,
		INVOICE_PAYMENT_ID_key,
		PO_DISTRIBUTION_ID_key,
		dist_code_combination_id_key,
		project_id_key,
		task_id_key,
		is_deleted_flg,
		source_app_id,
		dw_load_id,
		dw_insert_date,
		dw_update_date
	)	
	(
		SELECT 
		org_id,
		check_number,
		vendor_name,
		check_date,
		Amount,   
		check_id,
		invoice_id,
		INVOICE_PAYMENT_ID,
		status_lookup_code,
		/*  gcc.segment1 company_code,
			gcc.segment2 deptartment,
			gcc.segment3 core_account,
			gcc.segment4 intercompany,
			gcc.segment5 budget_id,
		gcc.segment6 LOCATION,*/
		distribution_amount,
		PO_DISTRIBUTION_ID,
		dist_code_combination_id,
		project_id,
		task_id ,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || check_id as check_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || invoice_id as invoice_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || INVOICE_PAYMENT_ID as INVOICE_PAYMENT_ID_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || PO_DISTRIBUTION_ID as PO_DISTRIBUTION_ID_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || dist_code_combination_id as dist_code_combination_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || project_id as project_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || task_id as task_id_key,
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
		)
		|| '-' || nvl(INVOICE_ID, 0)  
		|| '-' || nvl(dist_code_combination_id, 0)
		|| '-' || nvl(PO_DISTRIBUTION_ID, 0)
		|| '-' || nvl(INVOICE_PAYMENT_ID, 0)
		|| '-' || nvl(project_id, 0)
		|| '-' || nvl(task_id, 0)
		as dw_load_id,  
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		from
		(
			SELECT ac.org_id,ac.check_number,
			ac.vendor_name,
			ac.check_date,
			aip.amount Amount,   
			ac.check_id,
			aip.invoice_id,
			aip.INVOICE_PAYMENT_ID,
			ac.status_lookup_code,
			/*  gcc.segment1 company_code,
				gcc.segment2 deptartment,
				gcc.segment3 core_account,
				gcc.segment4 intercompany,
				gcc.segment5 budget_id,
			gcc.segment6 LOCATION,*/
			inv.distribution_amount,
			inv.PO_DISTRIBUTION_ID,
			inv.dist_code_combination_id,
			inv.project_id,
			inv.task_id
			FROM ( SELECT * FROM bec_ods.AP_CHECKS_ALL  where is_deleted_flg <> 'Y' )  ac,
			( SELECT * FROM bec_ods.AP_INVOICE_PAYMENTS_ALL where is_deleted_flg <> 'Y' ) AIP,
			-- GL_CODE_COMBINATIONS_KFV GCC,
			(  SELECT AIA.INVOICE_ID,
				AID.dist_code_combination_id,
				SUM (AID.AMOUNT) distribution_amount,
				AID.project_id,
				AID.TASK_ID,
				AID.PO_DISTRIBUTION_ID
                FROM (SELECT * FROM bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') AIA,
				(SELECT * FROM bec_ods.AP_INVOICE_LINES_ALL where is_deleted_flg <> 'Y') AILA,
				(SELECT * FROM bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y') AID
				WHERE     AIA.INVOICE_ID = AID.INVOICE_ID
				AND AIA.INVOICE_ID = AILA.INVOICE_ID
				AND AILA.LINE_NUMBER = AID.INVOICE_LINE_NUMBER
				GROUP BY AIA.INVOICE_ID,
				AID.dist_code_combination_id,
				AID.project_id,
				AID.TASK_ID,
			AID.PO_DISTRIBUTION_ID ) INV
			WHERE     1 = 1
			--   AND ac.org_id = 85
			AND AIP.INVOICE_ID = INV.INVOICE_ID
			AND aip.check_id = ac.check_id
		AND (aip.kca_seq_date > (
		SELECT
		(executebegints - prune_days)
		FROM
		bec_etl_ctrl.batch_dw_info
		WHERE
		dw_table_name = 'fact_ap_cash_burn'
	AND batch_name = 'ap') )
	));
	
	
END;

UPDATE
bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name = 'fact_ap_cash_burn'
AND batch_name = 'ap';