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
	drop  table if exists bec_dwh.FACT_SC_MASTER;
	create table bec_dwh.FACT_SC_MASTER diststyle all sortkey ( contract_id  ) as 
	( 
		select 
		contract_id,
		org_id,
		contract_number,
		description,
		header_status,
		cust_po_number,
		start_date,
		end_date,
		service_contract_currency,
		estimated_amount,
		invoice_currency_code,
		payment_term,
		acct_rule_name,
		inv_rule_name,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		bill_to_site_use_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || contract_id as contract_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || org_id as org_id_key,
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
		) || '-' || nvl(contract_id, 0)  as dw_load_id,  
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		from
		(
			with inv_curr as 
			(
				select distinct interface_line_attribute1 contract_number ,rct.invoice_currency_code
				from bec_ods.ra_customer_trx_lines_all rctl,bec_ods.ra_customer_trx_all rct
				where rctl.interface_line_context = 'OKS CONTRACTS' 
				and rct.customer_trx_id = rctl.customer_trx_id
			)
			SELECT
			okh.id            contract_id,
			okh.org_id,
			okh.contract_number,
			okst.short_description description,
			okh.sts_code      header_status,
			okh.cust_po_number,
			okh.start_date,
			okh.end_date,
			okh.currency_code service_contract_currency,
			okh.estimated_amount,
			--okh.attribute1  invoice_currency_code,
			inv.invoice_currency_code,
			(
				SELECT
				name
				FROM
				bec_ods.ra_terms_tl
				WHERE
				term_id = okh.payment_term_id
			)                 payment_term,
			(
				SELECT
				name
				FROM
				bec_ods.ra_rules
				WHERE
				rule_id = oks_h.acct_rule_id
			)                 acct_rule_name,
			(
				SELECT
				name
				FROM
				bec_ods.ra_rules
				WHERE
				rule_id = okh.inv_rule_id
			)                 inv_rule_name,
			okh.creation_date,
			okh.created_by,
			okh.last_update_date,
			okh.last_updated_by,
			okh.bill_to_site_use_id
			FROM
			bec_ods.okc_k_headers_all_b okh,
			bec_ods.okc_k_headers_tl    okst,
			bec_ods.oks_k_headers_b     oks_h,
			inv_curr inv
			WHERE
			1 = 1
			AND okh.id = okst.id
			AND okh.id = oks_h.chr_id
			AND okst.language = 'US'
			AND okh.bill_to_site_use_id IS NOT NULL
			--AND okh.sts_code = 'ACTIVE'
			AND okh.contract_number = inv.contract_number(+)
			-- AND okh.contract_number like 'HOM085%'
			GROUP BY
			okh.id,
			okh.org_id,
			okh.contract_number,
			okst.description,
			okh.sts_code,
			okh.cust_po_number,
			okh.start_date,
			okh.end_date,
			okh.currency_code,
			okh.estimated_amount,
			inv.invoice_currency_code,
			okh.payment_term_id,
			oks_h.acct_rule_id,
			okh.inv_rule_id,
			okh.creation_date,
			okh.created_by,
			okh.last_update_date,
			okh.last_updated_by,
			okh.bill_to_site_use_id,
			okst.short_description
		)
	);
	
END;
update 
bec_etl_ctrl.batch_dw_info 
set 
load_type = 'I', 
last_refresh_date = getdate() 
where 
dw_table_name = 'fact_sc_master' 
and batch_name = 'sc';
COMMIT;