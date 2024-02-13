/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.fact_journal_entries_rt;

create table bec_dwh_rpt.fact_journal_entries_rt 
	diststyle all as 
(SELECT gj.je_header_id
      ,gj.period_name
	  ,gj.JOURNAL_SOURCE
	  ,gj.je_line_num
	  ,gj.project_number
	  ,gj.task_number
	  ,NEXT_DAY (gj.effective_date, 'SUNDAY' ) expnd_ending_date
	  ,gj.effective_date
	  ,gj.expnd_type
	  ,gj.orig_transaction_reference as original_trans_ref
	  ,DECODE (gj.accounted_dr, NULL, gj.accounted_cr * -1, gj.accounted_dr) quantity
	  ,NULL expnd_type_class
	  ,NULL business_group
	  ,NULL employee_number
      ,'SUNNVYVALE OU' organization_name
      ,NULL trans_curr
	  ,DECODE (gj.accounted_dr, NULL, gj.accounted_cr * -1, gj.accounted_dr) trans_raw_cost
      ,NULL trans_burdened_cost
      ,DECODE (gj.accounted_dr, NULL, gj.accounted_cr * -1, gj.accounted_dr) functional_raw_cost
	  ,gj.effective_date gl_date
	  ,gj.code_combination_id debit_acct_ccid
	  ,gj.JOURNAL_LINE_STATUS
	  ,gj.company
	  ,gj.company
            || '.'
            || gj.department
            || '.'
            || gj.Account
            || '.'
            || gj.intercompany 
            || '.'
            || gj.budget_id
            || '.'
            || gj.LOCATION "Debit Account"
	  ,gj.code_combination_id credit_acct_ccid
	  ,DECODE (gj.accounted_dr, NULL, 'Y') negative_txn_flag
FROM bec_dwh.FACT_GL_JOURNALS gj
WHERE gj.orig_transaction_reference   not in (select  dei.orig_transaction_reference
  from bec_Dwh.dim_expenditure_items dei
where  dei.transaction_source =  'BE Project Journal')
AND gj.posted_date IS NOT NULL
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_journal_entries_rt'
	and batch_name = 'gl';

commit;