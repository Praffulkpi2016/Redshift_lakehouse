/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh_rpt.FACT_GL_SUPP_DETAILS_RT;

Insert Into bec_dwh_rpt.FACT_GL_SUPP_DETAILS_RT
(WITH AP_PO AS (select DISTINCT api.invoice_id, poh.segment1
from bec_ods.ap_invoice_lines_all api, 
     bec_ods.po_headers_all poh
 where api.po_header_id = poh.po_header_id
 and api.org_id = poh.org_id
 )
SELECT 
	gl.ledger_id, 
	gl.je_header_id, 
	gl.je_line_num, 
	gl.code_combination_id,
	apgl.ae_header_id, 
	apgl.ae_line_num,
	apgl.gl_sl_link_id,
    gl.account,
   -- xxbec_get_acct_desc(gl.account)                                   acct_descr,
    gl.batch_description,
    gl.batch_name,
    gl.budget_id,
    gl.company,
   -- xxbec_get_company_desc(gl.company)                                comp_descr,
    --gl.complete_account,
    gl.line_creation_date as creation_date,
    gl.department,
   -- xxbec_get_dept_desc(gl.department)                                dept_descr,
    gl.external_reference,
    gl.header_description,
    gl.je_category,
    gl.journal_name as je_header_name,
    gl.journal_source as je_source,
    gl.line_description,
    gl.period_name,
    gl.posted_by,
    gl.posted_date,
    gl.project_number,
    gl.running_total_accounted_cr,
    gl.running_total_accounted_dr,
    gl.journal_header_status as status,
    gl.task_number,
    NVL(decode(apgl.accounted_cr, NULL, gl.accounted_cr, apgl.accounted_cr),0) accounted_cr,
    NVL(decode(apgl.accounted_dr, NULL, gl.accounted_dr, apgl.accounted_dr),0) accounted_dr,
    api.invoice_num,
    --xxbec_get_po_number(inv.invoice_id)                               
	(SELECT LISTAGG(segment1 , ', ') WITHIN GROUP (ORDER BY invoice_id)
	 FROM AP_PO 
	 WHERE INVOICE_ID = API.INVOICE_ID
	 GROUP BY INVOICE_ID) po_number,
    api.vendor_id ,
	das.vendor_name,
	das.vendor_number
from bec_dwh.fact_gl_journals gl
    ,bec_dwh.fact_sla_details  apgl
	,bec_ods.ap_invoices_all api
	,bec_dwh.dim_ap_suppliers das
WHERE gl.je_header_id  = apgl.je_header_id(+)
AND gl.je_line_num  = apgl.je_line_num(+)
AND gl.ledger_id    = apgl.ledger_id(+)
AND gl.period_name  = apgl.period_name(+)
AND apgl.source_id_int_1 = api.invoice_id(+)
AND api.vendor_id   = das.vendor_id(+)
AND apgl.application_id(+) = 200
AND apgl.gl_transfer_mode_code(+) = 'S'
and apgl.accounting_entry_status_code(+) = 'F'
and apgl.gl_transfer_status_code(+) = 'Y'
and apgl.party_type_code(+) = 'S'
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_gl_supp_details_rt'
	and batch_name = 'gl';

commit;