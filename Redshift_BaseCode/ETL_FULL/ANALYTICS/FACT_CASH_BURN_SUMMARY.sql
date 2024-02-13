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
drop table if exists bec_dwh.FACT_CASH_BURN_SUMMARY;
create table bec_dwh.FACT_CASH_BURN_SUMMARY diststyle all sortkey(
  check_id, invoice_id ) AS 
 (
SELECT 
	DISTINCT AC.ORG_ID, ac.check_number,
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
);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cash_burn_summary' 
  and batch_name = 'ap';
commit;
