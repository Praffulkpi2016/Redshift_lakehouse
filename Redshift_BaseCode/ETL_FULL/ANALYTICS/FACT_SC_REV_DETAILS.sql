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
drop table if exists bec_dwh.FACT_SC_REV_DETAILS;
create table bec_dwh.FACT_SC_REV_DETAILS diststyle all sortkey(
  contract_id, contract_line_id,customer_trx_line_id ) AS 
 (
select 
contract_number
,sts_code
,hdr_start_date
,hdr_end_date
,contract_id
,contract_line_id
,contract_line_number
,contract_sub_line_id
,line_sts_code
,currency_code
,line_start_date
,line_end_date
,ship_to_site_use_id
,bill_to_site_use_id
,price_negotiated
,customer_trx_line_id
,gl_dist_id
,event_id
,transaction_currency
,invoice_number
,invoice_date
,line_number
,gl_date
,code_combination_id
,period_name
,invoice_amount
,revenue_amount_trans_curr
,acctd_amount
,revenue_amount_acctd_curr
,conversion_rate
,tmo_prcntge
,bill_from_date
,bill_to_date
,date_transaction
,date_to_interface
,level_billing_amount
,line_type
,org_id
,service_name
,gbl_invoice_amount
,gbl_acctd_amount
,gbl_level_billing_amount,
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
    ) || '-' || nvl(contract_id, 0)|| '-' || nvl(contract_line_id, 'NA') || '-' || nvl(customer_trx_line_id, 0)
    || '-'||nvl(date_transaction,'1900-01-01 00:00:00.000'::timestamp)|| '-'||nvl(date_to_interface,'1900-01-01 00:00:00.000'::timestamp) as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
from 
(
WITH dist AS (
    SELECT
        dist.customer_trx_line_id,
        dist.customer_trx_id,
        MAX(dist.cust_trx_line_gl_dist_id) cust_trx_line_gl_dist_id,
        SUM(dist.amount)                   amount,
        dist.gl_date                       gl_date,
        MAX(dist.event_id)                 event_id,
        MAX(dist.last_update_date)         last_update_date,
        SUM(dist.acctd_amount)             acctd_amount,
        dist.code_combination_id,
        dist.account_class
    FROM
        (SELECT * FROM BEC_ODS.ra_cust_trx_line_gl_dist_all WHERE is_deleted_flg <> 'Y') dist
    WHERE
            upper(dist.account_class) = 'REV'
        AND amount IS NOT NULL
    GROUP BY
        dist.gl_date,
        dist.customer_trx_line_id,
        dist.customer_trx_id,
        dist.code_combination_id,
        dist.account_class
), billing_details AS (
    SELECT
        rctl.interface_line_attribute1 contract_number,
        bcl.cle_id                     id,
        rctl.contract_line_id,
        dist.customer_trx_line_id,
        rct.trx_number,
        rct.trx_date,
        rct.invoice_currency_code,
        rct.exchange_rate,
        rctl.line_number,
        rctl.interface_line_attribute4,
        rctl.interface_line_attribute5,
        rctl.quantity_invoiced,
        rctl.unit_selling_price,
        rctl.interface_line_attribute11,
        rctl.interface_line_attribute12,
        dist.customer_trx_id,
        dist.cust_trx_line_gl_dist_id,
        dist.amount,
        dist.gl_date,
        dist.event_id,
        dist.last_update_date,
        dist.acctd_amount,
        dist.account_class,
        dist.code_combination_id
    FROM
        (SELECT * FROM BEC_ODS.oks_bill_cont_lines       WHERE is_deleted_flg <> 'Y') bcl,
        (SELECT * FROM BEC_ODS.oks_bill_transactions     WHERE is_deleted_flg <> 'Y') btn,
        (SELECT * FROM BEC_ODS.oks_bill_txn_lines        WHERE is_deleted_flg <> 'Y') btl,
        (SELECT * FROM BEC_ODS.ra_customer_trx_lines_all WHERE is_deleted_flg <> 'Y') rctl,
        (SELECT * FROM BEC_ODS.ra_customer_trx_all       WHERE is_deleted_flg <> 'Y') rct,
		dist
    WHERE
            btn.id = bcl.btn_id
        AND btl.btn_id = btn.id
        AND btl.bill_instance_number IS NOT NULL
        AND btl.bill_instance_number = rctl.interface_line_attribute3
        AND rctl.interface_line_context = 'OKS CONTRACTS'
        AND rct.customer_trx_id = rctl.customer_trx_id
        AND dist.customer_trx_line_id = rctl.customer_trx_line_id
        AND dist.account_class = 'REV'
)
SELECT
    hdr.contract_number,
    hdr.sts_code,
    hdr.start_date                     hdr_start_date,
    hdr.end_date                       hdr_end_date,
    hdr.id                             contract_id,
    okl.id                             contract_line_id,
    okl.line_number                    contract_line_number,
    okl1.id                            contract_sub_line_id,
    okl.sts_code                       line_sts_code,
    okl.currency_code,
    okl.start_date                     line_start_date,
    okl.end_date                       line_end_date,
    okl.ship_to_site_use_id,
    okl.bill_to_site_use_id,
    okl.price_negotiated,
    inv.customer_trx_line_id,
    inv.cust_trx_line_gl_dist_id       gl_dist_id,
    inv.event_id,
    inv.invoice_currency_code          transaction_currency,
    inv.trx_number                     invoice_number,
    inv.trx_date                       invoice_date,
    inv.line_number,
    inv.gl_date                        gl_date,
    inv.code_combination_id,
    to_char(inv.gl_date, 'MON-YY')     period_name,
    SUM(nvl(inv.amount,0)) invoice_amount,
    SUM((nvl(inv.quantity_invoiced, 0) * inv.unit_selling_price))   revenue_amount_trans_curr,
    SUM(nvl(inv.acctd_amount,0)) acctd_amount,
    (decode(inv.exchange_rate, NULL, 1, inv.exchange_rate) * SUM((nvl(inv.quantity_invoiced, 0) * inv.unit_selling_price))) revenue_amount_acctd_curr,
    decode(inv.exchange_rate, NULL, 1, inv.exchange_rate)    conversion_rate,
    inv.interface_line_attribute11                           tmo_prcntge,
    inv.interface_line_attribute4 ::timestamp                bill_from_date,
    inv.interface_line_attribute5  ::timestamp               bill_to_date,
    lvl.date_transaction,
    lvl.date_to_interface,
    SUM(NVL(lvl.amount,0))                                   level_billing_amount,
    lset.name                                                line_type,
	hdr.org_id,
	oldv.service_name,
		sum(cast(NVL(inv.amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2))) GBL_INVOICE_AMOUNT,
    sum(cast(NVL(inv.acctd_amount,0) * NVL(DCR.conversion_rate,1)as decimal(18,2))) GBL_ACCTD_AMOUNT,
	 sum(cast(NVL(lvl.amount,0) * NVL(DCR.conversion_rate,1)as decimal(18,2))) GBL_LEVEL_BILLING_AMOUNT
FROM
(SELECT * FROM BEC_ODS.okc_k_headers_all_b       WHERE is_deleted_flg <> 'Y')  hdr,
(SELECT * FROM BEC_ODS.okc_k_lines_b       WHERE is_deleted_flg <> 'Y') okl,
(SELECT * FROM BEC_ODS.okc_k_lines_b       WHERE is_deleted_flg <> 'Y') okl1,
--(SELECT * FROM BEC_ODS.oks_level_elements       WHERE is_deleted_flg <> 'Y') lvl,
(SELECT * FROM BEC_ODS.okc_line_styles_tl       WHERE is_deleted_flg <> 'Y') lset,
(SELECT * FROM BEC_ODS.OKS_LINE_DETAILS_V       WHERE is_deleted_flg <> 'Y') oldv,
(select cle_id,max(lvl.date_transaction) date_transaction,
    maX(lvl.date_to_interface) date_to_interface,sum(amount) amount
    from bec_ods.oks_level_elements lvl
    group by 
    cle_id) lvl,
    billing_details          inv,
	(select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y'
	and to_currency = 'USD'
	and conversion_type = 'Corporate') DCR
WHERE
        1 = 1
    AND hdr.id = okl.chr_id
    AND okl.cle_id IS NULL
    --AND hdr.sts_code in ('ACTIVE','SIGNED')
    AND okl.id = inv.id
    AND inv.contract_line_id = okl1.id
    AND okl1.dnz_chr_id = hdr.id  
    AND okl1.cle_id IS NOT NULL
    AND okl1.dnz_chr_id = okl.chr_id
    AND okl1.cle_id = okl.id
    --AND okl1.sts_code <> 'TERMINATED'
    AND lvl.cle_id = okl.id
    AND lset.id = okl.lse_id
	AND lset.id= 1 --added for service contracts
	and oldv.contract_id = hdr.id
	and oldv.line_id = okl.id
	--and hdr.contract_number  IN ( 'CTC012.0','SCE0000')
	--and gl_date = '2022-01-17 00:00:00.000'
	and inv.invoice_currency_code = DCR.from_currency(+)
	and DCR.conversion_date(+) = inv.trx_date
GROUP BY
    hdr.contract_number,
    hdr.sts_code,
    hdr.start_date,
    hdr.end_date,
    hdr.id,
    okl.id,
    okl.line_number,
    okl1.id,
    okl.sts_code,
    okl.currency_code,
    okl.start_date,
    okl.end_date,
    okl.ship_to_site_use_id,
    okl.bill_to_site_use_id,
    okl.price_negotiated,
    inv.customer_trx_line_id,
    inv.cust_trx_line_gl_dist_id,
    inv.event_id,
    inv.invoice_currency_code,
    inv.trx_number,
    inv.trx_date,
    inv.line_number,
    inv.gl_date,
    inv.code_combination_id,
    to_char(inv.gl_date, 'MON-YY'),
    inv.exchange_rate,
    inv.interface_line_attribute11,
    inv.interface_line_attribute4,
    inv.interface_line_attribute5,
    lvl.date_transaction,
    lvl.date_to_interface,
    lset.name,
    hdr.org_id,
    oldv.service_name
	));
	
	end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_sc_rev_details' 
  and batch_name = 'sc';
commit;
