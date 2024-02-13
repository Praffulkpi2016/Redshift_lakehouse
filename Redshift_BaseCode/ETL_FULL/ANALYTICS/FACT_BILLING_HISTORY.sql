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

drop table if exists bec_dwh.FACT_BILLING_HISTORY;

create table bec_dwh.FACT_BILLING_HISTORY diststyle all
sortkey (customer_trx_id,
customer_trx_line_id) 
as 
(
SELECT 
bill_to_account
,bill_to_customer_name
,ship_to_account
,ship_to_customer_name
,ship_to_address1
,ship_to_country
,state
,city
,zip
,source
,transaction_type
,invoice_number
,line_number
,line_type
,gl_date
,due_date
,invoice_date
,payment_terms
,purchase_order
,project_num_or_sales_order
,sales_order_date
,reference
,item
,inventory_item_id
,item_id_key
,item_desc
,uom
,billed_quantity
,unit_price
,extended_amount
,tax_amount
,unearned_account
,tax_account
,receivables_account
,inv_currency_code
,org_id
,org_id_key
,customer_trx_id
,customer_trx_line_id
,cast(NVL(a.extended_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) as GBL_EXTENDED_AMOUNT
,cast(NVL(a.tax_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) as GBL_TAX_AMOUNT
,'N' AS IS_DELETED_FLG
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
from 
(
select
DISTINCT
    hca_billto.account_number   bill_to_account,
    hz_billto.party_name        bill_to_customer_name,
    nvl(hca_shipto.account_number, lhca_shipto.account_number) ship_to_account,
    nvl(hz_shipto.party_name, lhz_shipto.party_name) ship_to_customer_name,
    nvl(hl.address1, lhl.address1) ship_to_address1,
    nvl(hl.country, lhl.country) ship_to_country,
    nvl(hl.state, lhl.state) state,
    nvl(hl.city, lhl.city) city,
    nvl(hl.postal_code, lhl.postal_code) zip,
    rbsa.name                   source,
    rctta.name                  transaction_type,
    ct.trx_number               invoice_number,
    rctla.line_number,
    rctla.line_type,
    rgds_gl.gl_date            gl_date,
    ct.term_due_date         due_date,
    trunc(ct.trx_date) invoice_date,
    	rt.name payment_terms,
	ct.purchase_order,
	case
		when rbsa.name = 'PROJECTS INVOICES' then
            ct.interface_header_attribute1
		when rbsa.name = 'OKS_CONTRACTS' then pnso.cognomen
	end project_num_or_sales_order,
	rctla.sales_order_date,
		case
		when rbsa.name = 'OKS_CONTRACTS'
		and rctta.name = 'Invoice-OKS' then
            rctla.sales_order
		when rbsa.name = 'PROJECTS INVOICES'
		and rctta.name = 'Projects Invoices'
		and ct.interface_header_context = 'PROJECTS INVOICES' then
            ct.interface_header_attribute1
		else
            ct.ct_reference
	end reference,
		(
	select
		segment1
	from
		bec_ods.mtl_system_items_b
	where
		inventory_item_id = rctla.inventory_item_id
	limit 1
    ) item,
	rctla.inventory_item_id,
		(
	select
			SYSTEM_ID
	from
			BEC_ETL_CTRL.ETLSOURCEAPPID
	where
			SOURCE_SYSTEM = 'EBS')|| '-' || rctla.inventory_item_id as ITEM_ID_KEY,
	rctla.description item_desc,
	rctla.uom_code uom,
	case
		when rctta.name = 'Manual Credit Memo' then
            rctla.quantity_credited
		else
            rctla.quantity_invoiced
	end billed_quantity,
	rctla.unit_selling_price unit_price,
		(
	select
		max(rctla.extended_amount) -- ,rctla.customer_trx_line_id,rgd1.cust_trx_line_gl_dist_id
	from
	bec_ods.ra_customer_trx_lines_all  t,
	bec_ods.ra_cust_trx_line_gl_dist_all  rgd1,
	bec_ods.ra_customer_trx_lines_all  rctla
	where
		t.customer_trx_id = ct.customer_trx_id
		and t.customer_trx_line_id = rgd1.customer_trx_line_id
		and rgd1.customer_trx_line_id = rctla.customer_trx_line_id
		and rgd.cust_trx_line_gl_dist_id = rgd1.cust_trx_line_gl_dist_id
		and rgd1.code_combination_id <> '1025'  
    group by rctla.customer_trx_line_id,rgd1.cust_trx_line_gl_dist_id		
    ) extended_amount,
    	case
			when rgd.code_combination_id = '1025' then tax_amount_db1.tax_amount
		else tax_amount_db2.tax_amount
	end as tax_amount,
		(
	select
		gcck.concatenated_segments
	from
    bec_ods.gl_code_combinations_kfv gcck,
    bec_ods.ra_cust_trx_line_gl_dist_all  rgd
	where
		gcck.code_combination_id = rgd.code_combination_id
		and rgd.account_class = 'UNEARN'
		and rgd.customer_trx_line_id = rctla.customer_trx_line_id
	group by
		gcck.concatenated_segments
	having
		SUM(rgd."percent") = 100
    ) unearned_account,
    	(
	select
		gcck.concatenated_segments
	from
	 bec_ods.ra_customer_trx_lines_all  tl,
	 bec_ods.ra_cust_trx_line_gl_dist_all  b,
	 bec_ods.gl_code_combinations_kfv gcck
	where
		tl.line_type = 'TAX'
		and tl.customer_trx_id = rctla.customer_trx_id
		and tl.link_to_cust_trx_line_id = rctla.customer_trx_line_id
		and tl.customer_trx_id = b.customer_trx_id
		and tl.customer_trx_line_id = b.customer_trx_line_id
		and b.code_combination_id = gcck.code_combination_id
		--            AND ROWNUM = 1
	limit 1
    ) tax_account,
    	(
	select
		distinct
            gcck.concatenated_segments
	from
	bec_ods.gl_code_combinations_kfv  gcck,
	bec_ods.ra_cust_trx_line_gl_dist_all  rgd
	where
		gcck.code_combination_id = rgd.code_combination_id
		and rgd.account_class = 'REC'
		and rgd.customer_trx_id = ct.customer_trx_id
		--            AND ROWNUM = 1
	limit 1
    ) receivables_account,
	ct.INVOICE_CURRENCY_CODE     INV_CURRENCY_CODE,
	ct.org_id,
	(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||ct.org_id as ORG_ID_KEY,
ct.customer_trx_id,
rctla.customer_trx_line_id,
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
    || '-' || nvl(ct.customer_trx_id, 0) 
    || '-' || nvl(rctla.customer_trx_line_id, 0)
	|| '-' || nvl(rgd.CUST_TRX_LINE_GL_DIST_ID, 0)
	as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
FROM
    (select account_number,cust_account_id,party_id from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y')               hca_billto,
    (select party_id,party_name from bec_ods.hz_parties where is_deleted_flg <> 'Y')                     hz_billto,
    (select cust_account_id,party_id,account_number from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y')               hca_shipto,
    (select cust_acct_site_id,party_site_id from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y')         hcasa_shipto,
    (select cust_acct_site_id,site_use_id from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y')          hcsua_shipto,
    (select party_id,party_name from bec_ods.hz_parties where is_deleted_flg <> 'Y')                     hz_shipto,
    (select cust_account_id,party_id,account_number from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y')               lhca_shipto,
    (select cust_acct_site_id,party_site_id from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y')         lhcasa_shipto,
    (select cust_acct_site_id,site_use_id from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y')          lhcsua_shipto,
    (select party_id,party_name from bec_ods.hz_parties where is_deleted_flg <> 'Y')                     lhz_shipto,
    (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y')                   lhl,
    (select location_id,party_site_id from bec_ods.hz_party_sites where is_deleted_flg <> 'Y')                 lhps,
    (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y')            ct,
    (select location_id,party_site_id from bec_ods.hz_party_sites where is_deleted_flg <> 'Y')                 hps,
    (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y')                   hl,
    (select batch_source_id,org_id,name from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y')           rbsa,
    (select cust_trx_type_id,org_id,name from bec_ods.ra_cust_trx_types_all where is_deleted_flg <> 'Y')          rctta,
    (select customer_trx_line_id,CUST_TRX_LINE_GL_DIST_ID,code_combination_id from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y')   rgd,
    	(
	select
		*
	from
		bec_ods.ra_terms_tl
	where
		language = 'US' and is_deleted_flg <> 'Y') rt,
	(select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') rctla,
	(
	select
			a.cognomen as cognomen,
			b.contract_number
	from
			(select * from bec_ods.okc_k_headers_tl where is_deleted_flg <> 'Y') a,
			(select * from bec_ods.okc_k_headers_all_b where is_deleted_flg <> 'Y') b
	where
			a.id = b.id
		and
		b.sts_code = 'ACTIVE') pnso
--    ra_customer_trx_partial_v      rctpv
		,
	(
	select
			MAX(nvl(t.quantity_invoiced, t.quantity_credited) * t.unit_selling_price) as tax_amount,
			t.customer_trx_id,
			rgd.customer_trx_line_id
	from
			(select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') t,
			(select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y') rgd
	where
			1 = 1
		and rgd.code_combination_id = '1025'
		and t.customer_trx_line_id = rgd.customer_trx_line_id
	group by
			t.customer_trx_id,
			rgd.customer_trx_line_id
            )tax_amount_db1,
	(
	select
			SUM(nvl(tl.extended_amount, 0)) as tax_amount,
--			tl.customer_trx_id,
			tl.link_to_cust_trx_line_id
	from
			(select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') tl
	where
			1 = 1
		and tl.line_type = 'TAX'
	group by
--			tl.customer_trx_id,
			tl.link_to_cust_trx_line_id
            ) tax_amount_db2,
            (select max(nvl(gl_date,'1900-01-01 12:00:00')) gl_date,
                rgds.customer_trx_line_id from (select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y') rgds
----    where  rctla.customer_trx_line_id = rgds.customer_trx_line_id 
    group by rgds.customer_trx_line_id )rgds_gl
WHERE
    ct.bill_to_customer_id = hca_billto.cust_account_id
    AND hca_billto.party_id = hz_billto.party_id
    AND ct.ship_to_customer_id = hca_shipto.cust_account_id (+)
    AND hca_shipto.party_id = hz_shipto.party_id (+)
    AND ct.ship_to_site_use_id = hcsua_shipto.site_use_id (+)
    AND hcsua_shipto.cust_acct_site_id = hcasa_shipto.cust_acct_site_id (+)
/*and hcasa_shipto.cust_Account_id=hca_shipto.cust_account_id*/
    AND hcasa_shipto.party_site_id = hps.party_site_id (+)
/*and hz_shipto.party_id=hps.party_id*/
    AND hps.location_id = hl.location_id (+)
/*- Line Ship to Start----*/
    AND rctla.ship_to_customer_id = lhca_shipto.cust_account_id (+)
    AND lhca_shipto.party_id = lhz_shipto.party_id (+)
    AND rctla.ship_to_site_use_id = lhcsua_shipto.site_use_id (+)
    AND lhcsua_shipto.cust_acct_site_id = lhcasa_shipto.cust_acct_site_id (+)
    AND lhcasa_shipto.party_site_id = lhps.party_site_id (+)
    AND lhps.location_id = lhl.location_id (+)
    AND hcasa_shipto.party_site_id = hps.party_site_id (+)
/*- Line Ship to End ----*/
    AND ct.batch_source_id = rbsa.batch_source_id
    AND ct.cust_trx_type_id = rctta.cust_trx_type_id
    AND ct.term_id = rt.term_id (+)
    AND rbsa.org_id = ct.org_id
    AND ct.org_id = rctta.org_id
    AND rctla.customer_trx_line_id = rgd.customer_trx_line_id
    AND ct.customer_trx_id = rctla.customer_trx_id
    AND rctla.line_type = 'LINE'
    and ct.interface_header_attribute1 = pnso.contract_number (+)
    	and rctla.customer_trx_line_id = tax_amount_db1.customer_trx_line_id (+)
	and rctla.customer_trx_line_id = tax_amount_db2.link_to_cust_trx_line_id (+)
	and rctla.customer_trx_line_id = rgds_gl.customer_trx_line_id(+)
) a
left outer join 
	/* Global Amounts */
      (
    select 
      * 
    from 
      bec_ods.GL_DAILY_RATES
    where 
      to_currency = 'USD' 
      and conversion_type = 'Corporate' and is_deleted_flg <> 'Y'
  ) DCR 
on a.INV_CURRENCY_CODE = DCR.from_currency
and a.gl_date = DCR.conversion_date 
)
;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_billing_history'
	and batch_name = 'ar';

commit;