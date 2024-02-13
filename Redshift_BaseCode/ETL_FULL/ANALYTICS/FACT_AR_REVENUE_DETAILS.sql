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

drop table if exists bec_dwh.FACT_AR_REVENUE_DETAILS;

create table bec_dwh.FACT_AR_REVENUE_DETAILS diststyle all sortkey(customer_trx_line_id)
as
(
select  
ORG_ID
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||org_id as ORG_ID_KEY
,INV_CM
,BILL_TO_ACCOUNT
,BILLTO_CUSTOMER_NAME
,SHIP_TO_ACCOUNT
,SHIP_TO_CUSTOMER_NAME
,BLOOM_SITEID
,SHIP_TO_CITY
,STATE
,ZIP
,country
,TRANSACTION_SOURCE
,TRANSACTION_TYPE
,INVOICE_NUM
,INVOICE_DATE
,LINE_NUM
,CUSTOMER_TRX_ID
,CUSTOMER_TRX_LINE_ID
,REFERENCE
,ITEM
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||item as ITEM_ID_KEY
,ITEM_DESC
,SERVICE_START_DATE
,SERVICE_END_DATE
,CURRENCY
,ORIGINAL_INVOICE_AMOUNT
,INVOICE_LINE_AMOUNT
,DEFERRED_ACCOUNT
,REVENUE_ACCOUNT
,(NVL(INVOICE_LINE_AMOUNT,0)::decimal(18,2) * NVL(DCR.conversion_rate,1)) GBL_INVOICE_LINE_AMOUNT
,(NVL(ORIGINAL_INVOICE_AMOUNT,0)::decimal(18,2) * NVL(DCR.conversion_rate,1)) GBL_ORIGINAL_INVOICE_AMOUNT
,'N' AS IS_DELETED_FLG
,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id
,	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')
		|| '-' || nvl(CUSTOMER_TRX_LINE_ID, 0)
 as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from 
((
select  rcta.org_id,
        'INV' INV_CM, hca_billto.account_number bill_to_account,
        hp_billto.party_name billto_customer_name,
        NVL(hca_shipto_l.account_number, hca_shipto.account_number) ship_to_account,
        NVL(hp_shipto_l.party_name, hp_shipto.party_name) ship_to_customer_name,
        NVL(hl_l.ADDRESS1, hl.ADDRESS1) bloom_SiteId,
        NVL(hl_l.city, hl.city) ship_to_city,
        NVL(hl_l.state, hl.state) state,
        NVL(hl_l.postal_code, hl.postal_code) ZIP,
		NVL(hl_l.country, hl.country) country,
        rbsa.name Transaction_source,
        rctta.name transaction_type,
        rcta.trx_number invoice_num,
        rcta.trx_date invoice_date,
		--rgd.gl_date,
        rctla.line_number line_num,
        rcta.customer_trx_id,
        rctla.CUSTOMER_TRX_LINE_ID,
        case
         when rbsa.name='OKS_CONTRACTS' and rctta.name='Invoice-OKS' then
             rctla.sales_order
        else
             rcta.ct_reference
        end Reference,
        (select segment1
                 from (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')  mtl_system_items_b
               where inventory_item_id=rctla.inventory_item_id 
--               and rownum=1
               limit 1
               )
			   Item,
                 rctla.description item_desc,
                 rctla.rule_start_date service_start_date,
        case
          when  RCTLA.rule_END_date is not null then
              RCTLA.rule_END_date
        else
        add_months(RCTLA.rule_start_date,
			RCTLA.accounting_rule_duration::integer)-1
        end
        Service_end_date,
        rcta.invoice_currency_code currency,
        (select sum(quantity_invoiced*unit_selling_price)  
           from (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') ra_customer_trx_lines_all 
          where customer_trx_id=rcta.customer_trx_id and line_type <> 'TAX')  
		  original_invoice_amount,
              /* sum(rctla.revenue_amount) Invoice_Line_Amount,*/
        (select sum(quantity_invoiced*unit_selling_price)
           from (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') ra_customer_trx_lines_all
          where customer_trx_id=rcta.customer_trx_id and customer_trx_line_id=rctla.customer_trx_line_id
            and line_type <> 'TAX')  
			invoice_line_amount,
        (SELECT DISTINCT gcck.concatenated_segments
           FROM (select * from bec_ods.gl_code_combinations_kfv where is_deleted_flg <> 'Y')       gcck,
                (select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y') rgd
          WHERE  gcck.code_combination_id = rgd.code_combination_id
            AND rgd.account_class = 'UNEARN'                    
            AND rgd.customer_trx_line_id = rctla.customer_trx_line_id
          --  AND ROWNUM = 1
         group by gcck.concatenated_segments
         limit 1
        ) 
		deferred_account,
        (SELECT DISTINCT gcck.concatenated_segments
            FROM (select * from bec_ods.gl_code_combinations_kfv where is_deleted_flg <> 'Y') gcck,
                 (select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y')   rgd
           WHERE gcck.code_combination_id = rgd.code_combination_id
             AND rgd.account_class = 'REV'
             AND rgd.customer_trx_line_id = rctla.customer_trx_line_id
--           AND ROWNUM = 1
            group by gcck.concatenated_segments
            having sum(rgd."percent") <> 0 and sum(rgd.amount) <> 0
        ) 
		revenue_account
        from  
            (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') rcta,        
            (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y')  hca_billto,
            (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y')  hp_billto,            
            (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y')  hca_shipto,
            (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y')  hp_shipto,
            (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y')  hcasa_shipto,
            (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y')  hcsua_shipto,
            (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y')  hps,    
            (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y')  hl,            
            (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y')  hca_shipto_l,
            (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y')  hp_shipto_l,
            (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y')  hcasa_shipto_l,
            (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsua_shipto_l, 
            (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps_l,            
            (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl_l,
            (select * from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y') rbsa,
            (select * from bec_ods.ra_cust_trx_types_all where is_deleted_flg <> 'Y') rctta,
            (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') rctla
        where rcta.bill_to_customer_id=hca_billto.cust_account_id
         and hca_billto.party_id=hp_billto.party_id
         and rcta.ship_to_customer_id=hca_shipto.cust_account_id(+)
         and hca_shipto.party_id=hp_shipto.party_id(+)
         and rcta.ship_to_site_use_id=hcsua_shipto.site_use_id(+)
         and hcsua_shipto.cust_acct_site_id=hcasa_shipto.cust_acct_site_id(+)
         --and hcasa_shipto.cust_Account_id(+)=hca_shipto.cust_account_id
         and hcasa_shipto.party_site_id=hps.party_site_id(+)
         --and hp_shipto.party_id=hps.party_id(+)
         and hps.location_id=hl.location_id(+)
         and rctla.ship_to_customer_id=hca_shipto_l.cust_account_id(+)
         and hca_shipto_l.party_id=hp_shipto_l.party_id(+)
         and rctla.ship_to_site_use_id=hcsua_shipto_l.site_use_id(+)
         and hcsua_shipto_l.cust_acct_site_id=hcasa_shipto_l.cust_acct_site_id(+)
         --and hcasa_shipto.cust_Account_id(+)=hca_shipto.cust_account_id
         and hcasa_shipto_l.party_site_id=hps_l.party_site_id(+)
         --and hp_shipto.party_id=hps.party_id(+)
         and hps_l.location_id=hl_l.location_id(+)         
         and rcta.batch_source_id=rbsa.batch_source_id
         and rctta.cust_trx_type_id=rcta.CUST_TRX_TYPE_ID
         and rctla.customer_trx_id=rcta.customer_trx_id
         and rcta.INVOICING_RULE_ID = -2
         and rctta.TYPE <> 'CM'
        group by hca_billto.account_number,
                 hp_billto.party_name,
                NVL(hca_shipto_l.account_number, hca_shipto.account_number) ,
                NVL(hp_shipto_l.party_name, hp_shipto.party_name) ,
                NVL(hl_l.ADDRESS1, hl.ADDRESS1) ,
                NVL(hl_l.city, hl.city) ,
                NVL(hl_l.state, hl.state) ,
                NVL(hl_l.postal_code, hl.postal_code) ,
				NVL(hl_l.country, hl.country) ,
                 rbsa.name,
                 rctta.name,
                 rcta.trx_number ,
                 rcta.trx_date,
                --rgd.gl_date,
                 rctla.sales_order,
                  rcta.ct_reference,
                  rctla.description,
                  rctla.rule_start_date,
                  rctla.rule_end_date,
                  --gp.period_name,
                  rctla.inventory_item_id,
                  rcta.customer_trx_id ,
                  rctla.customer_trx_line_id,
                  rcta.invoice_currency_code,
                  rctla.line_number,
                  rcta.ORG_ID,
                  accounting_rule_duration 
UNION ALL
        select rcta.org_id,'CM', hca_billto.account_number bill_to_account,
                hp_billto.party_name billto_customer_name,
                NVL(hca_shipto_l.account_number, hca_shipto.account_number) ship_to_account,
                NVL(hp_shipto_l.party_name, hp_shipto.party_name) ship_to_customer_name,
                NVL(hl_l.ADDRESS1, hl.ADDRESS1) bloom_SiteId,
                NVL(hl_l.city, hl.city) ship_to_city,
                NVL(hl_l.state, hl.state) state,
                NVL(hl_l.postal_code, hl.postal_code) ZIP,
				NVL(hl_l.country, hl.country) country,				
                rbsa.name Transaction_source,
                rctta.name transaction_type,
                rcta.trx_number invoice_num,
                rcta.trx_date invoice_date,
				--rgd.gl_date,
                rctla.line_number line_num,
                rcta.customer_trx_id,
                rctla.CUSTOMER_TRX_LINE_ID,
                case
                 when rbsa.name='OKS_CONTRACTS' and rctta.name='Invoice-OKS' then
                     rctla.sales_order
                else
                     rcta.ct_reference
                end Reference,
                 (select segment1
                         from (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') mtl_system_items_b
                       where inventory_item_id=rctla.inventory_item_id 
--                       and rownum=1
                       limit 1)Item,
                         rctla.description item_desc,
                         rctla.rule_start_date service_start_date,
                case
                  when  RCTLA.rule_END_date is not null then
                      RCTLA.rule_END_date
                else
--                 (select add_months(rule_start_date,accounting_rule_duration)-1 from dual )
                 add_months(RCTLA.rule_start_date,
			RCTLA.accounting_rule_duration::integer)-1
                end
                Service_end_date,
                         --(select add_months(rule_start_date,accounting_rule_duration) from dual ) service_end_date,
                rcta.invoice_currency_code currency,
                (select sum(quantity_invoiced*unit_selling_price)  
                   from (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') ra_customer_trx_lines_all 
                  where customer_trx_id=rcta.customer_trx_id and line_type <> 'TAX')  original_invoice_amount,
                      /* sum(rctla.revenue_amount) Invoice_Line_Amount,*/
                (select sum(quantity_invoiced*unit_selling_price)
                   from (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') ra_customer_trx_lines_all
                  where customer_trx_id=rcta.customer_trx_id and customer_trx_line_id=rctla.customer_trx_line_id
                    and line_type <> 'TAX')  invoice_line_amount,
                (SELECT DISTINCT gcck.concatenated_segments
                   FROM (select * from bec_ods.gl_code_combinations_kfv where is_deleted_flg <> 'Y')     gcck,
                        (select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y')   rgd
                  WHERE  gcck.code_combination_id = rgd.code_combination_id
                    AND rgd.account_class = 'UNEARN'                    
                    AND rgd.customer_trx_line_id = rctla.customer_trx_line_id
--                    AND ROWNUM = 1
                 group by gcck.concatenated_segments
                 limit 1
                ) deferred_account,
                ( SELECT DISTINCT gcck.concatenated_segments
                    FROM (select * from bec_ods.gl_code_combinations_kfv where is_deleted_flg <> 'Y')      gcck,
                         (select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y')    rgd
                   WHERE gcck.code_combination_id = rgd.code_combination_id
                     AND rgd.account_class = 'REV'
                     AND rgd.customer_trx_line_id = rctla.customer_trx_line_id
        --           AND ROWNUM = 1
                    group by gcck.concatenated_segments
                    having sum(rgd."percent") <> 0 and sum(rgd.amount) <> 0
                ) revenue_account
                from  
                    (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') rcta,        
                    (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') hca_billto,
                    (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') hp_billto,            
                    (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') hca_shipto,
                    (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') hp_shipto,
                    (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') hcasa_shipto,
                    (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsua_shipto,
                    (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps,    
                    (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl,            
                    (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') hca_shipto_l,
                    (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') hp_shipto_l,
                    (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') hcasa_shipto_l,
                    (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsua_shipto_l, 
                    (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps_l,            
                    (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl_l,
                    (select * from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y') rbsa,
                    (select * from bec_ods.ra_cust_trx_types_all where is_deleted_flg <> 'Y') rctta,
                    (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') rctla
                where rcta.bill_to_customer_id=hca_billto.cust_account_id
                 and hca_billto.party_id=hp_billto.party_id
                 and rcta.ship_to_customer_id=hca_shipto.cust_account_id(+)
                 and hca_shipto.party_id=hp_shipto.party_id(+)
                 and rcta.ship_to_site_use_id=hcsua_shipto.site_use_id(+)
                 and hcsua_shipto.cust_acct_site_id=hcasa_shipto.cust_acct_site_id(+)
                 --and hcasa_shipto.cust_Account_id(+)=hca_shipto.cust_account_id
                 and hcasa_shipto.party_site_id=hps.party_site_id(+)
                 --and hp_shipto.party_id=hps.party_id(+)
                 and hps.location_id=hl.location_id(+)                 
                 and rctla.ship_to_customer_id=hca_shipto_l.cust_account_id(+)
                 and hca_shipto_l.party_id=hp_shipto_l.party_id(+)
                 and rctla.ship_to_site_use_id=hcsua_shipto_l.site_use_id(+)
                 and hcsua_shipto_l.cust_acct_site_id=hcasa_shipto_l.cust_acct_site_id(+)
                 --and hcasa_shipto.cust_Account_id(+)=hca_shipto.cust_account_id
                 and hcasa_shipto_l.party_site_id=hps_l.party_site_id(+)
                 --and hp_shipto.party_id=hps.party_id(+)
                 and hps_l.location_id=hl_l.location_id(+)                         
                 and rcta.batch_source_id=rbsa.batch_source_id
                 and rctta.cust_trx_type_id=rcta.CUST_TRX_TYPE_ID
                 and rctla.customer_trx_id=rcta.customer_trx_id
                 and rctta.TYPE = 'CM'
                 and rctla.LINE_TYPE = 'LINE'
				 and rcta.INVOICING_RULE_ID = -2
                 and exists (select 1
                               from (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') rct_c, 
                                    (select * from bec_ods.ar_receivable_applications_all where is_deleted_flg <> 'Y') ara_c
                              where rct_c.customer_trx_id = ara_c.applied_customer_trx_id
                                and ara_c.customer_trx_id = rcta.customer_trx_id
                                and rct_c.INVOICING_RULE_ID = -2
                             )
                group by hca_billto.account_number,
                         hp_billto.party_name,
                         NVL(hca_shipto_l.account_number, hca_shipto.account_number) ,
                         NVL(hp_shipto_l.party_name, hp_shipto.party_name) ,
                         NVL(hl_l.ADDRESS1, hl.ADDRESS1) ,
                         NVL(hl_l.city, hl.city) ,
                         NVL(hl_l.state, hl.state) ,
                         NVL(hl_l.postal_code, hl.postal_code) ,
						 NVL(hl_l.country, hl.country) ,
                         rbsa.name,
                         rctta.name,
                         rcta.trx_number ,
                         rcta.trx_date,
                       -- rgd.gl_date,
                         rctla.sales_order,
                          rcta.ct_reference,
                          rctla.description,
                          rctla.rule_start_date,
                          rctla.rule_end_date,
                          --gp.period_name,
                          rctla.inventory_item_id,
                          rcta.customer_trx_id ,
                          rctla.customer_trx_line_id,
                          rcta.invoice_currency_code,
                          rctla.line_number,
                          rcta.ORG_ID,
                          accounting_rule_duration
                          ) a
LEFT OUTER JOIN
 (select * from bec_ods.GL_DAILY_RATES
where is_deleted_flg<>'Y' and to_currency = 'USD' 
and conversion_type = 'Corporate' ) DCR
on a.CURRENCY = DCR.from_currency
and DCR.conversion_date = a.invoice_date 
)	
);				  
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_revenue_details'
	and batch_name = 'ar';

commit;