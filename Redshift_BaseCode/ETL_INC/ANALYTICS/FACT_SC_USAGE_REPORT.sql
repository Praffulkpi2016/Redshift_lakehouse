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
	Truncate table bec_dwh.FACT_SC_USAGE_REPORT;
	
	Insert Into  bec_dwh.FACT_SC_USAGE_REPORT
	(  
		select 
		contract_number,
		hdr_status,
		hdr_start_date,
		hdr_end_date,
		BILLING_TYPE,
		KWH,
		line_number,
		Line_Type,
		line_status,
		ship_to_site_use_id,
		bill_to_site_use_id,
		service_name,
		Currency_Code,
		Contract_Line_Start_Date,
		Contract_Line_End_Date,
		Price_List_Name,
		Start_Date_Active,
		End_Date_Active,       
		product_uom_code,
		Unit_Price,
		invoice_number,
		invoice_date,
		invoice_line_number,
		invoice_line_amount,
		bill_from_date,
		bill_to_date,
		net_reading_qty ,
		TMO_PERC,
		percentage_factor,
		gl_date,
		cust_trx_line_gl_dist_id,
		id,
		org_id,
		code_combination_id,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || ship_to_site_use_id as ship_to_site_use_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || bill_to_site_use_id as bill_to_site_use_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || cust_trx_line_gl_dist_id as cust_trx_line_gl_dist_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || id as id_KEY, 
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || org_id as org_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || code_combination_id as code_combination_id_KEY,
		cast(NVL(invoice_line_amount,0) * NVL(conversion_rate,1) as decimal(18,2)) GBL_INVOICE_LINE_AMOUNT,
 		'N' AS IS_DELETED_FLG,	 
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		)                   AS source_app_id ,
		
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		) 
		
		||'-'||nvl(contract_number,'NA')
		||'-'||nvl(id,'NA') 
		||'-'||nvl(CUST_TRX_LINE_GL_DIST_ID,0)
		||'-'||nvl(start_date_active,'1900-01-01 00:00:00')
		||'-'||nvl(end_date_active,'1900-01-01 00:00:00')
		AS dw_load_id, 
		getdate()           AS dw_insert_date,
		getdate()           AS dw_update_date
		from
		(  with dist as 
			(
				SELECT
				dist.customer_trx_line_id,
				dist.customer_trx_id,
				MAX(dist.cust_trx_line_gl_dist_id)       cust_trx_line_gl_dist_id,
				SUM(nvl(dist.amount,0))                         amount,
				dist.gl_date                        gl_date,
				MAX(dist.event_id)                       event_id,
				MAX(dist.last_update_date)               last_update_date,
				SUM(nvl(dist.acctd_amount,0) )                   acctd_amount,
				dist.account_class,
				dist.code_combination_id				
				FROM
				(select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg<>'Y') dist
				WHERE
				upper(dist.account_class) = 'REV'
				AND AMOUNT IS NOT NULL
				GROUP BY
				dist.gl_date,
				dist.customer_trx_line_id,
				dist.customer_trx_id,
				dist.account_class,
				dist.code_combination_id
			),         
			billing_details as (
				select 
				rctl.interface_line_attribute1 contract_number,
				bcl.cle_id id,
				rctl.CONTRACT_LINE_ID,
				dist.customer_trx_line_id,
				rct.trx_number,
				rct.trx_date,
				rctl.line_number,
				rctl.INTERFACE_LINE_ATTRIBUTE4,
				rctl.INTERFACE_LINE_ATTRIBUTE5,
				rctl.quantity_invoiced QUANTITY,
				rctl.INTERFACE_LINE_ATTRIBUTE11,
				rctl.INTERFACE_LINE_ATTRIBUTE12,
				rctl.revenue_amount,
				dist.customer_trx_id,
				dist.cust_trx_line_gl_dist_id,
				dist.amount,
				dist.gl_date ,
				dist.event_id,
				dist.last_update_date,
				dist.acctd_amount,
				dist.account_class,
				dist.code_combination_id,
				rct.invoice_currency_code
				from
				(select * from bec_ods.oks_bill_cont_lines   where is_deleted_flg<>'Y')        bcl,
				(select * from bec_ods.oks_bill_transactions where is_deleted_flg<>'Y')        btn,
				(select * from bec_ods.oks_bill_txn_lines     where is_deleted_flg<>'Y')       btl,
				(select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg<>'Y')    rctl,
				(select * from bec_ods.ra_customer_trx_all      where is_deleted_flg<>'Y')     rct,
				dist 
				where btn.id = bcl.btn_id
				AND btl.btn_id = btn.id
				AND btl.bill_instance_number IS NOT NULL
				AND btl.bill_instance_number = rctl.interface_line_attribute3
				AND rctl.interface_line_context = 'OKS CONTRACTS'
				AND rct.customer_trx_id = rctl.customer_trx_id
				and 	dist.customer_trx_line_id = rctl.customer_trx_line_id
				and dist.account_class = 'REV'
			)
			SELECT  DISTINCT        
			OKH.contract_number,
			okh.sts_code hdr_status,
			okh.start_date hdr_start_date,
			okh.end_date hdr_end_date,
			CII.BILLING_TYPE,
			CII.KWH,
			oal.line_number,
			oll.lse_name Line_Type,
			oal.sts_code line_status,
			oal.ship_to_site_use_id,
			oal.bill_to_site_use_id,
			old1.service_name,
			oal.currency_code   Currency_Code,
			trunc(oal.start_date) Contract_Line_Start_Date,
			trunc(oal.end_date) Contract_Line_End_Date,
			qpl.name  Price_List_Name,
			trunc(qll.start_date_active) Start_Date_Active,
			trunc(qll.end_date_active) End_Date_Active,       
			qll.product_uom_code,
			qll.operand Unit_Price,
			inv.trx_number invoice_number,
			inv.trx_date invoice_date,
			inv.line_number invoice_line_number,
			inv.revenue_amount invoice_line_amount,
			inv.interface_line_attribute4 ::timestamp                            bill_from_date,
			inv.interface_line_attribute5  ::timestamp                           bill_to_date,
			inv.QUANTITY net_reading_qty ,
			inv.INTERFACE_LINE_ATTRIBUTE11 TMO_PERC,
			inv.INTERFACE_LINE_ATTRIBUTE12 percentage_factor,
			inv.gl_date,
			inv.cust_trx_line_gl_dist_id,
			oal.id,
			OKH.org_id,
			inv.code_combination_id,
			DCR.conversion_rate			
			FROM  (select * from bec_ods.okc_k_headers_all_b where is_deleted_flg<>'Y')okh,
			(select * from bec_ods.OKC_K_LINES_B  where is_deleted_flg<>'Y')    oal,		
			(SELECT 
				CLEB.ID ID
				, CLEB.CHR_ID CHR_ID
				, CLEB.LINE_NUMBER LINE_NUMBER
				, CLEB.STS_CODE STS_CODE
				, CLEB.LSE_ID
				, LSET.NAME LSE_NAME
				, CLEB.DNZ_CHR_ID DNZ_CHR_ID
				from
				(select * from bec_ods.OKC_LINE_STYLES_TL where is_deleted_flg<>'Y') LSET
				, (select * from bec_ods.OKC_K_LINES_B where is_deleted_flg<>'Y') CLEB
				, (select * from bec_ods.OKC_K_LINES_TL where is_deleted_flg<>'Y') CLET
				WHERE CLEB.ID = CLET.ID
				AND LSET.ID = CLEB.LSE_ID
			AND LSET.LANGUAGE = CLET.language
						)  oll,
			(select * from bec_ods.qp_pricelists_lov_v where is_deleted_flg<>'Y')  qpl,
			(select * from bec_ods.oks_line_details_v  where is_deleted_flg<>'Y')  old1,
			(select * from bec_ods.qp_list_lines_v     where is_deleted_flg<>'Y') qll,
			(select * from bec_ods.fnd_user  where is_deleted_flg<>'Y')  fu,
			(select * from bec_ods.per_all_people_f  where is_deleted_flg<>'Y') ppf,
			billing_details inv,
			(select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y'
			and to_currency = 'USD'
			and conversion_type = 'Corporate') DCR,
			--added tables
			(select okl1.dnz_chr_id,
			        okl1.cle_id,
			        CI.ATTRIBUTE4 BILLING_TYPE,
                    SUM(  (CI.ATTRIBUTE5 :: numeric(28,10) )  ) KWH
                    from
			        (select * from bec_ods.okc_k_lines_b where is_deleted_flg<>'Y') okl1,
                    (select * from bec_ods.okc_k_items where is_deleted_flg<>'Y') oki1,
                    (select * from bec_ods.csi_counter_associations where is_deleted_flg<>'Y') ca,
                    (select * from bec_ods.csi_item_instances where is_deleted_flg<>'Y') ci
                    where 
                    --AND okl1.dnz_chr_id = okh.id  -- added on 100721
                     okl1.cle_id IS NOT NULL
                    --AND okl1.dnz_chr_id = oal.chr_id
                    --AND okl1.cle_id = oal.id
                    AND okl1.dnz_chr_id = oki1.dnz_chr_id
                    AND  okl1.id =oki1.cle_id
                    AND oki1.jtot_object1_code = 'OKX_COUNTER'
                    AND okl1.STS_CODE <> 'TERMINATED'
                    AND  ca.counter_id = oki1.object1_id1
                    AND ci.instance_id = ca.source_object_id
                    group by okl1.dnz_chr_id,
			        okl1.cle_id,
			        okl1.cle_id,
			        CI.ATTRIBUTE4 
			) cii
			WHERE  okh.ID = oal.chr_id
			AND  oal.id = oll.id 
			AND  oal.cle_id IS NULL
			AND  qpl.price_list_id = oal.price_list_id
			AND  old1.contract_id = oll.chr_id
			AND  old1.line_id    = oal.id
			AND  oal.price_list_id = qll.list_header_id
			AND  old1.object1_id1 = qll.PRODUCT_ATTR_VALUE
		    AND oll.lse_name = 'Usage' 
		    AND cii.dnz_chr_id = okh.id
            AND cii.dnz_chr_id = oal.chr_id
            AND cii.cle_id = oal.id
			and  qll.last_updated_by = fu.user_id
			and  fu.employee_id = ppf.person_id
			and  qll.last_update_date between ppf.effective_start_date and ppf.effective_end_date
			and  okh.sts_code NOT IN ( 'TERMINATED', 'CANCELLED')
			and  oal.sts_code <> 'TERMINATED'   
			and oal.id = inv.id(+)
			and inv.interface_line_attribute4 ::timestamp between qll.start_date_active and 
			qll.end_date_active
			--AND okh.contract_number = 'UBR001.0'
			and inv.invoice_currency_code = DCR.from_currency(+)
			and inv.trx_date = DCR.conversion_date(+)
		)
	); 
end; 

UPDATE bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name  = 'fact_sc_usage_report'
and batch_name = 'sc';

commit;					