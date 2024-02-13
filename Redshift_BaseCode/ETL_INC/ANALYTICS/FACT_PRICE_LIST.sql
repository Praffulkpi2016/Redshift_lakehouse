/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;
-- Delete Records

delete
from
	bec_dwh.FACT_PRICE_LIST
where
	(
nvl(LIST_HEADER_ID, 0),
nvl(LIST_LINE_ID, 0),
nvl(PRICING_ATTRIBUTE_ID, 0)
)
in
(
	select
		nvl(ods.LIST_HEADER_ID, 0) as LIST_HEADER_ID,
		nvl(ods.LIST_LINE_ID, 0) as LIST_LINE_ID,
		nvl(ods.PRICING_ATTRIBUTE_ID, 0) as PRICING_ATTRIBUTE_ID
	from
		bec_dwh.FACT_PRICE_LIST dw,
		(
SELECT 
QPLT.LIST_HEADER_ID,
SPLL.LIST_LINE_ID,
QPA.PRICING_ATTRIBUTE_ID
FROM 
bec_ods.QP_LIST_LINES  SPLL
LEFT OUTER JOIN bec_ods.QP_PRICING_ATTRIBUTES  QPA ON SPLL.LIST_LINE_ID   = QPA.LIST_LINE_ID
LEFT OUTER JOIN bec_ods.QP_LIST_HEADERS_B  SPL ON SPLL.LIST_HEADER_ID = SPL.LIST_HEADER_ID
LEFT OUTER JOIN bec_ods.QP_LIST_HEADERS_TL  QPLT ON SPLL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
AND SPL.LIST_HEADER_ID  = QPLT.LIST_HEADER_ID AND QPLT.LANGUAGE = 'US'
Where (
		 QPLT.kca_seq_date >= (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_price_list'
				and batch_name = 'om')
		or SPLL.kca_seq_date >= (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_price_list'
				and batch_name = 'om')
		or QPA.kca_seq_date >= (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_price_list'
				and batch_name = 'om')
		OR QPLT.IS_DELETED_FLG ='Y'	
		OR SPLL.IS_DELETED_FLG ='Y'	
		OR QPA.IS_DELETED_FLG ='Y'	
		OR SPL.IS_DELETED_FLG ='Y'		)
)ods
	where
		1 = 1
		and dw.dw_load_id =
(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ODS.LIST_HEADER_ID, 0)|| '-' || nvl(ODS.LIST_LINE_ID, 0)|| '-' || nvl(ODS.PRICING_ATTRIBUTE_ID, 0)
);

commit;

-- Insert Records

insert
	into
	bec_dwh.fact_price_list
(organization_id,
	organization_id_key,
	inventory_item_id,
	list_line_id,
	list_header_id,
	terms_id,
	price_list_name,
	description,
	currency,
	effectivity_date_from,
	product_context,
	PRODUCT_ATTR_ID,
	uom,
	value,
	application_method,
	start_date,
	orig_org_id,
	pricing_attribute_id,
	LIST_LINE_TYPE_CODE,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date)
(
SELECT 
 NVL(SPLL.ORGANIZATION_ID,90) as ORGANIZATION_ID
,(select system_id from bec_etl_ctrl.etlsourceappid
 where source_system = 'EBS')|| '-' || NVL(SPLL.ORGANIZATION_ID,90) as ORGANIZATION_ID_KEY
,SPLL.INVENTORY_ITEM_ID
,SPLL.LIST_LINE_ID
,SPLL.LIST_HEADER_ID
,SPL.TERMS_ID
,QPLT.NAME 						PRICE_LIST_NAME
,QPLT.DESCRIPTION
,SPL.CURRENCY_CODE				CURRENCY
,SPL.START_DATE_ACTIVE    		EFFECTIVITY_DATE_FROM
,QPA.PRODUCT_ATTRIBUTE_CONTEXT	PRODUCT_CONTEXT
,DECODE(QPA.product_attr_value,'ALL','-999999',QPA.product_attr_value) AS PRODUCT_ATTR_ID  --Added to fix UAT issue
,QPA.PRODUCT_UOM_CODE			UOM
,SPLL.OPERAND					VALUE
,SPLL.ARITHMETIC_OPERATOR 		APPLICATION_METHOD
,SPLL.START_DATE_ACTIVE 			START_DATE
,SPL.ORIG_ORG_ID
,QPA.PRICING_ATTRIBUTE_ID
,SPLL.LIST_LINE_TYPE_CODE
,'N' AS IS_DELETED_FLG
,(select system_id from bec_etl_ctrl.etlsourceappid
  where source_system = 'EBS') as source_app_id
,(select system_id from bec_etl_ctrl.etlsourceappid
 where source_system = 'EBS')
	   || '-' || nvl(QPLT.LIST_HEADER_ID, 0)|| '-' || nvl(SPLL.LIST_LINE_ID, 0)|| '-' || nvl(QPA.PRICING_ATTRIBUTE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM 
(select * from bec_ods.QP_LIST_LINES where is_deleted_flg <> 'Y') SPLL
LEFT OUTER JOIN (select * from bec_ods.QP_PRICING_ATTRIBUTES where is_deleted_flg <> 'Y') QPA ON SPLL.LIST_LINE_ID   = QPA.LIST_LINE_ID
LEFT OUTER JOIN (select * from bec_ods.QP_LIST_HEADERS_B where is_deleted_flg <> 'Y') SPL ON SPLL.LIST_HEADER_ID = SPL.LIST_HEADER_ID
LEFT OUTER JOIN (select * from bec_ods.QP_LIST_HEADERS_TL where is_deleted_flg <> 'Y') QPLT ON SPLL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
AND SPL.LIST_HEADER_ID  = QPLT.LIST_HEADER_ID AND QPLT.LANGUAGE = 'US'
Where (
		 QPLT.kca_seq_date >= (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_price_list'
				and batch_name = 'om')
		or SPLL.kca_seq_date >= (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_price_list'
				and batch_name = 'om')
		or QPA.kca_seq_date >= (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_price_list'
				and batch_name = 'om'))
);
commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_price_list'
	and batch_name = 'om';

commit;