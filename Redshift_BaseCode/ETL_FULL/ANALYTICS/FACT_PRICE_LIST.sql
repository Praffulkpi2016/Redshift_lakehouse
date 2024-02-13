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

drop table if exists bec_dwh.FACT_PRICE_LIST;

create table bec_dwh.FACT_PRICE_LIST diststyle all sortkey(LIST_LINE_ID)
as 
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
(select * from bec_ods.QP_LIST_LINES where is_deleted_flg<>'Y') SPLL
LEFT OUTER JOIN (select * from bec_ods.QP_PRICING_ATTRIBUTES where is_deleted_flg<>'Y') QPA ON SPLL.LIST_LINE_ID   = QPA.LIST_LINE_ID
LEFT OUTER JOIN (select * from bec_ods.QP_LIST_HEADERS_B where is_deleted_flg<>'Y') SPL ON SPLL.LIST_HEADER_ID = SPL.LIST_HEADER_ID
LEFT OUTER JOIN (select * from bec_ods.QP_LIST_HEADERS_TL where is_deleted_flg<>'Y') QPLT ON SPLL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
AND SPL.LIST_HEADER_ID  = QPLT.LIST_HEADER_ID AND QPLT.LANGUAGE = 'US'
);
end;

commit;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_price_list'
	and batch_name = 'om';

commit;