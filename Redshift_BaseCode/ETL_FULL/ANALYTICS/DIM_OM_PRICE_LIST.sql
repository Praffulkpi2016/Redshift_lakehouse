/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.DIM_OM_PRICE_LIST;

create table bec_dwh.DIM_OM_PRICE_LIST
	diststyle all sortkey(LIST_HEADER_ID)
as
(
SELECT
 SPL.LIST_HEADER_ID
,SPL.TERMS_ID
,QPLT.NAME 						PRICE_LIST_NAME
,QPLT.DESCRIPTION
,SPL.CURRENCY_CODE				CURRENCY
,SPL.START_DATE_ACTIVE    		EFFECTIVITY_DATE_FROM
,SPL.END_DATE_ACTIVE 			EFFECTIVITY_DATE_TO
,SPL.ORIG_ORG_ID 
,SPL.AUTOMATIC_FLAG
,SPL.ROUNDING_FACTOR
,SPL.SOURCE_SYSTEM_CODE
,SPL.GLOBAL_FLAG,
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
)
||'-'|| nvl(SPL.LIST_HEADER_ID, 0)  as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM BEC_ODS.QP_LIST_HEADERS_B       SPL
,BEC_ODS.QP_LIST_HEADERS_TL      QPLT
WHERE  1=1     
AND SPL.LIST_HEADER_ID  = QPLT.LIST_HEADER_ID 
AND QPLT.LANGUAGE   = 'US'
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_om_price_list'
	and batch_name = 'om';

commit;