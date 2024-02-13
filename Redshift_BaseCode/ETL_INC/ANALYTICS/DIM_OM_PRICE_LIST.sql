/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
--delete records

delete from bec_dwh.DIM_OM_PRICE_LIST
where (nvl(LIST_HEADER_ID, 0))
in 
(select nvl(ODS.LIST_HEADER_ID, 0) as LIST_HEADER_ID
from bec_dwh.DIM_OM_PRICE_LIST DW,
(
SELECT
SPL.LIST_HEADER_ID
FROM 
BEC_ODS.QP_LIST_HEADERS_B       SPL
,BEC_ODS.QP_LIST_HEADERS_TL      QPLT
WHERE  1=1     
AND SPL.LIST_HEADER_ID  = QPLT.LIST_HEADER_ID 
AND QPLT.LANGUAGE   = 'US'
AND (QPLT.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_price_list' and batch_name = 'om')
 
)
) ODS
WHERE 1=1
AND DW.DW_LOAD_ID = 
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(ods.LIST_HEADER_ID, 0)
);

commit;
-- Insert Records
insert into bec_dwh.DIM_OM_PRICE_LIST
(
list_header_id
,terms_id
,price_list_name
,description
,currency
,effectivity_date_from
,effectivity_date_to
,orig_org_id
,automatic_flag
,rounding_factor
,source_system_code
,global_flag
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
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
AND (QPLT.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_price_list' and batch_name = 'om')
 
)
);
commit;

-- Soft delete
update bec_dwh.DIM_OM_PRICE_LIST set is_deleted_flg = 'Y'
where (nvl(LIST_HEADER_ID, 0))
not in 
(select nvl(ODS.LIST_HEADER_ID, 0) as LIST_HEADER_ID
from bec_dwh.DIM_OM_PRICE_LIST DW,
(
SELECT
SPL.LIST_HEADER_ID
FROM 
(select * from BEC_ODS.QP_LIST_HEADERS_B  where is_deleted_flg <> 'Y')      SPL
,(select * from BEC_ODS.QP_LIST_HEADERS_TL  where is_deleted_flg <> 'Y')     QPLT
WHERE  1=1     
AND SPL.LIST_HEADER_ID  = QPLT.LIST_HEADER_ID 
AND QPLT.LANGUAGE   = 'US'
) ODS
WHERE 1=1
AND DW.DW_LOAD_ID = 
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(ods.LIST_HEADER_ID, 0)
);

commit;