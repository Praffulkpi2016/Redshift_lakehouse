/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
-- Delete Records
delete
from
	bec_dwh.DIM_OM_PO_NUM
where (nvl(LINE_ID, 0)) 
in 
(
select ods.LINE_ID 
from bec_dwh.DIM_OM_PO_NUM dw,
(
SELECT 
nvl(ODSS.LINE_ID,0) as LINE_ID
FROM BEC_ODS.OE_DROP_SHIP_SOURCES ODSS
	,BEC_ODS.PO_HEADERS_ALL POH
WHERE 1 = 1
AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID(+)
AND (ODSS.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_po_num' and batch_name = 'om')
OR
POH.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_po_num' and batch_name = 'om')
 
)
)ods 
where 1=1
and dw.dw_load_id =
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(ODS.LINE_ID, 0)
);
commit;

--Insert records
insert into bec_dwh.DIM_OM_PO_NUM
(
line_id
,po_header_id
,po_number
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
SELECT 
ODSS.LINE_ID, 
ODSS.PO_HEADER_ID, 
POH.SEGMENT1 PO_NUMBER,
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
||'-'|| nvl(ODSS.LINE_ID, 0) as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM BEC_ODS.OE_DROP_SHIP_SOURCES ODSS
	,BEC_ODS.PO_HEADERS_ALL POH
WHERE 1 = 1
AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID(+)
AND (ODSS.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_po_num' and batch_name = 'om')
OR
POH.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_om_po_num' and batch_name = 'om')
 
)
);
commit;

--Soft DELETE

update bec_dwh.DIM_OM_PO_NUM set is_deleted_flg = 'Y'
where (nvl(LINE_ID, 0)) 
not in 
(
select ods.LINE_ID 
from bec_dwh.DIM_OM_PO_NUM dw,
(
SELECT 
nvl(ODSS.LINE_ID,0) as LINE_ID
FROM (select * from BEC_ODS.OE_DROP_SHIP_SOURCES where is_deleted_flg <> 'Y') ODSS
	,(select * from BEC_ODS.PO_HEADERS_ALL where is_deleted_flg <> 'Y') POH
WHERE 1 = 1
AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID(+)
)ods 
where 1=1
and dw.dw_load_id =
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(ODS.LINE_ID, 0)
);
commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_om_po_num'
	and batch_name = 'om';

commit;