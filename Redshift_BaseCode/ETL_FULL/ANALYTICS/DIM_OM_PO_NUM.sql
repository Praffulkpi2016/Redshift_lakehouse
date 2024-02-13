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

drop table if exists bec_dwh.DIM_OM_PO_NUM;

create table bec_dwh.DIM_OM_PO_NUM
	diststyle all sortkey(LINE_ID)
as
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
);
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