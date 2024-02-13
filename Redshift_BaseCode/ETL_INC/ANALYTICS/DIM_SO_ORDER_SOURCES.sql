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

delete from bec_dwh.DIM_SO_ORDER_SOURCES
where nvl(ORDER_SOURCE_ID,0) in (
select nvl(ods.ORDER_SOURCE_ID,0) as ORDER_SOURCE_ID from bec_dwh.DIM_SO_ORDER_SOURCES dw, bec_ods.OE_ORDER_SOURCES ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.ORDER_SOURCE_ID, 0)
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_so_order_sources' and batch_name = 'om')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_SO_ORDER_SOURCES
(
ORDER_SOURCE_ID,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
NAME,
DESCRIPTION,
ENABLED_FLAG,
CREATE_CUSTOMERS_FLAG,
USE_IDS_FLAG,
AIA_ENABLED_FLAG,
ZD_EDITION_NAME
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
select
	ORDER_SOURCE_ID,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	NAME,
	DESCRIPTION,
	ENABLED_FLAG,
	CREATE_CUSTOMERS_FLAG,
	USE_IDS_FLAG,
	AIA_ENABLED_FLAG,
	ZD_EDITION_NAME,
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
    || '-' || nvl(ORDER_SOURCE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.OE_ORDER_SOURCES
 where (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
 where dw_table_name ='dim_so_order_sources' and batch_name = 'om')
  )
 );

-- Soft delete

update bec_dwh.DIM_SO_ORDER_SOURCES
set is_deleted_flg = 'Y'
where nvl(ORDER_SOURCE_ID,0) not in (
select nvl(ods.ORDER_SOURCE_ID,0) as ORDER_SOURCE_ID from bec_dwh.DIM_SO_ORDER_SOURCES dw, (select * from bec_ods.OE_ORDER_SOURCES where is_deleted_flg <> 'Y') ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.ORDER_SOURCE_ID, 0)
);


commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_so_order_sources' and batch_name = 'om';

COMMIT;