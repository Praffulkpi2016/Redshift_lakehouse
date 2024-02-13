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

delete from bec_dwh.DIM_SO_TRANSACTION_TYPES
where (nvl(TRANSACTION_TYPE_ID, 0),nvl(language, 'NA')) in (
select nvl(ods.TRANSACTION_TYPE_ID, 0)as TRANSACTION_TYPE_ID,nvl(ods.language, 'NA') as language from bec_dwh.DIM_SO_TRANSACTION_TYPES dw, bec_ods.OE_TRANSACTION_TYPES_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.TRANSACTION_TYPE_ID, 0) 
	|| '-' || nvl(ods.language, 'NA') 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_so_transaction_types' and batch_name = 'om')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_SO_TRANSACTION_TYPES
(
TRANSACTION_TYPE_ID,
language,
SOURCE_LANG,
NAME,
DESCRIPTION,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
REQUEST_ID
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
select
	TRANSACTION_TYPE_ID,
	language,
	SOURCE_LANG,
	NAME,
	DESCRIPTION,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	REQUEST_ID,
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
    || '-' || nvl(TRANSACTION_TYPE_ID, 0) 
	|| '-' || nvl(language, 'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.OE_TRANSACTION_TYPES_TL
 where (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
 where dw_table_name ='dim_so_transaction_types' and batch_name = 'om')
  )
 );

-- Soft delete

update bec_dwh.DIM_SO_TRANSACTION_TYPES set is_deleted_flg = 'Y'
where (nvl(TRANSACTION_TYPE_ID, 0),nvl(language, 'NA')) not in (
select nvl(ods.TRANSACTION_TYPE_ID, 0)as TRANSACTION_TYPE_ID,nvl(ods.language, 'NA') as language  
from bec_dwh.DIM_SO_TRANSACTION_TYPES dw, (select * from bec_ods.OE_TRANSACTION_TYPES_TL where is_deleted_flg <> 'Y') ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.TRANSACTION_TYPE_ID, 0) 
	|| '-' || nvl(ods.language, 'NA'));

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_so_transaction_types' and batch_name = 'om';

COMMIT;