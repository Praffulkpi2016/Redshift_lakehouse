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

delete from bec_dwh.DIM_QP_LIST_HEADERS
where (nvl(LIST_HEADER_ID, 0),nvl(language, 'NA')) in (
select nvl(ods.LIST_HEADER_ID, 0) as LIST_HEADER_ID,nvl(ods.language, 'NA')as language from bec_dwh.DIM_QP_LIST_HEADERS dw, bec_ods.QP_LIST_HEADERS_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.LIST_HEADER_ID, 0) 
	|| '-' || nvl(ods.language, 'NA') 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_qp_list_headers' and batch_name = 'om') 					 
					 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_QP_LIST_HEADERS
(
LIST_HEADER_ID,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
language,
SOURCE_LANG,
NAME,
DESCRIPTION,
VERSION_NO
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
select
	LIST_HEADER_ID,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	language,
	SOURCE_LANG,
	NAME,
	DESCRIPTION,
	VERSION_NO,
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
    || '-'|| nvl(LIST_HEADER_ID, 0) 
	|| '-'|| nvl(language,'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.QP_LIST_HEADERS_TL
 where (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
 where dw_table_name ='dim_qp_list_headers' and batch_name = 'om')
 					 )
 );

-- Soft delete

update bec_dwh.DIM_QP_LIST_HEADERS set is_deleted_flg = 'Y'
where (nvl(LIST_HEADER_ID, 0),nvl(language, 'NA')) not in (
select nvl(ods.LIST_HEADER_ID, 0) as LIST_HEADER_ID,nvl(ods.language, 'NA')as language from bec_dwh.DIM_QP_LIST_HEADERS dw, (select * from bec_ods.QP_LIST_HEADERS_TL where is_deleted_flg <> 'Y') ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.LIST_HEADER_ID, 0) 
	|| '-' || nvl(ods.language, 'NA'));

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_qp_list_headers' and batch_name = 'om';

COMMIT;