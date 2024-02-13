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
	bec_dwh.dim_wf_item_types
where
	nvl(NAME, 'NA') 
in 
(
	select
		ods.NAME
	from
		bec_dwh.dim_wf_item_types dw,
		(
		select
			nvl(B.NAME, 'NA') as NAME
		from
			BEC_ODS.WF_ITEM_TYPES B
	,
			BEC_ODS.WF_ITEM_TYPES_TL T
		where
			B.NAME = T.NAME
			and T.LANGUAGE = 'US'
			and (
substring(B.kca_seq_id::varchar, 1, 8)::timestamp > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_wf_item_types'
				and batch_name = 'om')
			or 
substring(T.kca_seq_id::varchar, 1, 8)::timestamp > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_wf_item_types'
				and batch_name = 'om')
)
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
			source_system = 'EBS'
)
|| '-' || nvl(ODS.NAME,'NA')
);

commit;
--Insert records
insert
	into
	bec_dwh.dim_wf_item_types
(
	NAME,
	PROTECT_LEVEL,
	CUSTOM_LEVEL,
	WF_SELECTOR,
	READ_ROLE,
	WRITE_ROLE,
	EXECUTE_ROLE,
	PERSISTENCE_TYPE,
	PERSISTENCE_DAYS,
	DISPLAY_NAME,
	DESCRIPTION
	,is_deleted_flg
	,source_app_id
	,dw_load_id
	,dw_insert_date
	,dw_update_date
)
(
	select
		B.NAME,
		B.PROTECT_LEVEL,
		B.CUSTOM_LEVEL,
		B.WF_SELECTOR,
		B.READ_ROLE,
		B.WRITE_ROLE,
		B.EXECUTE_ROLE,
		B.PERSISTENCE_TYPE,
		B.PERSISTENCE_DAYS,
		T.DISPLAY_NAME,
		T.DESCRIPTION,
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
|| '-' || nvl(B.NAME, 'NA') as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		BEC_ODS.WF_ITEM_TYPES B
	,
		BEC_ODS.WF_ITEM_TYPES_TL T
	where
		B.NAME = T.NAME
		and T.LANGUAGE = 'US'
		and (
substring(B.kca_seq_id::varchar, 1, 8)::timestamp > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_dw_info
		where
			dw_table_name = 'dim_wf_item_types'
			and batch_name = 'om')
		or 
substring(T.kca_seq_id::varchar, 1, 8)::timestamp > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_dw_info
		where
			dw_table_name = 'dim_wf_item_types'
			and batch_name = 'om')
)
);

commit;
--Soft DELETE

update
	bec_dwh.dim_wf_item_types
set
	is_deleted_flg = 'Y'
where
	nvl(NAME, 'NA') 
not in 
(
	select
		ods.NAME
	from
		bec_dwh.dim_wf_item_types dw,
		(
		select
			nvl(B.NAME, 'NA') as NAME
		from
			(
			select
				*
			from
				BEC_ODS.WF_ITEM_TYPES
			where
				is_deleted_flg <> 'Y') B
	,
			(select
				*
			from
				BEC_ODS.WF_ITEM_TYPES_TL
			where
				is_deleted_flg <> 'Y') T
	where
		B.NAME = T.NAME
		and T.LANGUAGE = 'US'
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
		source_system = 'EBS'
)
|| '-' || nvl(ODS.NAME, 'NA')
);

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_wf_item_types'
	and batch_name = 'om';

commit;