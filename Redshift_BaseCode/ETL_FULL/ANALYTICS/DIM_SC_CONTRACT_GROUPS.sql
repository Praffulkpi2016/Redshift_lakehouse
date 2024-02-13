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

drop table if exists bec_dwh.DIM_SC_CONTRACT_GROUPS;

create table bec_dwh.DIM_SC_CONTRACT_GROUPS diststyle all sortkey(INCLUDED_CHR_ID )
as 
(
select
	OKG.ID,
	OKG.CGP_PARENT_ID,
	OKG.INCLUDED_CHR_ID,
	OKG.SCS_CODE,
	OKGT.NAME,
	OKGT.SHORT_DESCRIPTION,
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
    || '-'
       || nvl(OKG.INCLUDED_CHR_ID, 0)  as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	FROM bec_ods.OKC_K_GRPINGS OKG,
		 bec_ods.OKC_K_GROUPS_TL OKGT 
	 WHERE
	1 = 1
	AND OKG.CGP_PARENT_ID = OKGT.ID  
);

commit;

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_sc_contract_groups'
	and batch_name = 'sc';

commit;