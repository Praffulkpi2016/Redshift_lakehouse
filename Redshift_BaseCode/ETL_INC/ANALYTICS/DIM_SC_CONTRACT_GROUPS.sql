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
	bec_dwh.DIM_SC_CONTRACT_GROUPS
	where
	(nvl(INCLUDED_CHR_ID, 0) ) in (
		select
		nvl(ods.INCLUDED_CHR_ID, 0) as INCLUDED_CHR_ID 
		from
		bec_dwh.DIM_SC_CONTRACT_GROUPS dw,
		(select
			OKG.INCLUDED_CHR_ID
			
			FROM
			bec_ods.OKC_K_GRPINGS OKG,
			bec_ods.OKC_K_GROUPS_TL OKGT
			WHERE
			1 = 1
			AND OKG.CGP_PARENT_ID = OKGT.ID 
			and (OKG.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'dim_sc_contract_groups'
			and batch_name = 'sc'))
		)  ods 
		where
		dw.dw_load_id = (
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')  
		|| '-' || nvl(ods.INCLUDED_CHR_ID,0) 
	);
	
	commit;
	-- Insert records
	
	insert
	into
	bec_dwh.DIM_SC_CONTRACT_GROUPS
	(
		id,
		cgp_parent_id,
		included_chr_id,
		scs_code,
		name,
		SHORT_DESCRIPTION,
		is_deleted_flg,
		source_app_id,
		dw_load_id,
		dw_insert_date,
		dw_update_date
	)
	
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
		|| '-' || nvl(OKG.INCLUDED_CHR_ID, 0)  as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		FROM bec_ods.OKC_K_GRPINGS OKG,
		bec_ods.OKC_K_GROUPS_TL OKGT 
		WHERE
		1 = 1
		AND OKG.CGP_PARENT_ID = OKGT.ID  	and
		(
			OKG.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'dim_sc_contract_groups'
			and batch_name = 'sc') 			
		)
		
	);
	commit;
	-- Soft delete
	
	
	update
	bec_dwh.DIM_SC_CONTRACT_GROUPS 
	set
	is_deleted_flg = 'Y'
	where
	(nvl(INCLUDED_CHR_ID, 0) ) not in (
		select
		nvl(ods.INCLUDED_CHR_ID, 0) as INCLUDED_CHR_ID 
		from
		bec_dwh.DIM_SC_CONTRACT_GROUPS dw,
		(select
			OKG.INCLUDED_CHR_ID
			
			FROM
			(select * from BEC_ODS.OKC_K_GRPINGS where is_deleted_flg <> 'Y') OKG,
			(select * from bec_ods.OKC_K_GROUPS_TL where is_deleted_flg <> 'Y') OKGT
			WHERE
			1 = 1
			AND OKG.CGP_PARENT_ID = OKGT.ID 
			   
		)  ods 
		where
		dw.dw_load_id = (
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')  
		|| '-' || nvl(ods.INCLUDED_CHR_ID,0) 
	);
	
	commit;
end;

update
bec_etl_ctrl.batch_dw_info
set
last_refresh_date = getdate()
where
dw_table_name = 'dim_sc_contract_groups'
and batch_name = 'sc';

commit;