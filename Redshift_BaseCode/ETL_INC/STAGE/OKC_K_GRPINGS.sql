/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.OKC_K_GRPINGS;

COMMIT;

insert
	into
	bec_ods_stg.OKC_K_GRPINGS
(	ID,
	CGP_PARENT_ID,
	INCLUDED_CGP_ID,
	INCLUDED_CHR_ID,
	OBJECT_VERSION_NUMBER,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	SECURITY_GROUP_ID,
	SCS_CODE,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		ID,
		CGP_PARENT_ID,
		INCLUDED_CGP_ID,
		INCLUDED_CHR_ID,
		OBJECT_VERSION_NUMBER,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		SECURITY_GROUP_ID,
		SCS_CODE,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.OKC_K_GRPINGS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(ID,'NA'),
		kca_seq_id) in 
(
		select
			nvl(ID,'NA') as ID,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.OKC_K_GRPINGS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(ID,'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'okc_k_grpings')
);
end;