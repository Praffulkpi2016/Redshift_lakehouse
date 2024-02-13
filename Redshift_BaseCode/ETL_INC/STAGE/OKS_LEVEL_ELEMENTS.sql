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
	table bec_ods_stg.OKS_LEVEL_ELEMENTS;
	
COMMIT;

insert
	into
	bec_ods_stg.OKS_LEVEL_ELEMENTS
(	ID,
	SEQUENCE_NUMBER,
	DATE_START,
	AMOUNT,
	DATE_RECEIVABLE_GL,
	DATE_REVENUE_RULE_START,
	DATE_TRANSACTION,
	DATE_DUE,
	DATE_PRINT,
	DATE_TO_INTERFACE,
	DATE_COMPLETED,
	OBJECT_VERSION_NUMBER,
	RUL_ID,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	SECURITY_GROUP_ID,
	CLE_ID,
	DNZ_CHR_ID,
	PARENT_CLE_ID,
	DATE_END,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		ID,
		SEQUENCE_NUMBER,
		DATE_START,
		AMOUNT,
		DATE_RECEIVABLE_GL,
		DATE_REVENUE_RULE_START,
		DATE_TRANSACTION,
		DATE_DUE,
		DATE_PRINT,
		DATE_TO_INTERFACE,
		DATE_COMPLETED,
		OBJECT_VERSION_NUMBER,
		RUL_ID,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		SECURITY_GROUP_ID,
		CLE_ID,
		DNZ_CHR_ID,
		PARENT_CLE_ID,
		DATE_END,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(ID, 'NA'),
		kca_seq_id) in 
(
		select
			nvl(ID,'NA') as ID,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(ID, 'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oks_level_elements')
);
end;