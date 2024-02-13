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
	table bec_ods_stg.CS_INCIDENT_SEVERITIES_TL;

COMMIT;

insert
	into
	bec_ods_stg.CS_INCIDENT_SEVERITIES_TL
(	
	INCIDENT_SEVERITY_ID, 
	LANGUAGE, 
	SOURCE_LANG, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	DESCRIPTION, 
	NAME, 
	SECURITY_GROUP_ID,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		INCIDENT_SEVERITY_ID, 
		LANGUAGE, 
		SOURCE_LANG, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		DESCRIPTION, 
		NAME, 
		SECURITY_GROUP_ID,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.CS_INCIDENT_SEVERITIES_TL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(INCIDENT_SEVERITY_ID, 0),
		nvl(LANGUAGE, 'NA'),
		kca_seq_id) in 
(
		select
			nvl(INCIDENT_SEVERITY_ID,0) as INCIDENT_SEVERITY_ID,
			nvl(LANGUAGE, 'NA') as LANGUAGE,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.CS_INCIDENT_SEVERITIES_TL
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(INCIDENT_SEVERITY_ID, 0),
			nvl(LANGUAGE, 'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cs_incident_severities_tl')
);
end;