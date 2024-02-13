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

truncate table bec_ods_stg.fnd_responsibility_tl;

insert into	bec_ods_stg.fnd_responsibility_tl
   (	
	APPLICATION_ID,
	RESPONSIBILITY_ID,
	LANGUAGE,
	RESPONSIBILITY_NAME,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	DESCRIPTION,
	SOURCE_LANG,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date) 
(
	select
		APPLICATION_ID,
		RESPONSIBILITY_ID,
		LANGUAGE,
		RESPONSIBILITY_NAME,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		DESCRIPTION,
		SOURCE_LANG,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.fnd_responsibility_tl
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
	and (nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(APPLICATION_ID,0) as APPLICATION_ID,nvl(RESPONSIBILITY_ID,0) as RESPONSIBILITY_ID,nvl(LANGUAGE,'NA') as LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.fnd_responsibility_tl 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(APPLICATION_ID,0),nvl(RESPONSIBILITY_ID,0),nvl(LANGUAGE,'NA'))
        and	(   kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='fnd_responsibility_tl')
	)
);
end;