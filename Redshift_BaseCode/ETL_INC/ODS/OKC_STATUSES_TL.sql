/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents Incremental load approach for ODS.
	# File Version: KPI v1.0
*/
begin;
	
	-- Delete Records
	
	delete from bec_ods.OKC_STATUSES_TL
	where (nvl(CODE,'NA'),nvl(LANGUAGE, 'NA' ))  in (
		select nvl(stg.CODE,'NA') as CODE,nvl(stg.LANGUAGE,'NA') as LANGUAGE from bec_ods.OKC_STATUSES_TL ods, bec_ods_stg.OKC_STATUSES_TL stg
		where nvl(ods.CODE,'NA') = nvl(stg.CODE,'NA')
		and nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA')
		and stg.kca_operation IN ('INSERT','UPDATE')
	);
	
	commit;
	
	-- Insert records
	
	insert into	bec_ods.OKC_STATUSES_TL
	(
		code,
		"language",
		source_lang,
		sfwt_flag,
		meaning,
		description,
		created_by,
		creation_date,
		last_updated_by,
		last_update_date,
		last_update_login,
		security_group_id,
		zd_edition_name,
		zd_sync,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
	kca_seq_date)	
	(
		select
		code,
		"language",
		source_lang,
		sfwt_flag,
		meaning,
		description,
		created_by,
		creation_date,
		last_updated_by,
		last_update_date,
		last_update_login,
		security_group_id,
		zd_edition_name,
		zd_sync,
        KCA_OPERATION,
		'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.OKC_STATUSES_TL
		where kca_operation IN ('INSERT','UPDATE') 
		and (nvl(CODE,'NA'),nvl(LANGUAGE, 'NA' ),kca_seq_id) in 
		(select nvl(CODE,'NA') as CODE,nvl(LANGUAGE, 'NA' ) as LANGUAGE,max(kca_seq_id) from bec_ods_stg.OKC_STATUSES_TL 
			where kca_operation IN ('INSERT','UPDATE')
		group by nvl(CODE,'NA'),nvl(LANGUAGE, 'NA' ))
	);
	
	commit;
	
	-- Soft delete
update bec_ods.OKC_STATUSES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKC_STATUSES_TL set IS_DELETED_FLG = 'Y'
where (nvl(CODE,'NA'),nvl(LANGUAGE, 'NA'  ))  in
(
select nvl(CODE,'NA'),nvl(LANGUAGE, 'NA'  ) from bec_raw_dl_ext.OKC_STATUSES_TL
where (nvl(CODE,'NA'),nvl(LANGUAGE, 'NA'  ),KCA_SEQ_ID)
in 
(
select nvl(CODE,'NA'),nvl(LANGUAGE, 'NA'  ),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKC_STATUSES_TL
group by nvl(CODE,'NA'),nvl(LANGUAGE, 'NA'  )
) 
and kca_operation= 'DELETE'
);
commit;
	
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'okc_statuses_tl';

commit;