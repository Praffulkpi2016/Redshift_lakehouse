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
	
	truncate table bec_ods_stg.OKC_STATUSES_TL;
	
	insert into	bec_ods_stg.OKC_STATUSES_TL
	(code,
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
		kca_seq_id,
		kca_seq_date
		from bec_raw_dl_ext.OKC_STATUSES_TL
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
		and (nvl(CODE,'NA'),nvl(LANGUAGE, 'NA' ),kca_seq_id) in 
		(select nvl(CODE,'NA') as CODE,nvl(LANGUAGE, 'NA' ) as LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.OKC_STATUSES_TL 
			where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by nvl(CODE,'NA'),nvl(LANGUAGE, 'NA' ))
        and	kca_seq_date > (
			select
			(executebegints-prune_days)
			from
			bec_etl_ctrl.batch_ods_info
			where
		ods_table_name = 'okc_statuses_tl')
	);
end;
