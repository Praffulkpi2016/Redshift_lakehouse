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

truncate table bec_ods_stg.OKC_LINE_STYLES_TL;

insert into	bec_ods_stg.OKC_LINE_STYLES_TL
   (id,
	"language",
	source_lang,
	sfwt_flag,
	"name",
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
	id,
	"language",
	source_lang,
	sfwt_flag,
	"name",
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
	from bec_raw_dl_ext.OKC_LINE_STYLES_TL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(ID,0),nvl(LANGUAGE, 'NA' ),kca_seq_id) in 
	(select nvl(ID,0) as ID,nvl(LANGUAGE, 'NA' ) as LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.OKC_LINE_STYLES_TL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(ID,0),nvl(LANGUAGE, 'NA' ))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'okc_line_styles_tl')
);
end;
