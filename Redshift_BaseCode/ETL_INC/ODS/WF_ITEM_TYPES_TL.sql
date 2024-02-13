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

delete from bec_ods.WF_ITEM_TYPES_TL
where (nvl(NAME,'NA'),nvl(LANGUAGE,'NA')) in 
(
select nvl(stg.NAME,'NA') as NAME,nvl(stg.LANGUAGE,'NA') as LANGUAGE  from bec_ods.WF_ITEM_TYPES_TL ods, bec_ods_stg.WF_ITEM_TYPES_TL stg
where nvl(ods.NAME,'NA') = nvl(stg.NAME,'NA') and
 nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA')  and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.WF_ITEM_TYPES_TL
       (	
			"name",
		"language",
		display_name,
		protect_level,
		custom_level,
		description,
		source_lang,
		security_group_id,
		zd_edition_name,
		zd_sync,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
(
	select
			"name",
		"language",
		display_name,
		protect_level,
		custom_level,
		description,
		source_lang,
		security_group_id,
		zd_edition_name,
		zd_sync,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.WF_ITEM_TYPES_TL
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(NAME,'NA'),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(NAME,'NA') as NAME,nvl(LANGUAGE,'NA') as LANGUAGE ,max(kca_seq_id) from bec_ods_stg.WF_ITEM_TYPES_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(NAME,'NA'),nvl(LANGUAGE,'NA'))
);

commit;



-- Soft delete
update bec_ods.WF_ITEM_TYPES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WF_ITEM_TYPES_TL set IS_DELETED_FLG = 'Y'
where (nvl(NAME,'NA'),nvl(LANGUAGE,'NA'))  in
(
select nvl(NAME,'NA'),nvl(LANGUAGE,'NA') from bec_raw_dl_ext.WF_ITEM_TYPES_TL
where (nvl(NAME,'NA'),nvl(LANGUAGE,'NA'),KCA_SEQ_ID)
in 
(
select nvl(NAME,'NA'),nvl(LANGUAGE,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WF_ITEM_TYPES_TL
group by nvl(NAME,'NA'),nvl(LANGUAGE,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wf_item_types_tl';

commit;