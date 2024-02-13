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

truncate table bec_ods_stg.HR_LOCATIONS_ALL_TL;
INSERT INTO bec_ods_stg.HR_LOCATIONS_ALL_TL (
	location_id,
	"language",
	source_lang,
	location_code,
	description,
	last_update_date,
	last_updated_by,
	last_update_login,
	created_by,
	creation_date,
	kca_operation,
	kca_seq_id,
	kca_seq_date
)

(
	 SELECT
   location_id,
	"language",
	source_lang,
	location_code,
	description,
	last_update_date,
	last_updated_by,
	last_update_login,
	created_by,
	creation_date,
	kca_operation,
    kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (location_id,language ,kca_seq_id) in 
	(select location_id,language ,max(kca_seq_id) from bec_raw_dl_ext.HR_LOCATIONS_ALL_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by location_id,language )
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'hr_locations_all_tl')
		 
            )
);

END;