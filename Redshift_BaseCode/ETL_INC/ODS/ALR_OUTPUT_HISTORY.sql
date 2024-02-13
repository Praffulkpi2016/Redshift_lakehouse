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

delete from bec_ods.ALR_OUTPUT_HISTORY
where (nvl(APPLICATION_ID,0),nvl(CHECK_ID,0),nvl(ROW_NUMBER, 0),nvl(NAME, 'NA')) in (
select nvl(stg.APPLICATION_ID,0) as APPLICATION_ID,
nvl(stg.CHECK_ID,0) as CHECK_ID,
nvl(stg.ROW_NUMBER, 0) as ROW_NUMBER,
nvl(stg.NAME, 'NA') as NAME
from bec_ods.alr_output_history ods, bec_ods_stg.alr_output_history stg
where nvl(ods.APPLICATION_ID,0) = nvl(stg.APPLICATION_ID,0) 
and nvl(ods.CHECK_ID,0) = nvl(stg.CHECK_ID,0)
and nvl(ods.ROW_NUMBER, 0) = nvl(stg.ROW_NUMBER, 0)
and nvl(ods.NAME, 'NA') = nvl(stg.NAME, 'NA')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.alr_output_history
       (	
		APPLICATION_ID, 
		NAME, 
		CHECK_ID, 
		ROW_NUMBER, 
		DATA_TYPE, 
		VALUE, 
		SECURITY_GROUP_ID, 
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		APPLICATION_ID, 
		NAME, 
		CHECK_ID, 
		ROW_NUMBER, 
		DATA_TYPE, 
		VALUE, 
		SECURITY_GROUP_ID, 
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.alr_output_history
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(APPLICATION_ID,0),nvl(CHECK_ID,0),nvl(ROW_NUMBER, 0),nvl(NAME, 'NA'),kca_seq_ID) in 
	(select nvl(APPLICATION_ID,0) as APPLICATION_ID,nvl(CHECK_ID,0) as CHECK_ID,nvl(ROW_NUMBER, 0) as ROW_NUMBER,nvl(NAME, 'NA') as NAME,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.alr_output_history 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(APPLICATION_ID,0),nvl(CHECK_ID,0),nvl(ROW_NUMBER, 0),nvl(NAME, 'NA'))
);

commit;

-- Soft delete
update bec_ods.alr_output_history set IS_DELETED_FLG = 'N';
update bec_ods.alr_output_history set IS_DELETED_FLG = 'Y'
where (nvl(APPLICATION_ID,0),nvl(CHECK_ID,0),nvl(ROW_NUMBER, 0),nvl(NAME, 'NA'))  in
(
select APPLICATION_ID,CHECK_ID,row_number,NAME from bec_raw_dl_ext.alr_output_history
where (APPLICATION_ID,CHECK_ID,row_number,NAME,KCA_SEQ_ID)
in 
(
select nvl(APPLICATION_ID,0) as APPLICATION_ID,nvl(CHECK_ID,0) as CHECK_ID,nvl(ROW_NUMBER, 0) as ROW_NUMBER,
nvl(NAME, 'NA') as NAME,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.alr_output_history
group by nvl(APPLICATION_ID,0),nvl(CHECK_ID,0),nvl(ROW_NUMBER, 0),nvl(NAME, 'NA')
) 
and kca_operation= 'DELETE'
);


commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'alr_output_history';

commit;