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

truncate table bec_ods_stg.CST_COST_ELEMENTS;

insert into	bec_ods_stg.CST_COST_ELEMENTS
   (
   COST_ELEMENT_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
COST_ELEMENT,
DESCRIPTION,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
ZD_EDITION_NAME,
ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		COST_ELEMENT_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
COST_ELEMENT,
DESCRIPTION,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
ZD_EDITION_NAME,
ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.CST_COST_ELEMENTS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (
	    nvl(COST_ELEMENT_ID, '0'),

	kca_seq_id) in
	(select 
	nvl(COST_ELEMENT_ID, '0') as COST_ELEMENT_ID,

	max(kca_seq_id) from bec_raw_dl_ext.CST_COST_ELEMENTS
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by 
        nvl(COST_ELEMENT_ID, '0')
        )
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_cost_elements')
);
end;
