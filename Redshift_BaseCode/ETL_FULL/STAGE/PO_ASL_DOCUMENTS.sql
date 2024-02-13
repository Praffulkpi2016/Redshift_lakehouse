/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/


begin;

drop table if exists bec_ods_stg.PO_ASL_DOCUMENTS   ;

create table bec_ods_stg.PO_ASL_DOCUMENTS  
	DISTKEY (ASL_ID)
SORTKEY (ASL_ID, USING_ORGANIZATION_ID, DOCUMENT_HEADER_ID, last_update_date)

as (
	select
	*
from
	bec_raw_dl_ext.PO_ASL_DOCUMENTS   
where
	kca_operation != 'DELETE'
	and 
(	nvl(ASL_ID, 0),
	nvl(USING_ORGANIZATION_ID, 0),
	nvl(DOCUMENT_HEADER_ID, 0),
	last_update_date)
in (
	select
		nvl(ASL_ID, 0) as ASL_ID,
		nvl(USING_ORGANIZATION_ID, 0) as USING_ORGANIZATION_ID,
        nvl(DOCUMENT_HEADER_ID, 0) as DOCUMENT_HEADER_ID,
		max(last_update_date)
	from
		bec_raw_dl_ext.PO_ASL_DOCUMENTS   
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		nvl(ASL_ID, 0) ,
		nvl(USING_ORGANIZATION_ID, 0) ,
        nvl(DOCUMENT_HEADER_ID, 0) 

		
)
);
end;