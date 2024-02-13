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

truncate table bec_ods_stg.OE_HOLD_SOURCES_ALL;

insert into	bec_ods_stg.OE_HOLD_SOURCES_ALL
   (	
	HOLD_SOURCE_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_LOGIN
,      CREATION_DATE
,      CREATED_BY
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      REQUEST_ID
,      HOLD_ID
,      HOLD_ENTITY_CODE
,      HOLD_ENTITY_ID
,      HOLD_ENTITY_CODE2
,      HOLD_ENTITY_ID2
,      HOLD_UNTIL_DATE
,      RELEASED_FLAG
,      HOLD_COMMENT
,      ORG_ID
,      CONTEXT
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      HOLD_RELEASE_ID,
    KCA_OPERATION,
	kca_seq_id
	,kca_seq_date)
(
	select
		HOLD_SOURCE_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_LOGIN
,      CREATION_DATE
,      CREATED_BY
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      REQUEST_ID
,      HOLD_ID
,      HOLD_ENTITY_CODE
,      HOLD_ENTITY_ID
,      HOLD_ENTITY_CODE2
,      HOLD_ENTITY_ID2
,      HOLD_UNTIL_DATE
,      RELEASED_FLAG
,      HOLD_COMMENT
,      ORG_ID
,      CONTEXT
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      HOLD_RELEASE_ID,
        KCA_OPERATION,
		kca_seq_id
		,kca_seq_date
	from bec_raw_dl_ext.OE_HOLD_SOURCES_ALL
	where kca_operation != 'DELETE'    and nvl(kca_seq_id,'') != ''
	and (nvl(HOLD_SOURCE_ID,0),kca_seq_id) in 
	(select nvl(HOLD_SOURCE_ID,0),max(kca_seq_id) from bec_raw_dl_ext.OE_HOLD_SOURCES_ALL 
     where kca_operation != 'DELETE'   and nvl(kca_seq_id,'') != ''
     group by nvl(HOLD_SOURCE_ID,0))
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oe_hold_sources_all')
)
);
end;