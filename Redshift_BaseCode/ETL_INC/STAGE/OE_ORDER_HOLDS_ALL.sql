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

truncate table bec_ods_stg.OE_ORDER_HOLDS_ALL;

insert into	bec_ods_stg.OE_ORDER_HOLDS_ALL
   (	
	ORDER_HOLD_ID
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,REQUEST_ID
,HOLD_SOURCE_ID
,HOLD_RELEASE_ID
,HEADER_ID
,LINE_ID
,ORG_ID
,CONTEXT
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,RELEASED_FLAG
,INST_ID
,CREDIT_PROFILE_LEVEL,
    KCA_OPERATION,
	kca_seq_id
	,kca_seq_date)
(
	select
		ORDER_HOLD_ID
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,REQUEST_ID
,HOLD_SOURCE_ID
,HOLD_RELEASE_ID
,HEADER_ID
,LINE_ID
,ORG_ID
,CONTEXT
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,RELEASED_FLAG
,INST_ID
,CREDIT_PROFILE_LEVEL,
        KCA_OPERATION,
		kca_seq_id
		,kca_seq_date
	from bec_raw_dl_ext.OE_ORDER_HOLDS_ALL
	where kca_operation != 'DELETE'     and nvl(kca_seq_id,'') != ''
	and (nvl(ORDER_HOLD_ID,0),kca_seq_id) in 
	(select nvl(ORDER_HOLD_ID,0),max(kca_seq_id) from bec_raw_dl_ext.OE_ORDER_HOLDS_ALL 
     where kca_operation != 'DELETE'    and nvl(kca_seq_id,'') != ''
     group by nvl(ORDER_HOLD_ID,0))
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oe_order_holds_all')
)
);
end;