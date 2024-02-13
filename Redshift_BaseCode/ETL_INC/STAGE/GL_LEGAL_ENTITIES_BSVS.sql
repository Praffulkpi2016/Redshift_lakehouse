/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06.
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;

TRUNCATE TABLE bec_ods_stg.GL_LEGAL_ENTITIES_BSVS;

insert into	bec_ods_stg.GL_LEGAL_ENTITIES_BSVS
    (
	LEGAL_ENTITY_ID,
    FLEX_VALUE_SET_ID,
    FLEX_SEGMENT_VALUE,
    START_DATE,
    END_DATE, 
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY, 
    LAST_UPDATE_LOGIN,
    CREATION_DATE, 
    CREATED_BY,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date
	)
(select
    LEGAL_ENTITY_ID,
    FLEX_VALUE_SET_ID,
    FLEX_SEGMENT_VALUE,
    START_DATE,
    END_DATE, 
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY, 
    LAST_UPDATE_LOGIN,
    CREATION_DATE, 
    CREATED_BY,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date
from
	bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS 
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE,KCA_SEQ_ID) in 
	(select LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE,max(KCA_SEQ_ID) from bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by LEGAL_ENTITY_ID,FLEX_VALUE_SET_ID,FLEX_SEGMENT_VALUE)
     and ( kca_seq_date > (select (executebegints-prune_days) 
	from bec_etl_ctrl.batch_ods_info where ods_table_name ='gl_legal_entities_bsvs')
    
            )
);
END;
