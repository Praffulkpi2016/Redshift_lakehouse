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

truncate table bec_ods_stg.XLA_DISTRIBUTION_LINKS;

insert into	bec_ods_stg.XLA_DISTRIBUTION_LINKS
   (
   APPLICATION_ID
,EVENT_ID
,AE_HEADER_ID
,AE_LINE_NUM
,SOURCE_DISTRIBUTION_TYPE
,SOURCE_DISTRIBUTION_ID_CHAR_1
,SOURCE_DISTRIBUTION_ID_CHAR_2
,SOURCE_DISTRIBUTION_ID_CHAR_3
,SOURCE_DISTRIBUTION_ID_CHAR_4
,SOURCE_DISTRIBUTION_ID_CHAR_5
,SOURCE_DISTRIBUTION_ID_NUM_1
,SOURCE_DISTRIBUTION_ID_NUM_2
,SOURCE_DISTRIBUTION_ID_NUM_3
,SOURCE_DISTRIBUTION_ID_NUM_4
,SOURCE_DISTRIBUTION_ID_NUM_5
,TAX_LINE_REF_ID
,TAX_SUMMARY_LINE_REF_ID
,TAX_REC_NREC_DIST_REF_ID
,STATISTICAL_AMOUNT
,REF_AE_HEADER_ID
,REF_TEMP_LINE_NUM
,ACCOUNTING_LINE_CODE
,ACCOUNTING_LINE_TYPE_CODE
,MERGE_DUPLICATE_CODE
,TEMP_LINE_NUM
,REF_EVENT_ID
,LINE_DEFINITION_OWNER_CODE
,LINE_DEFINITION_CODE
,EVENT_CLASS_CODE
,EVENT_TYPE_CODE
,UPG_BATCH_ID
,CALCULATE_ACCTD_AMTS_FLAG
,CALCULATE_G_L_AMTS_FLAG
,ROUNDING_CLASS_CODE
,DOCUMENT_ROUNDING_LEVEL
,UNROUNDED_ENTERED_DR
,UNROUNDED_ENTERED_CR
,DOC_ROUNDING_ENTERED_AMT
,DOC_ROUNDING_ACCTD_AMT
,UNROUNDED_ACCOUNTED_CR
,UNROUNDED_ACCOUNTED_DR
,APPLIED_TO_APPLICATION_ID
,APPLIED_TO_ENTITY_CODE
,APPLIED_TO_ENTITY_ID
,APPLIED_TO_SOURCE_ID_NUM_1
,APPLIED_TO_SOURCE_ID_NUM_2
,APPLIED_TO_SOURCE_ID_NUM_3
,APPLIED_TO_SOURCE_ID_NUM_4
,APPLIED_TO_SOURCE_ID_CHAR_1
,APPLIED_TO_SOURCE_ID_CHAR_2
,APPLIED_TO_SOURCE_ID_CHAR_3
,APPLIED_TO_SOURCE_ID_CHAR_4
,APPLIED_TO_DISTRIBUTION_TYPE
,APPLIED_TO_DIST_ID_NUM_1
,APPLIED_TO_DIST_ID_NUM_2
,APPLIED_TO_DIST_ID_NUM_3
,APPLIED_TO_DIST_ID_NUM_4
,APPLIED_TO_DIST_ID_NUM_5
,APPLIED_TO_DIST_ID_CHAR_1
,APPLIED_TO_DIST_ID_CHAR_2
,APPLIED_TO_DIST_ID_CHAR_3
,APPLIED_TO_DIST_ID_CHAR_4
,APPLIED_TO_DIST_ID_CHAR_5
,ALLOC_TO_APPLICATION_ID
,ALLOC_TO_ENTITY_CODE
,ALLOC_TO_SOURCE_ID_NUM_1
,ALLOC_TO_SOURCE_ID_NUM_2
,ALLOC_TO_SOURCE_ID_NUM_3
,ALLOC_TO_SOURCE_ID_NUM_4
,ALLOC_TO_SOURCE_ID_CHAR_1
,ALLOC_TO_SOURCE_ID_CHAR_2
,ALLOC_TO_SOURCE_ID_CHAR_3
,ALLOC_TO_SOURCE_ID_CHAR_4
,ALLOC_TO_DISTRIBUTION_TYPE
,ALLOC_TO_DIST_ID_NUM_1
,ALLOC_TO_DIST_ID_NUM_2
,ALLOC_TO_DIST_ID_NUM_3
,ALLOC_TO_DIST_ID_NUM_4
,ALLOC_TO_DIST_ID_NUM_5
,ALLOC_TO_DIST_ID_CHAR_1
,ALLOC_TO_DIST_ID_CHAR_2
,ALLOC_TO_DIST_ID_CHAR_3
,ALLOC_TO_DIST_ID_CHAR_4
,ALLOC_TO_DIST_ID_CHAR_5
,GAIN_OR_LOSS_REF
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	)
(
	select
		APPLICATION_ID
		,EVENT_ID
		,AE_HEADER_ID
		,AE_LINE_NUM
		,SOURCE_DISTRIBUTION_TYPE
		,SOURCE_DISTRIBUTION_ID_CHAR_1
		,SOURCE_DISTRIBUTION_ID_CHAR_2
		,SOURCE_DISTRIBUTION_ID_CHAR_3
		,SOURCE_DISTRIBUTION_ID_CHAR_4
		,SOURCE_DISTRIBUTION_ID_CHAR_5
		,SOURCE_DISTRIBUTION_ID_NUM_1
		,SOURCE_DISTRIBUTION_ID_NUM_2
		,SOURCE_DISTRIBUTION_ID_NUM_3
		,SOURCE_DISTRIBUTION_ID_NUM_4
		,SOURCE_DISTRIBUTION_ID_NUM_5
		,TAX_LINE_REF_ID
		,TAX_SUMMARY_LINE_REF_ID
		,TAX_REC_NREC_DIST_REF_ID
		,STATISTICAL_AMOUNT
		,REF_AE_HEADER_ID
		,REF_TEMP_LINE_NUM
		,ACCOUNTING_LINE_CODE
		,ACCOUNTING_LINE_TYPE_CODE
		,MERGE_DUPLICATE_CODE
		,TEMP_LINE_NUM
		,REF_EVENT_ID
		,LINE_DEFINITION_OWNER_CODE
		,LINE_DEFINITION_CODE
		,EVENT_CLASS_CODE
		,EVENT_TYPE_CODE
		,UPG_BATCH_ID
		,CALCULATE_ACCTD_AMTS_FLAG
		,CALCULATE_G_L_AMTS_FLAG
		,ROUNDING_CLASS_CODE
		,DOCUMENT_ROUNDING_LEVEL
		,UNROUNDED_ENTERED_DR
		,UNROUNDED_ENTERED_CR
		,DOC_ROUNDING_ENTERED_AMT
		,DOC_ROUNDING_ACCTD_AMT
		,UNROUNDED_ACCOUNTED_CR
		,UNROUNDED_ACCOUNTED_DR
		,APPLIED_TO_APPLICATION_ID
		,APPLIED_TO_ENTITY_CODE
		,APPLIED_TO_ENTITY_ID
		,APPLIED_TO_SOURCE_ID_NUM_1
		,APPLIED_TO_SOURCE_ID_NUM_2
		,APPLIED_TO_SOURCE_ID_NUM_3
		,APPLIED_TO_SOURCE_ID_NUM_4
		,APPLIED_TO_SOURCE_ID_CHAR_1
		,APPLIED_TO_SOURCE_ID_CHAR_2
		,APPLIED_TO_SOURCE_ID_CHAR_3
		,APPLIED_TO_SOURCE_ID_CHAR_4
		,APPLIED_TO_DISTRIBUTION_TYPE
		,APPLIED_TO_DIST_ID_NUM_1
		,APPLIED_TO_DIST_ID_NUM_2
		,APPLIED_TO_DIST_ID_NUM_3
		,APPLIED_TO_DIST_ID_NUM_4
		,APPLIED_TO_DIST_ID_NUM_5
		,APPLIED_TO_DIST_ID_CHAR_1
		,APPLIED_TO_DIST_ID_CHAR_2
		,APPLIED_TO_DIST_ID_CHAR_3
		,APPLIED_TO_DIST_ID_CHAR_4
		,APPLIED_TO_DIST_ID_CHAR_5
		,ALLOC_TO_APPLICATION_ID
		,ALLOC_TO_ENTITY_CODE
		,ALLOC_TO_SOURCE_ID_NUM_1
		,ALLOC_TO_SOURCE_ID_NUM_2
		,ALLOC_TO_SOURCE_ID_NUM_3
		,ALLOC_TO_SOURCE_ID_NUM_4
		,ALLOC_TO_SOURCE_ID_CHAR_1
		,ALLOC_TO_SOURCE_ID_CHAR_2
		,ALLOC_TO_SOURCE_ID_CHAR_3
		,ALLOC_TO_SOURCE_ID_CHAR_4
		,ALLOC_TO_DISTRIBUTION_TYPE
		,ALLOC_TO_DIST_ID_NUM_1
		,ALLOC_TO_DIST_ID_NUM_2
		,ALLOC_TO_DIST_ID_NUM_3
		,ALLOC_TO_DIST_ID_NUM_4
		,ALLOC_TO_DIST_ID_NUM_5
		,ALLOC_TO_DIST_ID_CHAR_1
		,ALLOC_TO_DIST_ID_CHAR_2
		,ALLOC_TO_DIST_ID_CHAR_3
		,ALLOC_TO_DIST_ID_CHAR_4
		,ALLOC_TO_DIST_ID_CHAR_5
		,GAIN_OR_LOSS_REF
		,KCA_OPERATION
		,KCA_SEQ_ID
		,KCA_SEQ_DATE
  from 
    bec_raw_dl_ext.XLA_DISTRIBUTION_LINKS 
  where 
    kca_operation != 'DELETE' 
    and nvl(kca_seq_id, '') != '' 
    and (
      nvl(APPLICATION_ID, 0), 
      nvl(REF_AE_HEADER_ID, 0), 
      nvl(TEMP_LINE_NUM, 0), 
      nvl(AE_HEADER_ID, 0), 
      kca_seq_id
    ) in (
      select 
        nvl(APPLICATION_ID, 0), 
        nvl(REF_AE_HEADER_ID, 0), 
        nvl(TEMP_LINE_NUM, 0), 
        nvl(AE_HEADER_ID, 0), 
        max(kca_seq_id) 
      from 
        bec_raw_dl_ext.XLA_DISTRIBUTION_LINKS 
      where 
        kca_operation != 'DELETE' 
        and nvl(kca_seq_id, '') != '' 
      group by 
        nvl(APPLICATION_ID, 0), 
        nvl(REF_AE_HEADER_ID, 0), 
        nvl(TEMP_LINE_NUM, 0), 
        nvl(AE_HEADER_ID, 0)
    ) 
    and KCA_SEQ_DATE > (
      select 
        (executebegints - prune_days) 
      from 
        bec_etl_ctrl.batch_ods_info 
      where 
        ods_table_name = 'xla_distribution_links'
    )
);
end;


