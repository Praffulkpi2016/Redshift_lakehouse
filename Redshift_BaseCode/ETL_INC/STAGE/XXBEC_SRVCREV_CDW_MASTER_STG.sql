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

truncate table bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG;

insert into	bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG
   (  "source",
	contract_group,
	contract_id,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	start_date,
	end_date,
	last_update_date,
	extract_date,
	site_use_id,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
	 "source",
	contract_group,
	contract_id,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	start_date,
	end_date,
	last_update_date,
	extract_date,
	site_use_id,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.XXBEC_SRVCREV_CDW_MASTER_STG
	 
);
end;