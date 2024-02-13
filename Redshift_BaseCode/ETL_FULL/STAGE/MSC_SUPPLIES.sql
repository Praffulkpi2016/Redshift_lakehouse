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
BEGIN; 
	
	DROP TABLE IF EXISTS bec_ods_stg.MSC_SUPPLIES;
	
	CREATE TABLE bec_ods_stg.MSC_SUPPLIES DISTSTYLE ALL
	SORTKEY (  PLAN_ID, SR_INSTANCE_ID, TRANSACTION_ID, last_update_date)
	AS 
	SELECT * FROM bec_raw_dl_ext.MSC_SUPPLIES
	where kca_operation != 'DELETE' 
	and (nvl(PLAN_ID,0),nvl(SR_INSTANCE_ID, 0 ),nvl(TRANSACTION_ID, 0 ),last_update_date) in 
	(select nvl(PLAN_ID,0) as PLAN_ID,nvl(SR_INSTANCE_ID, 0 ) as SR_INSTANCE_ID,nvl(TRANSACTION_ID, 0 ) as TRANSACTION_ID,max(last_update_date) from bec_raw_dl_ext.MSC_SUPPLIES 
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
	group by nvl(PLAN_ID,0),nvl(SR_INSTANCE_ID, 0 ),nvl(TRANSACTION_ID, 0 ));

END;