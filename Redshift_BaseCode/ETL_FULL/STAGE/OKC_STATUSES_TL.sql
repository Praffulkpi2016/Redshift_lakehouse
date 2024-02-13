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
	
	DROP TABLE IF EXISTS bec_ods_stg.OKC_STATUSES_TL;
	
	CREATE TABLE bec_ods_stg.OKC_STATUSES_TL 
	DISTKEY (CODE)
SORTKEY (CODE, LANGUAGE, last_update_date)
AS 
	SELECT * FROM bec_raw_dl_ext.OKC_STATUSES_TL
	where kca_operation != 'DELETE' 
	and (nvl(CODE,'NA') ,nvl(LANGUAGE, 'NA' ),last_update_date) in 
	(select nvl(CODE,'NA') AS CODE  ,nvl(LANGUAGE,'NA' ) AS LANGUAGE,max(last_update_date) from bec_raw_dl_ext.OKC_STATUSES_TL 
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
	group by nvl(CODE,'NA') ,nvl(LANGUAGE,'NA' ));

END;