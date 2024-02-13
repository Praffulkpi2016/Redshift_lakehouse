/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN; 

TRUNCATE TABLE bec_ods_stg.BEC_CUSTOMER_DETAILS_VIEW;

INSERT INTO bec_ods_stg.BEC_CUSTOMER_DETAILS_VIEW
SELECT * FROM bec_raw_dl_ext.BEC_CUSTOMER_DETAILS_VIEW
where kca_operation != 'DELETE';

END;
