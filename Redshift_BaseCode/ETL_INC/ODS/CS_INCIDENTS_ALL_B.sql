/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.CS_INCIDENTS_ALL_B
where NVL(INCIDENT_ID,0) in (
select NVL(stg.INCIDENT_ID,0) 
from bec_ods.CS_INCIDENTS_ALL_B ods, bec_ods_stg.CS_INCIDENTS_ALL_B stg
where ods.INCIDENT_ID = stg.INCIDENT_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.CS_INCIDENTS_ALL_B
(		INCIDENT_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      CREATION_TIME
,      INCIDENT_NUMBER
,      INCIDENT_DATE
,      OPEN_FLAG
,      SERVICE_ENTITY_ID
,      INCIDENT_STATUS_ID
,      INCIDENT_TYPE_ID
,      INCIDENT_URGENCY_ID
,      INCIDENT_SEVERITY_ID
,      SUMMARY
,      RESPONSIBLE_GROUP_ID
,      SECURITY_LEVEL_CODE
,      INCIDENT_OWNER_ID
,      CLASSIFICATION_CODE
,      INVENTORY_ITEM_ID
,      COMPONENT_INVENTORY_ITEM_ID
,      CUSTOMER_ID
,      ORIGINAL_CUSTOMER_ID
,      CUSTOMER_NAME
,      BILL_TO_SITE_USE_ID
,      PURCHASE_ORDER_NUM
,      IDENTIFICATION_NUMBER
,      SERVICE_INVENTORY_ITEM_ID
,      EMPLOYEE_ID
,      FILED_BY_EMPLOYEE_FLAG
,      BILLABLE_FLAG
,      SHIP_TO_SITE_USE_ID
,      PROBLEM_CODE
,      EXPECTED_RESOLUTION_DATE
,      ACTUAL_RESOLUTION_DATE
,      PROBLEM_DESCRIPTION
,      RESOLUTION_DESCRIPTION
,      RUNNING_TOTALS
,      CURRENT_CONTACT_PERSON_ID
,      INITIATING_PERSON_ID
,      CURRENT_CONTACT_NAME
,      CURRENT_CONTACT_AREA_CODE
,      CURRENT_CONTACT_TELEPHONE
,      CURRENT_CONTACT_EXTENSION
,      CURRENT_CONTACT_EMAIL_ADDRESS
,      CURRENT_CONTACT_TIME_DIFF
,      CURRENT_CONTACT_FAX_AREA_CODE
,      CURRENT_CONTACT_FAX_NUMBER
,      REPRESENTED_BY_EMPLOYEE
,      REPRESENTED_BY_NAME
,      REPRESENTED_BY_TIME_DIFFERENCE
,      REPRESENTED_BY_AREA_CODE
,      REPRESENTED_BY_TELEPHONE
,      REPRESENTED_BY_EXTENSION
,      REPRESENTED_BY_FAX_AREA_CODE
,      REPRESENTED_BY_FAX_NUMBER
,      REPRESENTED_BY_EMAIL_ADDRESS
,      START_DATE_ACTIVE
,      END_DATE_ACTIVE
,      INTERNAL_CONTACT_ID
,      HOURS_AFTER_GMT
,      INITIATING_TIME_DIFFERENCE
,      CUSTOMER_PRODUCT_ID
,      CP_SERVICE_ID
,      CURRENTLY_SERVICED_FLAG
,      LAST_CUSTOMER_CALLBACK_TIME
,      PREVENTIVE_MAINTENANCE_FLAG
,      BILL_TO_TITLE
,      BILL_TO_CUSTOMER
,      BILL_TO_ADDRESS_LINE_1
,      BILL_TO_ADDRESS_LINE_2
,      BILL_TO_ADDRESS_LINE_3
,      BILL_TO_CONTACT
,      SHIP_TO_TITLE
,      SHIP_TO_CUSTOMER
,      SHIP_TO_ADDRESS_LINE_1
,      SHIP_TO_ADDRESS_LINE_2
,      SHIP_TO_ADDRESS_LINE_3
,      SHIP_TO_CONTACT
,      INSTALL_TITLE
,      INSTALL_CUSTOMER
,      INSTALL_ADDRESS_LINE_1
,      INSTALL_ADDRESS_LINE_2
,      INSTALL_ADDRESS_LINE_3
,      INSTALL_SITE_USE_ID
,      BILL_TO_CONTACT_ID
,      SHIP_TO_CONTACT_ID
,      INITIATING_NAME
,      INITIATING_AREA_CODE
,      INITIATING_TELEPHONE
,      INITIATING_EXTENSION
,      INITIATING_FAX_AREA_CODE
,      INITIATING_FAX_NUMBER
,      PRODUCT_NAME
,      PRODUCT_DESCRIPTION
,      CURRENT_SERIAL_NUMBER
,      PRODUCT_REVISION
,      CUSTOMER_NUMBER
,      SYSTEM_ID
,      INCIDENT_ATTRIBUTE_1
,      INCIDENT_ATTRIBUTE_2
,      INCIDENT_ATTRIBUTE_3
,      INCIDENT_ATTRIBUTE_4
,      INCIDENT_ATTRIBUTE_5
,      INCIDENT_ATTRIBUTE_6
,      INCIDENT_ATTRIBUTE_7
,      INCIDENT_ATTRIBUTE_8
,      INCIDENT_ATTRIBUTE_9
,      INCIDENT_ATTRIBUTE_10
,      INCIDENT_ATTRIBUTE_11
,      INCIDENT_ATTRIBUTE_12
,      INCIDENT_ATTRIBUTE_13
,      INCIDENT_ATTRIBUTE_14
,      INCIDENT_ATTRIBUTE_15
,      INCIDENT_CONTEXT
,      RECORD_IS_VALID_FLAG
,      INCIDENT_COMMENTS
,      RESOLUTION_CODE
,      RMA_NUMBER
,      RMA_HEADER_ID
,      RMA_FLAG
,      ORG_ID
,      ORIGINAL_ORDER_NUMBER
,      WORKFLOW_PROCESS_ID
,      WEB_ENTRY_FLAG
,      CLOSE_DATE
,      PUBLISH_FLAG
,      MAKE_PUBLIC_PROBLEM
,      MAKE_PUBLIC_RESOLUTION
,      ESTIMATE_ID
,      ESTIMATE_BUSINESS_GROUP_ID
,      INTERFACED_TO_DEPOT_FLAG
,      QA_COLLECTION_ID
,      PROJECT_ID
,      TASK_ID
,      CONTRACT_ID
,      CONTRACT_NUMBER
,      CONTRACT_SERVICE_ID
,      RESOURCE_TYPE
,      RESOURCE_SUBTYPE_ID
,      ACCOUNT_ID
,      KB_TYPE
,      KB_SOLUTION_ID
,      TIME_ZONE_ID
,      TIME_DIFFERENCE
,      CUSTOMER_PO_NUMBER
,      OWNER_GROUP_ID
,      CUSTOMER_TICKET_NUMBER
,      OBLIGATION_DATE
,      SITE_ID
,      CUSTOMER_SITE_ID
,      CALLER_TYPE
,      PLATFORM_VERSION_ID
,      OBJECT_VERSION_NUMBER
,      CP_COMPONENT_ID
,      CP_COMPONENT_VERSION_ID
,      CP_SUBCOMPONENT_ID
,      CP_SUBCOMPONENT_VERSION_ID
,      PLATFORM_ID
,      LANGUAGE_ID
,      TERRITORY_ID
,      CP_REVISION_ID
,      INV_ITEM_REVISION
,      INV_COMPONENT_ID
,      INV_COMPONENT_VERSION
,      INV_SUBCOMPONENT_ID
,      INV_SUBCOMPONENT_VERSION
,      INV_ORGANIZATION_ID
,      SECURITY_GROUP_ID
,      UPGRADED_STATUS_FLAG1
,      UPGRADED_STATUS_FLAG2
,      UPGRADED_STATUS_FLAG3
,      ORIG_SYSTEM_REFERENCE
,      ORIG_SYSTEM_REFERENCE_ID
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      PROJECT_NUMBER
,      PLATFORM_VERSION
,      DB_VERSION
,      CUST_PREF_LANG_ID
,      TIER
,      TIER_VERSION
,      CATEGORY_ID
,      OPERATING_SYSTEM
,      OPERATING_SYSTEM_VERSION
,      DATABASE
,      GROUP_TYPE
,      GROUP_TERRITORY_ID
,      OWNER_ASSIGNED_TIME
,      OWNER_ASSIGNED_FLAG
,      INV_PLATFORM_ORG_ID
,      COMPONENT_VERSION
,      SUBCOMPONENT_VERSION
,      COMM_PREF_CODE
,      LAST_UPDATE_CHANNEL
,      CUST_PREF_LANG_CODE
,      ERROR_CODE
,      CATEGORY_SET_ID
,      EXTERNAL_REFERENCE
,      INCIDENT_OCCURRED_DATE
,      INCIDENT_RESOLVED_DATE
,      INC_RESPONDED_BY_DATE
,      INCIDENT_LOCATION_ID
,      INCIDENT_ADDRESS
,      INCIDENT_CITY
,      INCIDENT_STATE
,      INCIDENT_COUNTRY
,      INCIDENT_PROVINCE
,      INCIDENT_POSTAL_CODE
,      INCIDENT_COUNTY
,      SR_CREATION_CHANNEL
,      DEF_DEFECT_ID
,      DEF_DEFECT_ID2
,      CREDIT_CARD_NUMBER
,      CREDIT_CARD_TYPE_CODE
,      CREDIT_CARD_EXPIRATION_DATE
,      CREDIT_CARD_HOLDER_FNAME
,      CREDIT_CARD_HOLDER_MNAME
,      CREDIT_CARD_HOLDER_LNAME
,      CREDIT_CARD_ID
,      EXTERNAL_ATTRIBUTE_1
,      EXTERNAL_ATTRIBUTE_2
,      EXTERNAL_ATTRIBUTE_3
,      EXTERNAL_ATTRIBUTE_4
,      EXTERNAL_ATTRIBUTE_5
,      EXTERNAL_ATTRIBUTE_6
,      EXTERNAL_ATTRIBUTE_7
,      EXTERNAL_ATTRIBUTE_8
,      EXTERNAL_ATTRIBUTE_9
,      EXTERNAL_ATTRIBUTE_10
,      EXTERNAL_ATTRIBUTE_11
,      EXTERNAL_ATTRIBUTE_12
,      EXTERNAL_ATTRIBUTE_13
,      EXTERNAL_ATTRIBUTE_14
,      EXTERNAL_ATTRIBUTE_15
,      EXTERNAL_CONTEXT
,      LAST_UPDATE_PROGRAM_CODE
,      CREATION_PROGRAM_CODE
,      COVERAGE_TYPE
,      BILL_TO_ACCOUNT_ID
,      SHIP_TO_ACCOUNT_ID
,      CUSTOMER_EMAIL_ID
,      CUSTOMER_PHONE_ID
,      BILL_TO_PARTY_ID
,      SHIP_TO_PARTY_ID
,      BILL_TO_SITE_ID
,      SHIP_TO_SITE_ID
,      PROGRAM_LOGIN_ID
,      STATUS_FLAG
,      PRIMARY_CONTACT_ID
,      INCIDENT_POINT_OF_INTEREST
,      INCIDENT_CROSS_STREET
,      INCIDENT_DIRECTION_QUALIFIER
,      INCIDENT_DISTANCE_QUALIFIER
,      INCIDENT_DISTANCE_QUAL_UOM
,      INCIDENT_ADDRESS2
,      INCIDENT_ADDRESS3
,      INCIDENT_ADDRESS4
,      INCIDENT_ADDRESS_STYLE
,      INCIDENT_ADDR_LINES_PHONETIC
,      INCIDENT_PO_BOX_NUMBER
,      INCIDENT_HOUSE_NUMBER
,      INCIDENT_STREET_SUFFIX
,      INCIDENT_STREET
,      INCIDENT_STREET_NUMBER
,      INCIDENT_FLOOR
,      INCIDENT_SUITE
,      INCIDENT_POSTAL_PLUS4_CODE
,      INCIDENT_POSITION
,      INCIDENT_LOCATION_DIRECTIONS
,      INCIDENT_LOCATION_DESCRIPTION
,      INSTALL_SITE_ID
,      OWNING_DEPARTMENT_ID
,      ITEM_SERIAL_NUMBER
,      INCIDENT_LOCATION_TYPE
,      INCIDENT_LAST_MODIFIED_DATE
,      UNASSIGNED_INDICATOR
,      MAINT_ORGANIZATION_ID
,      INSTRUMENT_PAYMENT_USE_ID
,      PROJECT_TASK_ID
,      SLA_DATE_1
,      SLA_DATE_2
,      SLA_DATE_3
,      SLA_DATE_4
,      SLA_DATE_5
,      SLA_DATE_6
,      SLA_DURATION_1
,      SLA_DURATION_2
,      PRICE_LIST_HEADER_ID
,      EXPENDITURE_ORG_ID
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
		INCIDENT_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      CREATION_TIME
,      INCIDENT_NUMBER
,      INCIDENT_DATE
,      OPEN_FLAG
,      SERVICE_ENTITY_ID
,      INCIDENT_STATUS_ID
,      INCIDENT_TYPE_ID
,      INCIDENT_URGENCY_ID
,      INCIDENT_SEVERITY_ID
,      SUMMARY
,      RESPONSIBLE_GROUP_ID
,      SECURITY_LEVEL_CODE
,      INCIDENT_OWNER_ID
,      CLASSIFICATION_CODE
,      INVENTORY_ITEM_ID
,      COMPONENT_INVENTORY_ITEM_ID
,      CUSTOMER_ID
,      ORIGINAL_CUSTOMER_ID
,      CUSTOMER_NAME
,      BILL_TO_SITE_USE_ID
,      PURCHASE_ORDER_NUM
,      IDENTIFICATION_NUMBER
,      SERVICE_INVENTORY_ITEM_ID
,      EMPLOYEE_ID
,      FILED_BY_EMPLOYEE_FLAG
,      BILLABLE_FLAG
,      SHIP_TO_SITE_USE_ID
,      PROBLEM_CODE
,      EXPECTED_RESOLUTION_DATE
,      ACTUAL_RESOLUTION_DATE
,      PROBLEM_DESCRIPTION
,      RESOLUTION_DESCRIPTION
,      RUNNING_TOTALS
,      CURRENT_CONTACT_PERSON_ID
,      INITIATING_PERSON_ID
,      CURRENT_CONTACT_NAME
,      CURRENT_CONTACT_AREA_CODE
,      CURRENT_CONTACT_TELEPHONE
,      CURRENT_CONTACT_EXTENSION
,      CURRENT_CONTACT_EMAIL_ADDRESS
,      CURRENT_CONTACT_TIME_DIFF
,      CURRENT_CONTACT_FAX_AREA_CODE
,      CURRENT_CONTACT_FAX_NUMBER
,      REPRESENTED_BY_EMPLOYEE
,      REPRESENTED_BY_NAME
,      REPRESENTED_BY_TIME_DIFFERENCE
,      REPRESENTED_BY_AREA_CODE
,      REPRESENTED_BY_TELEPHONE
,      REPRESENTED_BY_EXTENSION
,      REPRESENTED_BY_FAX_AREA_CODE
,      REPRESENTED_BY_FAX_NUMBER
,      REPRESENTED_BY_EMAIL_ADDRESS
,      START_DATE_ACTIVE
,      END_DATE_ACTIVE
,      INTERNAL_CONTACT_ID
,      HOURS_AFTER_GMT
,      INITIATING_TIME_DIFFERENCE
,      CUSTOMER_PRODUCT_ID
,      CP_SERVICE_ID
,      CURRENTLY_SERVICED_FLAG
,      LAST_CUSTOMER_CALLBACK_TIME
,      PREVENTIVE_MAINTENANCE_FLAG
,      BILL_TO_TITLE
,      BILL_TO_CUSTOMER
,      BILL_TO_ADDRESS_LINE_1
,      BILL_TO_ADDRESS_LINE_2
,      BILL_TO_ADDRESS_LINE_3
,      BILL_TO_CONTACT
,      SHIP_TO_TITLE
,      SHIP_TO_CUSTOMER
,      SHIP_TO_ADDRESS_LINE_1
,      SHIP_TO_ADDRESS_LINE_2
,      SHIP_TO_ADDRESS_LINE_3
,      SHIP_TO_CONTACT
,      INSTALL_TITLE
,      INSTALL_CUSTOMER
,      INSTALL_ADDRESS_LINE_1
,      INSTALL_ADDRESS_LINE_2
,      INSTALL_ADDRESS_LINE_3
,      INSTALL_SITE_USE_ID
,      BILL_TO_CONTACT_ID
,      SHIP_TO_CONTACT_ID
,      INITIATING_NAME
,      INITIATING_AREA_CODE
,      INITIATING_TELEPHONE
,      INITIATING_EXTENSION
,      INITIATING_FAX_AREA_CODE
,      INITIATING_FAX_NUMBER
,      PRODUCT_NAME
,      PRODUCT_DESCRIPTION
,      CURRENT_SERIAL_NUMBER
,      PRODUCT_REVISION
,      CUSTOMER_NUMBER
,      SYSTEM_ID
,      INCIDENT_ATTRIBUTE_1
,      INCIDENT_ATTRIBUTE_2
,      INCIDENT_ATTRIBUTE_3
,      INCIDENT_ATTRIBUTE_4
,      INCIDENT_ATTRIBUTE_5
,      INCIDENT_ATTRIBUTE_6
,      INCIDENT_ATTRIBUTE_7
,      INCIDENT_ATTRIBUTE_8
,      INCIDENT_ATTRIBUTE_9
,      INCIDENT_ATTRIBUTE_10
,      INCIDENT_ATTRIBUTE_11
,      INCIDENT_ATTRIBUTE_12
,      INCIDENT_ATTRIBUTE_13
,      INCIDENT_ATTRIBUTE_14
,      INCIDENT_ATTRIBUTE_15
,      INCIDENT_CONTEXT
,      RECORD_IS_VALID_FLAG
,      INCIDENT_COMMENTS
,      RESOLUTION_CODE
,      RMA_NUMBER
,      RMA_HEADER_ID
,      RMA_FLAG
,      ORG_ID
,      ORIGINAL_ORDER_NUMBER
,      WORKFLOW_PROCESS_ID
,      WEB_ENTRY_FLAG
,      CLOSE_DATE
,      PUBLISH_FLAG
,      MAKE_PUBLIC_PROBLEM
,      MAKE_PUBLIC_RESOLUTION
,      ESTIMATE_ID
,      ESTIMATE_BUSINESS_GROUP_ID
,      INTERFACED_TO_DEPOT_FLAG
,      QA_COLLECTION_ID
,      PROJECT_ID
,      TASK_ID
,      CONTRACT_ID
,      CONTRACT_NUMBER
,      CONTRACT_SERVICE_ID
,      RESOURCE_TYPE
,      RESOURCE_SUBTYPE_ID
,      ACCOUNT_ID
,      KB_TYPE
,      KB_SOLUTION_ID
,      TIME_ZONE_ID
,      TIME_DIFFERENCE
,      CUSTOMER_PO_NUMBER
,      OWNER_GROUP_ID
,      CUSTOMER_TICKET_NUMBER
,      OBLIGATION_DATE
,      SITE_ID
,      CUSTOMER_SITE_ID
,      CALLER_TYPE
,      PLATFORM_VERSION_ID
,      OBJECT_VERSION_NUMBER
,      CP_COMPONENT_ID
,      CP_COMPONENT_VERSION_ID
,      CP_SUBCOMPONENT_ID
,      CP_SUBCOMPONENT_VERSION_ID
,      PLATFORM_ID
,      LANGUAGE_ID
,      TERRITORY_ID
,      CP_REVISION_ID
,      INV_ITEM_REVISION
,      INV_COMPONENT_ID
,      INV_COMPONENT_VERSION
,      INV_SUBCOMPONENT_ID
,      INV_SUBCOMPONENT_VERSION
,      INV_ORGANIZATION_ID
,      SECURITY_GROUP_ID
,      UPGRADED_STATUS_FLAG1
,      UPGRADED_STATUS_FLAG2
,      UPGRADED_STATUS_FLAG3
,      ORIG_SYSTEM_REFERENCE
,      ORIG_SYSTEM_REFERENCE_ID
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      PROJECT_NUMBER
,      PLATFORM_VERSION
,      DB_VERSION
,      CUST_PREF_LANG_ID
,      TIER
,      TIER_VERSION
,      CATEGORY_ID
,      OPERATING_SYSTEM
,      OPERATING_SYSTEM_VERSION
,      DATABASE
,      GROUP_TYPE
,      GROUP_TERRITORY_ID
,      OWNER_ASSIGNED_TIME
,      OWNER_ASSIGNED_FLAG
,      INV_PLATFORM_ORG_ID
,      COMPONENT_VERSION
,      SUBCOMPONENT_VERSION
,      COMM_PREF_CODE
,      LAST_UPDATE_CHANNEL
,      CUST_PREF_LANG_CODE
,      ERROR_CODE
,      CATEGORY_SET_ID
,      EXTERNAL_REFERENCE
,      INCIDENT_OCCURRED_DATE
,      INCIDENT_RESOLVED_DATE
,      INC_RESPONDED_BY_DATE
,      INCIDENT_LOCATION_ID
,      INCIDENT_ADDRESS
,      INCIDENT_CITY
,      INCIDENT_STATE
,      INCIDENT_COUNTRY
,      INCIDENT_PROVINCE
,      INCIDENT_POSTAL_CODE
,      INCIDENT_COUNTY
,      SR_CREATION_CHANNEL
,      DEF_DEFECT_ID
,      DEF_DEFECT_ID2
,      CREDIT_CARD_NUMBER
,      CREDIT_CARD_TYPE_CODE
,      CREDIT_CARD_EXPIRATION_DATE
,      CREDIT_CARD_HOLDER_FNAME
,      CREDIT_CARD_HOLDER_MNAME
,      CREDIT_CARD_HOLDER_LNAME
,      CREDIT_CARD_ID
,      EXTERNAL_ATTRIBUTE_1
,      EXTERNAL_ATTRIBUTE_2
,      EXTERNAL_ATTRIBUTE_3
,      EXTERNAL_ATTRIBUTE_4
,      EXTERNAL_ATTRIBUTE_5
,      EXTERNAL_ATTRIBUTE_6
,      EXTERNAL_ATTRIBUTE_7
,      EXTERNAL_ATTRIBUTE_8
,      EXTERNAL_ATTRIBUTE_9
,      EXTERNAL_ATTRIBUTE_10
,      EXTERNAL_ATTRIBUTE_11
,      EXTERNAL_ATTRIBUTE_12
,      EXTERNAL_ATTRIBUTE_13
,      EXTERNAL_ATTRIBUTE_14
,      EXTERNAL_ATTRIBUTE_15
,      EXTERNAL_CONTEXT
,      LAST_UPDATE_PROGRAM_CODE
,      CREATION_PROGRAM_CODE
,      COVERAGE_TYPE
,      BILL_TO_ACCOUNT_ID
,      SHIP_TO_ACCOUNT_ID
,      CUSTOMER_EMAIL_ID
,      CUSTOMER_PHONE_ID
,      BILL_TO_PARTY_ID
,      SHIP_TO_PARTY_ID
,      BILL_TO_SITE_ID
,      SHIP_TO_SITE_ID
,      PROGRAM_LOGIN_ID
,      STATUS_FLAG
,      PRIMARY_CONTACT_ID
,      INCIDENT_POINT_OF_INTEREST
,      INCIDENT_CROSS_STREET
,      INCIDENT_DIRECTION_QUALIFIER
,      INCIDENT_DISTANCE_QUALIFIER
,      INCIDENT_DISTANCE_QUAL_UOM
,      INCIDENT_ADDRESS2
,      INCIDENT_ADDRESS3
,      INCIDENT_ADDRESS4
,      INCIDENT_ADDRESS_STYLE
,      INCIDENT_ADDR_LINES_PHONETIC
,      INCIDENT_PO_BOX_NUMBER
,      INCIDENT_HOUSE_NUMBER
,      INCIDENT_STREET_SUFFIX
,      INCIDENT_STREET
,      INCIDENT_STREET_NUMBER
,      INCIDENT_FLOOR
,      INCIDENT_SUITE
,      INCIDENT_POSTAL_PLUS4_CODE
,      INCIDENT_POSITION
,      INCIDENT_LOCATION_DIRECTIONS
,      INCIDENT_LOCATION_DESCRIPTION
,      INSTALL_SITE_ID
,      OWNING_DEPARTMENT_ID
,      ITEM_SERIAL_NUMBER
,      INCIDENT_LOCATION_TYPE
,      INCIDENT_LAST_MODIFIED_DATE
,      UNASSIGNED_INDICATOR
,      MAINT_ORGANIZATION_ID
,      INSTRUMENT_PAYMENT_USE_ID
,      PROJECT_TASK_ID
,      SLA_DATE_1
,      SLA_DATE_2
,      SLA_DATE_3
,      SLA_DATE_4
,      SLA_DATE_5
,      SLA_DATE_6
,      SLA_DURATION_1
,      SLA_DURATION_2
,      PRICE_LIST_HEADER_ID
,      EXPENDITURE_ORG_ID
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
,kca_seq_date
from bec_ods_stg.CS_INCIDENTS_ALL_B
where kca_operation IN ('INSERT','UPDATE') 
	and (INCIDENT_ID,kca_seq_id) in 
	(select INCIDENT_ID,max(kca_seq_id) from bec_ods_stg.CS_INCIDENTS_ALL_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by INCIDENT_ID)
);

commit;

-- Soft delete
update bec_ods.CS_INCIDENTS_ALL_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CS_INCIDENTS_ALL_B set IS_DELETED_FLG = 'Y'
where (INCIDENT_ID )  in
(
select INCIDENT_ID  from bec_raw_dl_ext.CS_INCIDENTS_ALL_B
where (INCIDENT_ID ,KCA_SEQ_ID)
in 
(
select INCIDENT_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CS_INCIDENTS_ALL_B
group by INCIDENT_ID 
) 
and kca_operation= 'DELETE'
);
commit;


end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate() 
where ods_table_name='cs_incidents_all_b';
commit;