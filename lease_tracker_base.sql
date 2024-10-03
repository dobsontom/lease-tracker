CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker_base` AS (
    WITH
    -- Fetches and formats invoice data from GCP
    invoice_data AS (
        SELECT
            id AS ssp_number,
            leasing_request_c AS leasing_request,
            name AS invoice_name,
            bill_period_end_c AS invoice_bill_period_end,
            bill_period_start_c AS invoice_bill_period_start,
            billed_amount_c AS invoice_billed_amount,
            billing_source_c AS invoice_billing_source,
            wholesale_credit_rebill_c AS invoice_wholesale_credit_rebill,
            wholesale_or_retail_c AS invoice_wholesale_or_retail,
            created_date AS invoice_created_date,
            COALESCE(
                invoice_number_c,
                external_invoice_id_c,
                internal_invoice_id_c
            ) AS invoice_number,
            COALESCE(
                billing_status_c,
                external_billing_status_c,
                internal_billing_status_c
            ) AS invoice_billing_status
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.invoice_c`
        WHERE
            is_deleted = FALSE
    ),

    -- Pivot and concatenation performed on retail and wholesale invoice
    -- numbers to get a single value for each SSP number
    invoice_numbers AS (
        SELECT
            ssp_number,
            retail AS retail_invoice_id,
            wholesale AS wholesale_invoice_id
        FROM (
            SELECT
                ssp_number,
                invoice_wholesale_or_retail,
                STRING_AGG(DISTINCT invoice_number, '; ') AS invoice_number
            FROM
                invoice_data
            GROUP BY
                ssp_number,
                invoice_wholesale_or_retail
        ) PIVOT (
            MAX(invoice_number) FOR invoice_wholesale_or_retail IN (
                'Retail' AS retail, 'Wholesale' AS wholesale
            )
        )
    ),

    -- Replicates a calculation performed in Salesforce but not in GCP
    business_unit_formula AS (
        SELECT DISTINCT
            contract_number_c,
            CASE
                WHEN
                    business_unit_c IN ('Aviation', 'Enterprise', 'Maritime', 'US Government', 'Global Government')
                    THEN business_unit_c
                WHEN business_unit_c = 'Air' THEN 'Aviation'
                WHEN business_unit_c = 'Land' THEN 'Enterprise'
                WHEN business_unit_c = 'Sea' THEN 'Maritime'
                WHEN business_unit_c IN ('USG', 'US Gov', 'US Govt') THEN 'US Government'
                WHEN business_unit_c IN ('G2', 'Global', 'Global Gov', 'Global Govt') THEN 'Global Government'
            END AS business_unit
        FROM (
            SELECT
                lr.contract_number_c,
                COALESCE(lr.business_unit_c, lr.business_unit_1_c, rt.name) AS business_unit_c
            FROM
                `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS lr
            LEFT JOIN
                `inm-iar-data-warehouse-dev.sdp_salesforce_src.account` AS acc
                ON lr.account_c = acc.id
            LEFT JOIN
                `inm-iar-data-warehouse-dev.sdp_salesforce_src.record_type` AS rt
                ON acc.record_type_id = rt.id
            WHERE
                lr.contract_number_c IS NOT NULL
                AND COALESCE(lr.business_unit_c, lr.business_unit_1_c, rt.name) IS NOT NULL
        )
    ),

    -- Fetches and formats leasing request data from GCP, performs some calculations
    -- done in the original Alteryx workflow, and joins on the business unit formula
    lease_data AS (
        SELECT
            ld.account_manager_c AS account_manager,
            ld.account_number_c AS account_number,
            ld.approval_document_c AS approval_document,
            ld.approval_document_id_c AS approval_document_id,
            ld.approval_document_status_c AS approval_document_status,
            ld.approval_status_change_date_c AS approval_status_change_date,
            ld.assessment_comments_c AS assessment_comments,
            ld.billing_acknowledgement_of_approval_docu_c
                AS billing_acknowledgement_of_approval_docu,
            ld.bupa_c AS bupa,
            bu.business_unit,
            ld.can_be_accommodated_c AS can_be_accommodated,
            ld.channel_no_2_c AS channel_no_2,
            ld.channel_weeks_charge_c AS channel_weeks_charge,
            ld.channel_weeks_trust_comm_l_tac_c AS channel_weeks_trust_comm_l_tac,
            ld.commercials_confirmed_c AS commercials_confirmed,
            ld.contract_duration_c AS contract_duration,
            ld.contract_number_c AS ssp_number,
            ld.contract_progress_c AS contract_progress,
            ld.contract_progress_comments_c AS contract_progress_comments,
            ld.contract_sent_to_lsp_c AS contract_sent_to_lsp,
            ld.contract_signed_by_lsp_c AS contract_signed_by_lsp,
            ld.contracts_acknowledgement_of_approval_do_c
                AS contracts_acknowledgement_of_approval_do,
            ld.conus_discount_trust_comm_l_tac_c AS conus_discount_trust_comm_l_tac,
            ld.created_date,
            ld.credit_check_comments_c AS credit_check_comments,
            ld.credit_limit_approved_c AS credit_limit_approved,
            ld.credit_rejected_c AS credit_rejected,
            ld.customer_name_c AS customer_name,
            ld.disaster_recovery_charge_c AS disaster_recovery_charge,
            ld.disaster_recovery_with_spectrum_c AS disaster_recovery_with_spectrum,
            ld.disaster_recovery_without_spectrum_c AS disaster_recovery_without_spectrum,
            ld.document_approval_status_changed_date_c AS document_approval_status_changed_date,
            ld.due_date_c AS due_date,
            ld.end_user_organisation_c AS end_user_organisation,
            ld.escalation_reason_c AS escalation_reason,
            ld.forward_bandwidth_k_hz_c AS forward_bandwidth_k_hz,
            ld.forward_bandwidth_k_hz_2_c AS forward_bandwidth_khz_2,
            ld.forward_bandwidth_k_hz_3_c AS forward_bandwidth_khz_3,
            ld.forward_bandwidth_k_hz_4_c AS forward_bandwidth_khz_4,
            ld.forward_data_rates_kbps_c AS forward_data_rates_kbps,
            ld.gx_email_alert_recursion_stop_c AS gx_email_alert_recursion_stop,
            ld.host_radio_3_c AS host_radio_3,
            ld.id,
            ld.initial_request_date_c AS initial_request_date,
            ld.is_this_application_subject_to_c AS is_this_application_subject_to,
            ld.is_deleted,
            ld.isol_gm_c AS isol_gm,
            ld.land_or_maritime_c AS land_aero_maritime,
            ld.last_modified_date,
            ld.lease_end_time_c AS lease_end_time,
            ld.lease_request_number_c AS lease_request_number,
            ld.lease_service_type_c AS service_type,
            ld.lease_start_time_c AS lease_start_time,
            ld.lease_type_c AS lease_type,
            ld.leasing_cc_emails_c AS leasing_cc_emails,
            ld.lsp_c AS lsp,
            ld.lspa_contract_no_c AS lspa_contract_no,
            ld.lsr_c AS lsr,
            ld.name AS end_customer,
            ld.number_of_beams_c AS number_of_beams,
            ld.other_technical_details_required_c AS other_technical_details_required,
            ld.pmp_volume_discount_c AS pmp_volume_discount,
            ld.power_d_bw_c AS power_d_bw,
            ld.pre_sales_required_c AS pre_sales_required,
            ld.price_plan_c AS price_plan,
            ld.pricing_acknowledgement_of_approval_docu_c
                AS pricing_acknowledgement_of_approval_docu,
            ld.primary_les_sas_c AS primary_les_sas,
            ld.project_id_c AS project_id,
            ld.project_name_c AS project_name,
            ld.record_type_id,
            ld.retail_bill_interval_c AS retail_bill_interval,
            ld.retail_bill_interval_comments_c AS retail_bill_interval_comments,
            ld.retail_billing_entered_c AS retail_billing_date_entered,
            ld.retail_contract_value_c AS retail_contract_value,
            ld.retail_periodic_payment_amount_c AS retail_periodic_payment_amount,
            ld.retail_pricing_comments_c AS retail_pricing_comments,
            ld.return_bandwidth_k_hz_c AS return_bandwidth_khz,
            ld.return_bandwidth_k_hz_2_c AS return_bandwidth_khz_2,
            ld.return_bandwidth_k_hz_3_c AS return_bandwidth_khz_3,
            ld.return_bandwidth_k_hz_4_c AS return_bandwidth_khz_4,
            ld.return_data_rates_kbps_c AS return_data_rates_kbps,
            ld.satellite_c AS satellite,
            ld.satellite_2_c AS satellite_2,
            ld.satellite_3_c AS satellite_3,
            ld.satellite_4_c AS satellite_4,
            ld.secondary_les_sas_c AS secondary_les_sas,
            ld.send_lrf_to_ops_c AS send_lrf_to_ops,
            ld.send_to_contracts_c AS send_to_contracts,
            ld.special_comments_c AS special_comments,
            ld.special_terms_and_conditions_applicable_c AS special_terms_and_conditions_applicable,
            ld.spot_beam_equivalent_c AS spot_beam_equivalent,
            ld.spot_beam_equivalent_4_c AS spot_beam_equivalent_4,
            ld.status_changed_date_c AS status_changed_date,
            ld.system_modstamp,
            ld.term_discount_c AS term_discount,
            ld.total_value_of_the_order_c AS total_value_of_the_order,
            ld.trust_comm_weeks_c AS trust_comm_weeks,
            ld.type_of_beam_c AS type_of_beam,
            ld.wholesale_bill_interval_c AS wholesale_bill_interval,
            ld.wholesale_bill_interval_comments_c AS wholesale_bill_interval_comments,
            ld.wholesale_billing_comments_c AS wholesale_billing_comments,
            ld.wholesale_billing_engine_c AS wholesale_billing_engine,
            ld.wholesale_billing_entered_c AS wholesale_billing_entered,
            ld.wholesale_contract_value_c AS wholesale_contract_value,
            ld.wholesale_monthly_minimum_charge_c AS wholesale_monthly_minimum_charge,
            ld.wholesale_or_retail_c AS wholesale_or_retail_formula,
            ld.wholesale_periodic_payment_amount_c AS wholesale_periodic_payment_amount,
            ld.wholesale_pricing_comments_c AS wholesale_pricing_comments,
            ld.billing_completed_c AS billing_completed,
            ld.disaster_recovery_frequency_reservation_c AS disaster_recovery_frequency_reservation,
            ld.forward_bandwidth_mhz_c AS forward_bandwidth_mhz,
            ld.forward_data_rates_mbps_c AS forward_data_rates_mbps,
            ld.reserved_capacity_c AS reserved_capacity,
            ld.return_bandwidth_mhz_c AS return_bandwidth_mhz,
            ld.return_data_rates_mbps_c AS return_data_rates_mbps,
            ld.optional_renewal_years_pricing_the_same_c AS optional_renewal_years_pricing_the_same,
            ld.billing_interval_c AS standard_billing_interval,
            ld.year_10_c AS year_10,
            ld.year_1_c AS year_1,
            ld.year_2_c AS year_2,
            ld.year_3_c AS year_3,
            ld.year_4_c AS year_4,
            ld.year_5_c AS year_5,
            ld.year_6_c AS year_6,
            ld.year_7_c AS year_7,
            ld.year_8_c AS year_8,
            ld.year_9_c AS year_9,
            ld.approval_document_status_date_capture_c AS approval_document_status_date_capture,
            ld.approvl_doc_status_date_capture_empty_c AS approvl_doc_status_date_capture_empty,
            ld.lease_update_status_solution_engineering_c
                AS lease_update_status_solution_engineering,
            ld.lease_update_status_pricing_c AS lease_update_status_pricing,
            ld.type_of_beam_equivalent_c AS type_of_beam_equivalent,
            ld.uid_unbilled_amount_retail_c AS uid_unbilled_amount_retail_formula,
            ld.uid_unbilled_amount_wholesale_c AS uid_unbilled_amount_wholesale_new,
            ld.wholesale_billing_entered_c AS wholesale_billing_date_entered,
            ld.uid_billed_amount_retail_c AS retail_uid_billed_amount,
            ld.uid_billed_amount_wholesale_c AS wholesale_uid_billed_amount,
            ld.po_amount_c AS po_amount,
            DATE(ld.account_period_of_first_invoice_yyyymm_c)
                AS account_period_of_first_wholesale_invoice_yyyymm,
            CURRENT_DATE() AS current_month,
            CURRENT_TIMESTAMP() AS last_refresh_time,
            LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS accrual_date,
            CASE
                WHEN
                    ld.lsp_c IN ('Inmarsat Government Inc.', 'Inmarsat Solutions (Canada) Inc.')
                    THEN 'Internal'
                ELSE 'External'
            END AS internal_external,
            CASE
                WHEN ld.lease_update_status_c = 'Commercials completed' THEN 1
                WHEN ld.lease_update_status_c = 'Lease Cancelled' THEN 9999999
                WHEN ld.lease_update_status_c = 'Billing' THEN 2
                WHEN ld.lease_update_status_c = 'Solution Engineering' THEN 3
                WHEN ld.lease_update_status_c = 'Pricing' THEN 4
                WHEN ld.lease_update_status_c = 'Contract' THEN 5
                ELSE 1000000000
            END AS lease_update_status_code,
            -- COALESCE()s ensure that the flags in the final SELECT are applied correctly for
            -- null values. Specifically, null NOT IN ('a', 'b', 'c') evaluates to FALSE, whereas
            -- we need this to be TRUE. To do this we substitute nulls with empty strings
            COALESCE(ld.lease_update_status_c, '') AS lease_update_status,
            COALESCE(ld.retail_billing_status_c, '') AS retail_billing_status,
            COALESCE(ld.revenue_recognition_c, '') AS revenue_recognition_basis,
            COALESCE(ld.wholesale_billing_status_c, '') AS wholesale_billing_status,
            CAST(
                CONCAT(
                    CAST(CAST(ld.lease_start_date_c AS DATE) AS STRING),
                    'T',
                    LEFT(CAST(ld.lease_start_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS start_date_of_current_lease,
            CAST(
                CONCAT(
                    CAST(CAST(ld.end_date_c AS DATE) AS STRING),
                    'T',
                    LEFT(CAST(ld.lease_end_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS end_date_of_current_lease,
            CASE
                WHEN CONTAINS_SUBSTR(ld.price_plan_c, 'Take or Pay')
                    OR CONTAINS_SUBSTR(ld.lease_type_c, 'Flex')
                    THEN 'Call Off Lease - Please refer to Call Off Tracker'
                ELSE ''
            END AS call_off_lease,
            CASE
                WHEN CONTAINS_SUBSTR(ld.contract_number_c, 'GX') THEN ld.contract_number_c
            END AS new_ssp_number
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS ld
        LEFT JOIN business_unit_formula AS bu ON ld.contract_number_c = bu.contract_number_c
        WHERE
            ld.contract_number_c IS NOT NULL
    ),

    -- Fetches and formats user data from GCP
    user_data AS (
        SELECT
            id AS user_id,
            name AS account_manager_name,
            division AS account_manager_division
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.user`
    ),

    -- Joins leasing request and user data
    lease_user_data AS (
        SELECT
            ld.*,
            u.account_manager_name,
            u.account_manager_division
        FROM
            lease_data AS ld
        LEFT JOIN user_data AS u ON ld.account_manager = u.user_id
    ),


    -- Joins invoice data to leasing request and user data
    add_invoice_data AS (
        SELECT
            lru.*,
            nums.retail_invoice_id,
            nums.wholesale_invoice_id,
            inv.invoice_wholesale_or_retail,
            inv.invoice_number,
            inv.invoice_name,
            inv.invoice_wholesale_credit_rebill,
            inv.invoice_billing_source,
            inv.invoice_bill_period_start,
            inv.invoice_bill_period_end,
            inv.invoice_billing_status,
            inv.invoice_billed_amount,
            inv.invoice_created_date
        FROM
            lease_user_data AS lru
        LEFT JOIN invoice_data AS inv ON lru.id = inv.leasing_request
        LEFT JOIN invoice_numbers AS nums ON inv.ssp_number = nums.ssp_number
    )

    SELECT
        *,
        CASE
            WHEN lease_update_status != 'Lease Cancelled' THEN 1
            ELSE 0
        END AS raw_data_flag,
        CASE
            WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND internal_external = 'External'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status NOT IN (
                    'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND NOT CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
            ELSE 0
        END AS wholesale_external_flag,
        CASE
            WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND internal_external = 'External'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status NOT IN (
                    'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
            ELSE 0
        END AS gx_wholesale_external_flag,
        CASE
            WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND internal_external = 'Internal'
                AND lsp = 'Inmarsat Solutions (Canada) Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND retail_billing_status NOT IN (
                    'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free') THEN 1
            ELSE 0
        END AS retail_flag,
        CASE
            WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND lsp = 'Inmarsat Government Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status NOT IN (
                    'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND NOT CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
            ELSE 0
        END AS segovia_flag,
        CASE
            WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND lsp = 'Inmarsat Government Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status NOT IN (
                    'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
            ELSE 0
        END AS gx_segovia_flag
    FROM add_invoice_data
);
