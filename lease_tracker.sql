CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker` AS (
    WITH
    -- Fetches and formats invoice data from GCP
    invoice_data AS (
        SELECT
            id,
            is_deleted,
            name,
            bill_period_end_c AS bill_period_end,
            bill_period_start_c AS bill_period_start,
            billed_amount_c AS billed_amount,
            billing_source_c AS billing_source,
            leasing_request_c AS leasing_request,
            wholesale_credit_rebill_c AS wholesale_credit_rebill,
            wholesale_or_retail_c AS wholesale_or_retail,
            created_date,
            COALESCE(
                invoice_number_c,
                external_invoice_id_c,
                internal_invoice_id_c
            ) AS invoice_number,
            COALESCE(
                billing_status_c,
                external_billing_status_c,
                internal_billing_status_c
            ) AS billing_status
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.invoice_c`
        WHERE
            is_deleted = FALSE
    ),

    -- Replicates a calculation performed in Salesforce but not in GCP
    business_unit_formula AS (
        SELECT
            contract_number_c,
            CASE
                WHEN business_unit_c IN ('Aviation', 'Enterprise', 'Maritime') THEN business_unit_c
                WHEN business_unit_c = 'USG' THEN 'US Government'
                WHEN business_unit_c = 'G2' THEN 'Global Government'
            END AS business_unit
        FROM (
            SELECT DISTINCT
                lr.contract_number_c,
                COALESCE(lr.business_unit_1_c, rt.name) AS business_unit_c
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
        )
    ),

    -- Fetches and formats leasing request data from GCP, performs some calculations
    -- done in the original Alteryx workflow, and joins on the business unit formula
    leasing_request_data AS (
        SELECT
            lr.account_manager_c AS account_manager,
            lr.account_number_c AS account_number,
            lr.approval_document_c AS approval_document,
            lr.approval_document_id_c AS approval_document_id,
            lr.approval_document_status_c AS approval_document_status,
            lr.approval_status_change_date_c AS approval_status_change_date,
            lr.assessment_comments_c AS assessment_comments,
            lr.billing_acknowledgement_of_approval_docu_c
                AS billing_acknowledgement_of_approval_docu,
            lr.bupa_c AS bupa,
            bu.business_unit,
            lr.can_be_accommodated_c AS can_be_accommodated,
            lr.channel_no_2_c AS channel_no_2,
            lr.channel_weeks_charge_c AS channel_weeks_charge,
            lr.channel_weeks_trust_comm_l_tac_c AS channel_weeks_trust_comm_l_tac,
            lr.commercials_confirmed_c AS commercials_confirmed,
            lr.contract_duration_c AS contract_duration,
            lr.contract_number_c AS ssp_number,
            lr.contract_progress_c AS contract_progress,
            lr.contract_progress_comments_c AS contract_progress_comments,
            lr.contract_sent_to_lsp_c AS contract_sent_to_lsp,
            lr.contract_signed_by_lsp_c AS contract_signed_by_lsp,
            lr.contracts_acknowledgement_of_approval_do_c
                AS contracts_acknowledgement_of_approval_do,
            lr.conus_discount_trust_comm_l_tac_c AS conus_discount_trust_comm_l_tac,
            lr.created_date,
            lr.credit_check_comments_c AS credit_check_comments,
            lr.credit_limit_approved_c AS credit_limit_approved,
            lr.credit_rejected_c AS credit_rejected,
            lr.customer_name_c AS customer_name,
            lr.disaster_recovery_charge_c AS disaster_recovery_charge,
            lr.disaster_recovery_with_spectrum_c AS disaster_recovery_with_spectrum,
            lr.disaster_recovery_without_spectrum_c AS disaster_recovery_without_spectrum,
            lr.document_approval_status_changed_date_c AS document_approval_status_changed_date,
            lr.due_date_c AS due_date,
            lr.end_user_organisation_c AS end_user_organisation,
            lr.escalation_reason_c AS escalation_reason,
            lr.forward_bandwidth_k_hz_c AS forward_bandwidth_k_hz,
            lr.forward_data_rates_kbps_c AS forward_data_rates_kbps,
            lr.gx_email_alert_recursion_stop_c AS gx_email_alert_recursion_stop,
            lr.host_radio_3_c AS host_radio_3,
            lr.id,
            lr.initial_request_date_c AS initial_request_date,
            lr.is_this_application_subject_to_c AS is_this_application_subject_to,
            lr.is_deleted,
            lr.isol_gm_c AS isol_gm,
            lr.land_or_maritime_c AS land_aero_maritime,
            lr.last_modified_date,
            lr.lease_end_time_c AS lease_end_time,
            lr.lease_request_number_c AS lease_request_number,
            lr.lease_service_type_c AS service_type,
            lr.lease_start_time_c AS lease_start_time,
            lr.lease_type_c AS lease_type,
            lr.leasing_cc_emails_c AS leasing_cc_emails,
            lr.lsp_c AS lsp,
            lr.lspa_contract_no_c AS lspa_contract_no,
            lr.lsr_c AS lsr,
            lr.name AS end_customer,
            lr.number_of_beams_c AS number_of_beams,
            lr.other_technical_details_required_c AS other_technical_details_required,
            lr.pmp_volume_discount_c AS pmp_volume_discount,
            lr.power_d_bw_c AS power_d_bw,
            lr.pre_sales_required_c AS pre_sales_required,
            lr.price_plan_c AS price_plan,
            lr.pricing_acknowledgement_of_approval_docu_c
                AS pricing_acknowledgement_of_approval_docu,
            lr.primary_les_sas_c AS primary_les_sas,
            lr.project_id_c AS project_id,
            lr.project_name_c AS project_name,
            lr.record_type_id,
            lr.retail_bill_interval_c AS retail_bill_interval,
            lr.retail_bill_interval_comments_c AS retail_bill_interval_comments,
            lr.retail_billing_entered_c AS retail_billing_date_entered,
            lr.retail_contract_value_c AS retail_contract_value,
            lr.retail_periodic_payment_amount_c AS retail_periodic_payment_amount,
            lr.retail_pricing_comments_c AS retail_pricing_comments,
            lr.return_bandwidth_k_hz_c AS return_bandwidth_khz,
            lr.return_data_rates_kbps_c AS return_data_rates_kbps,
            lr.satellite_c AS satellite,
            lr.secondary_les_sas_c AS secondary_les_sas,
            lr.send_lrf_to_ops_c AS send_lrf_to_ops,
            lr.send_to_contracts_c AS send_to_contracts,
            lr.special_comments_c AS special_comments,
            lr.special_terms_and_conditions_applicable_c AS special_terms_and_conditions_applicable,
            lr.spot_beam_equivalent_c AS spot_beam_equivalent,
            lr.spot_beam_equivalent_4_c AS spot_beam_equivalent_4,
            lr.status_changed_date_c AS status_changed_date,
            lr.system_modstamp,
            lr.term_discount_c AS term_discount,
            lr.total_value_of_the_order_c AS total_value_of_the_order,
            lr.trust_comm_weeks_c AS trust_comm_weeks,
            lr.type_of_beam_c AS type_of_beam,
            lr.wholesale_bill_interval_c AS wholesale_bill_interval,
            lr.wholesale_bill_interval_comments_c AS wholesale_bill_interval_comments,
            lr.wholesale_billing_comments_c AS wholesale_billing_comments,
            lr.wholesale_billing_engine_c AS wholesale_billing_engine,
            lr.wholesale_billing_entered_c AS wholesale_billing_entered,
            lr.wholesale_contract_value_c AS wholesale_contract_value,
            lr.wholesale_monthly_minimum_charge_c AS wholesale_monthly_minimum_charge,
            lr.wholesale_or_retail_c AS wholesale_or_retail_formula,
            lr.wholesale_periodic_payment_amount_c AS wholesale_periodic_payment_amount,
            lr.wholesale_pricing_comments_c AS wholesale_pricing_comments,
            lr.billing_completed_c AS billing_completed,
            lr.disaster_recovery_frequency_reservation_c AS disaster_recovery_frequency_reservation,
            lr.forward_bandwidth_mhz_c AS forward_bandwidth_mhz,
            lr.forward_data_rates_mbps_c AS forward_data_rates_mbps,
            lr.reserved_capacity_c AS reserved_capacity,
            lr.return_bandwidth_mhz_c AS return_bandwidth_mhz,
            lr.return_data_rates_mbps_c AS return_data_rates_mbps,
            lr.optional_renewal_years_pricing_the_same_c AS optional_renewal_years_pricing_the_same,
            lr.billing_interval_c AS standard_billing_interval,
            lr.year_10_c AS year_10,
            lr.year_1_c AS year_1,
            lr.year_2_c AS year_2,
            lr.year_3_c AS year_3,
            lr.year_4_c AS year_4,
            lr.year_5_c AS year_5,
            lr.year_6_c AS year_6,
            lr.year_7_c AS year_7,
            lr.year_8_c AS year_8,
            lr.year_9_c AS year_9,
            lr.approval_document_status_date_capture_c AS approval_document_status_date_capture,
            lr.approvl_doc_status_date_capture_empty_c AS approvl_doc_status_date_capture_empty,
            lr.lease_update_status_solution_engineering_c
                AS lease_update_status_solution_engineering,
            lr.lease_update_status_pricing_c AS lease_update_status_pricing,
            lr.type_of_beam_equivalent_c AS type_of_beam_equivalent,
            lr.uid_unbilled_amount_retail_c AS uid_unbilled_amount_retail_formula,
            lr.uid_unbilled_amount_wholesale_c AS uid_unbilled_amount_wholesale_new,
            lr.wholesale_billing_entered_c AS wholesale_billing_date_entered,
            lr.uid_billed_amount_retail_c AS retail_uid_billed_amount,
            lr.uid_billed_amount_wholesale_c AS wholesale_uid_billed_amount,
            lr.po_amount_c AS po_amount,
            DATE(lr.account_period_of_first_invoice_yyyymm_c)
                AS account_period_of_first_wholesale_invoice_yyyymm,
            CURRENT_DATE() AS current_month,
            CASE
                WHEN
                    lr.lsp_c IN ('Inmarsat Government Inc.', 'Inmarsat Solutions (Canada) Inc.')
                    THEN 'Internal'
                ELSE 'External'
            END AS internal_external,
            CASE
                WHEN lr.lease_update_status_c = 'Commercials completed' THEN 1
                WHEN lr.lease_update_status_c = 'Lease Cancelled' THEN 9999999
                WHEN lr.lease_update_status_c = 'Billing' THEN 2
                WHEN lr.lease_update_status_c = 'Solution Engineering' THEN 3
                WHEN lr.lease_update_status_c = 'Pricing' THEN 4
                WHEN lr.lease_update_status_c = 'Contract' THEN 5
                ELSE 1000000000
            END AS lease_update_status_code,
            -- COALESCE()s ensure that the flags in the final SELECT are applied correctly for
            -- null values. Specifically, null NOT IN ('a', 'b', 'c') evaluates to FALSE, whereas
            -- we need this to be TRUE. To do this we substitute nulls with empty strings
            COALESCE(lr.lease_update_status_c, '') AS lease_update_status,
            COALESCE(lr.retail_billing_status_c, '') AS retail_billing_status,
            COALESCE(lr.revenue_recognition_c, '') AS revenue_recognition_basis,
            COALESCE(lr.wholesale_billing_status_c, '') AS wholesale_billing_status,
            CAST(
                CONCAT(
                    CAST(CAST(lr.lease_start_date_c AS DATE) AS STRING),
                    'T',
                    LEFT(CAST(lr.lease_start_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS start_date_of_current_lease,
            CAST(
                CONCAT(
                    CAST(CAST(lr.end_date_c AS DATE) AS STRING),
                    'T',
                    LEFT(CAST(lr.lease_end_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS end_date_of_current_lease,
            CASE
                WHEN CONTAINS_SUBSTR(lr.price_plan_c, 'Take or Pay')
                    OR CONTAINS_SUBSTR(lr.lease_type_c, 'Flex')
                    THEN 'Call Off Lease - Please refer to Call Off Tracker'
                ELSE ''
            END AS call_off_lease,
            CASE
                WHEN CONTAINS_SUBSTR(lr.contract_number_c, 'GX') THEN lr.contract_number_c
            END AS new_ssp_number
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS lr
        LEFT JOIN business_unit_formula AS bu ON lr.contract_number_c = bu.contract_number_c
        WHERE
            lr.contract_number_c IS NOT NULL
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

    -- Joins leasing request and user data and performs a calculation from Excel
    leasing_request_user_data AS (
        SELECT
            lr.*,
            u.account_manager_name,
            u.account_manager_division,
            (
                DATE_DIFF(
                    DATE_ADD(lr.end_date_of_current_lease, INTERVAL 1 DAY),
                    lr.start_date_of_current_lease,
                    DAY
                )
                / 365.2422
            )
            * 12 AS total_no_of_months
        FROM
            leasing_request_data AS lr
        LEFT JOIN user_data AS u ON lr.account_manager = u.user_id
    ),

    -- Billing Data is the first and least processed output of the original Alteryx
    -- workflow. This is not currently used. If needed, this may need to be materialised
    -- in a separate table as it has a different schema to the final output
    billing_data AS (
        SELECT
            lru.ssp_number,
            lru.lsp,
            lru.lsr,
            lru.end_customer,
            lru.account_number,
            lru.land_aero_maritime,
            i.wholesale_or_retail AS invoice_wholesale_or_retail,
            i.invoice_number,
            i.name AS invoice_name,
            i.wholesale_credit_rebill AS invoice_wholesale_credit_rebill,
            i.billing_source AS invoice_billing_source,
            lru.service_type,
            lru.start_date_of_current_lease,
            lru.end_date_of_current_lease,
            i.bill_period_start AS invoice_bill_period_start,
            i.bill_period_end AS invoice_bill_period_end,
            i.billing_status AS invoice_billing_status,
            i.billed_amount AS invoice_billed_amount,
            i.created_date AS invoice_created_date
        FROM
            leasing_request_user_data AS lru
        INNER JOIN invoice_data AS i ON lru.id = i.leasing_request
    ),

    -- Pivot and concatenation performed on retail and wholesale invoice
    -- numbers from billing data to get a single value for each SSP number,
    -- as per the original Alteryx workflow
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
                billing_data
            GROUP BY
                ssp_number,
                invoice_wholesale_or_retail
        ) PIVOT (
            MAX(invoice_number) FOR invoice_wholesale_or_retail IN (
                'Retail' AS retail, 'Wholesale' AS wholesale
            )
        )
    ),

    -- Joins invoice numbers to leasing request and user data
    add_invoice_numbers AS (
        SELECT
            lru.*,
            inv.retail_invoice_id,
            inv.wholesale_invoice_id
        FROM
            leasing_request_user_data AS lru
        LEFT JOIN invoice_numbers AS inv ON lru.ssp_number = inv.ssp_number
    ),

    -- Adds flags allowing the data to be easily filtered to the slices
    -- that populating the different tabs in the original Excel report
    add_data_flags AS (
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
            END AS gx_segovia_flag,
            CASE
                WHEN CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                    AND lsp = 'Inmarsat Solutions (Canada) Inc.'
                    AND revenue_recognition_basis != 'Flex'
                    AND lease_update_status != 'Lease Cancelled'
                    AND retail_billing_status NOT IN (
                        'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                    )
                    AND start_date_of_current_lease <= current_month
                    AND end_date_of_current_lease < current_month THEN 1
                ELSE 0
            END AS call_off_retail_flag,
            CASE
                WHEN CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                    AND lsp != 'Inmarsat Solutions (Canada) Inc.'
                    AND revenue_recognition_basis != 'Flex'
                    AND lease_update_status != 'Lease Cancelled'
                    AND wholesale_billing_status NOT IN (
                        'Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required'
                    )
                    AND start_date_of_current_lease <= current_month
                    AND end_date_of_current_lease < current_month THEN 1
                ELSE 0
            END AS call_off_wholesale_external_flag

        FROM
            add_invoice_numbers
    ),

    -- Calculations that reference only fields output by the query (level 0)
    level_1_calcs AS (
        SELECT
            *,
            LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS accrual_date,

            -- Is Lease within a Single Month
            EXTRACT(YEAR FROM end_date_of_current_lease)
            = EXTRACT(YEAR FROM start_date_of_current_lease)
            AND EXTRACT(MONTH FROM end_date_of_current_lease)
            = EXTRACT(MONTH FROM start_date_of_current_lease)
                AS is_lease_within_single_month,

            -- Is First Calendar Month Whole?
            EXTRACT(DAY FROM start_date_of_current_lease) = 1 AS is_first_calendar_month_whole,

            -- Is Last Calendar Month Whole?
            EXTRACT(DAY FROM end_date_of_current_lease)
            = EXTRACT(DAY FROM LAST_DAY(end_date_of_current_lease)) AS is_last_calendar_month_whole
        FROM add_data_flags -- Replace with your base CTE
    ),

    -- Calculations that reference level 1 calculations and below
    level_2_calcs AS (
        SELECT
            *,
            -- Does Lease Start/End in Consecutive Months
            DATE_DIFF(
                DATE_TRUNC(end_date_of_current_lease, MONTH),
                DATE_TRUNC(DATE_ADD(start_date_of_current_lease, INTERVAL -1 MONTH), MONTH),
                MONTH
            )
            = 1
            AND NOT is_first_calendar_month_whole
            AND NOT is_last_calendar_month_whole AS lease_start_end_consecutive_months
        FROM level_1_calcs
    ),

    -- Calculations that reference level 2 calculations and below
    level_3_calcs AS (
        SELECT
            *,
            -- Lease contains Whole Calendar Month
            NOT is_lease_within_single_month
            AND NOT lease_start_end_consecutive_months
            AND is_first_calendar_month_whole
            AND is_last_calendar_month_whole AS lease_contains_whole_calendar_month
        FROM level_2_calcs
    ),

    -- Calculations that reference level 3 calculations and below
    level_4_calcs AS (
        SELECT
            *,
            -- Date of First Whole Month
            CASE
                WHEN lease_contains_whole_calendar_month THEN
                    CASE WHEN is_first_calendar_month_whole THEN start_date_of_current_lease
                        ELSE DATE_TRUNC(
                                DATE_ADD(start_date_of_current_lease, INTERVAL 1 MONTH), MONTH
                            )
                    END
            END AS date_of_first_whole_month,

            -- Date of Last Whole Month
            CASE
                WHEN lease_contains_whole_calendar_month THEN
                    CASE WHEN is_last_calendar_month_whole THEN end_date_of_current_lease
                        ELSE DATE_TRUNC(
                                DATE_SUB(end_date_of_current_lease, INTERVAL 1 MONTH), MONTH
                            )
                    END
            END AS date_of_last_whole_month,

            -- Count of Consecutive Months
            DATE_DIFF(
                DATE_TRUNC(end_date_of_current_lease, MONTH),
                DATE_TRUNC(DATE_ADD(start_date_of_current_lease, INTERVAL -1 MONTH), MONTH),
                MONTH
            )
                AS count_of_consecutive_months
        FROM level_3_calcs
    ),

    -- Calculations that reference level 4 calculations and below
    level_5_calcs AS (
        SELECT
            *,
            -- Count of Whole Calendar Months
            CASE
                WHEN lease_contains_whole_calendar_month
                    THEN
                        DATE_DIFF(date_of_last_whole_month, date_of_first_whole_month, MONTH) + 1
                ELSE 0
            END AS count_of_whole_calendar_months,

            -- Wholesale Whole Month Value
            CASE
                WHEN
                    lease_contains_whole_calendar_month
                    THEN wholesale_contract_value / total_no_of_months
                ELSE 0
            END AS wholesale_whole_month_value,

            -- Retail Whole Month Value
            CASE
                WHEN lsp = 'Inmarsat Solutions (Canada) Inc.'
                    AND lease_contains_whole_calendar_month
                    AND NOT lease_start_end_consecutive_months
                    THEN retail_contract_value / total_no_of_months
                ELSE 0
            END AS retail_whole_month_value,

            -- Count of Days in Split First Month
            CASE
                WHEN NOT is_lease_within_single_month AND NOT is_first_calendar_month_whole
                    THEN
                        DATE_DIFF(
                            LAST_DAY(start_date_of_current_lease), start_date_of_current_lease, DAY
                        )
                        + 1
                WHEN is_lease_within_single_month AND NOT is_first_calendar_month_whole THEN
                    DATE_DIFF(end_date_of_current_lease, start_date_of_current_lease, DAY) + 1
            END AS count_of_days_in_split_first_month,

            -- Count of Days in Split Last Month
            CASE
                WHEN NOT is_lease_within_single_month AND NOT is_last_calendar_month_whole
                    THEN
                        EXTRACT(DAY FROM end_date_of_current_lease)
                WHEN is_lease_within_single_month AND NOT is_last_calendar_month_whole THEN
                    DATE_DIFF(end_date_of_current_lease, start_date_of_current_lease, DAY) + 1
            END AS count_of_days_in_split_last_month
        FROM level_4_calcs
    ),

    -- Calculations that reference level 5 calculations and below
    level_6_calcs AS (
        SELECT
            *,
            -- Wholesale Total Whole Months Value
            CASE
                WHEN
                    lease_contains_whole_calendar_month
                    THEN count_of_whole_calendar_months * wholesale_whole_month_value
                ELSE 0
            END AS wholesale_total_whole_months_value,

            -- Retail Total Whole Months Value
            CASE
                WHEN
                    lease_contains_whole_calendar_month
                    THEN count_of_whole_calendar_months * retail_whole_month_value
                ELSE 0
            END AS retail_total_whole_months_value,

            -- Total Split Days
            CASE
                WHEN
                    count_of_days_in_split_first_month IS NULL
                    THEN count_of_days_in_split_last_month
                WHEN
                    count_of_days_in_split_last_month IS NULL
                    THEN count_of_days_in_split_first_month
                ELSE count_of_days_in_split_first_month + count_of_days_in_split_last_month
            END AS total_split_days
        FROM level_5_calcs
    ),

    -- Calculations that reference level 6 calculations and below
    level_7_calcs AS (
        SELECT
            *,
            -- Wholesale Remainder Value
            wholesale_contract_value
            - wholesale_total_whole_months_value AS wholesale_remainder_value,

            -- Retail Remainder Value
            retail_contract_value - retail_total_whole_months_value AS retail_remainder_value,

            -- First Month Split Percentage
            CASE
                WHEN is_lease_within_single_month THEN 1
                ELSE COALESCE(count_of_days_in_split_first_month / total_split_days, 0)
            END AS first_month_split_percentage,

            -- Last Month Split Percentage
            CASE
                WHEN is_lease_within_single_month THEN 0
                ELSE COALESCE(count_of_days_in_split_last_month / total_split_days, 0)
            END AS last_month_split_percentage
        FROM level_6_calcs
    ),

    -- Calculations that reference level 7 calculations and below
    level_8_calcs AS (
        SELECT
            *,
            -- Wholesale First Month Value
            wholesale_remainder_value * first_month_split_percentage AS wholesale_first_month_value,

            -- Retail First Month Value
            retail_remainder_value * first_month_split_percentage AS retail_first_month_value,

            -- Wholesale Last Month Value
            wholesale_remainder_value * last_month_split_percentage AS wholesale_last_month_value,

            -- Retail Last Month Value
            retail_remainder_value * last_month_split_percentage AS retail_last_month_value,

            -- Count of Whole Months Past
            CASE
                WHEN date_of_first_whole_month < accrual_date
                    THEN
                        DATE_DIFF(accrual_date, date_of_first_whole_month, MONTH) + 1
                ELSE 0
            END AS count_of_whole_months_past
        FROM level_7_calcs
    )

    SELECT
        *
    FROM level_8_calcs
);
