CREATE OR REPLACE TABLE
    inm-iar-data-warehouse-dev.lease_tracker.lease_tracker AS (
        WITH
            invoice_c AS (
                SELECT
                    id,
                    is_deleted,
                    name,
                    created_date,
                    created_by_id,
                    last_modified_date,
                    last_modified_by_id,
                    system_modstamp,
                    connection_received_id,
                    connection_sent_id,
                    bill_period_end_c,
                    bill_period_start_c,
                    billed_amount_c,
                    billing_source_c,
                    billing_status_c,
                    invoice_number_c,
                    leasing_request_c,
                    wholesale_credit_rebill_c,
                    wholesale_or_retail_c,
                    IFNULL(
                        invoice_number_c,
                        (
                            IFNULL(external_invoice_id_c, internal_invoice_id_c)
                        )
                    ) AS invoice_number,
                    IFNULL(
                        billing_status_c,
                        (
                            IFNULL(
                                external_billing_status_c,
                                internal_billing_status_c
                            )
                        )
                    ) AS billing_status
                FROM
                    inm-iar-data-warehouse-dev.sdp_salesforce_src.invoice_c
                WHERE
                    is_deleted = FALSE
            ),
            -- Fields that are commented out are selected from Salesforce in
            -- the original workflow, but are not readily available in the
            -- leasing_request_c GCP table.
            leasing_request_c AS (
                SELECT
                    account_number_c,
                    account_period_of_first_invoice_yyyymm_c,
                    approval_document_c,
                    approval_document_id_c AS approval_document_id,
                    approval_document_status_c,
                    approval_status_change_date_c,
                    assessment_comments_c,
                    billing_acknowledgement_of_approval_docu_c,
                    bupa_c,
                    business_unit_1_c AS business_unit,
                    can_be_accommodated_c,
                    channel_no_2_c,
                    channel_weeks_charge_c,
                    channel_weeks_trust_comm_l_tac_c,
                    commercials_confirmed_c,
                    -- company_id_c,
                    -- consumed_amount_c,
                    -- contract_daily_rate_c,
                    contract_duration_c,
                    contract_number_c AS ssp_number,
                    contract_progress_c,
                    contract_progress_comments_c,
                    contract_sent_to_lsp_c,
                    contract_signed_by_lsp_c,
                    -- contract_sla_c,
                    contracts_acknowledgement_of_approval_do_c conus_discount_trustcomm_l_tac_c,
                    -- count_c,
                    created_date,
                    credit_check_comments_c,
                    credit_limit_approved_c,
                    credit_rejected_c,
                    -- credit_sla_c,
                    customer_name_c,
                    disaster_recovery_charge_c,
                    disaster_recovery_with_spectrum_c,
                    disaster_recovery_without_spectrum_c,
                    document_approval_status_changed_date_c,
                    due_date_c,
                    end_user_organisation_c,
                    escalation_reason_c,
                    -- external_sale_profit_center_c,
                    forward_bandwidth_k_hz_c,
                    forward_data_rates_kbps_c,
                    gx_email_alert_recursion_stop_c,
                    host_radio_3_c,
                    initial_request_date_c,
                    internal_external_c AS internal_external,
                    -- internal_sale_profit_center,
                    is_this_application_subject_to_c,
                    is_deleted,
                    isol_gm_c,
                    land_or_maritime_c AS land_aero_maritime,
                    last_modified_date,
                    lease_end_time_c,
                    lease_request_number_c,
                    lease_service_type_c AS service_type,
                    lease_start_time_c,
                    lease_type_c,
                    lease_update_status_c AS lease_update_status,
                    leasing_cc_emails_c,
                    lsp_c AS lsp,
                    lspa_contract_no_c,
                    lsr_c AS lsr,
                    -- material_code_c,
                    name AS end_customer,
                    number_of_beams_c,
                    other_technical_details_required_c,
                    pmp_volume_discount_c,
                    power_d_bw_c,
                    pre_sales_required_c,
                    price_plan_c,
                    pricing_acknowledgement_of_approval_docu_c,
                    -- pricing_sla_c,
                    primary_les_sas_c,
                    project_id_c,
                    project_name_c,
                    record_type_id,
                    -- retail_account_codes_c,
                    retail_bill_interval_c AS retail_bill_interval,
                    retail_bill_interval_comments_c,
                    retail_billing_entered_c,
                    retail_billing_status_c,
                    -- retail_consumed_amount_c,
                    -- retail_consumed_amount_per_c,
                    retail_contract_value_c AS retail_contract_value,
                    -- retail_margin_c,
                    retail_periodic_payment_amount_c AS retail_periodic_payment_amount,
                    retail_pricing_comments_c AS retail_pricing_comments,
                    return_bandwidth_k_hz_c,
                    return_data_rates_kbps_c,
                    revenue_recognition_c AS revenue_recognition_basis,
                    satellite_c,
                    secondary_les_sas_c,
                    send_lrf_to_ops_c,
                    send_to_contracts_c,
                    special_comments_c,
                    special_terms_and_conditions_applicable_c,
                    spot_beam_equivalent_c,
                    spot_beam_equivalent_4_c,
                    status_changed_date_c,
                    -- status_report_c,
                    system_modstamp,
                    -- technical_bd_sla_c,
                    term_discount_c,
                    total_value_of_the_order_c,
                    trust_comm_weeks_c,
                    type_of_beam_c,
                    -- wholesale_account_code_c,
                    wholesale_bill_interval_c AS wholesale_bill_interval,
                    wholesale_bill_interval_comments_c,
                    wholesale_billing_comments_c,
                    wholesale_billing_engine_c,
                    wholesale_billing_entered_c,
                    wholesale_billing_status_c,
                    wholesale_contract_value_c AS wholesale_contract_value,
                    wholesale_monthly_minimum_charge_c,
                    wholesale_or_retail_c AS wholease_or_retail_formula_c,
                    wholesale_periodic_payment_amount_c AS wholesale_periodic_payment_amount,
                    wholesale_pricing_comments_c AS wholesale_pricing_comments,
                    billing_completed_c,
                    -- contract_days_time_difference_c,
                    -- contract_no_of_days_c,
                    -- contract_date_difference_c,
                    disaster_recovery_frequency_reservation_c,
                    forward_bandwidth_mhz_c,
                    forward_data_rates_mbps_c,
                    -- isol_gm_formula_c,
                    reserved_capacity_c,
                    -- retail_consumed_amount_left_c,
                    return_bandwidth_mhz_c,
                    return_data_rates_mbps_c,
                    -- wholesale_consumed_amount_left_c,
                    optional_renewal_years_pricing_the_same_c,
                    billing_interval_c AS standard_billing_interval_c,
                    year_10_c,
                    year_1_c,
                    year_2_c,
                    year_3_c,
                    year_4_c,
                    year_5_c,
                    year_6_c,
                    year_7_c,
                    year_8_c,
                    year_9_c,
                    -- retail_contract_daily_value_c,
                    -- wholesale_contract_daily_value_c,
                    -- approval_doc_status_empty_to_value_c,
                    approval_document_status_date_capture_c,
                    approvl_doc_status_date_capture_empty_c,
                    -- completed_in_billing_sla_c,
                    -- lease_status_solution_to_pricing_timedif_c,
                    lease_update_status_solution_engineering_c,
                    lease_update_status_pricing_c,
                    -- pg_bd_sla_c,
                    -- retail_billing_completed_in_sla_c,
                    type_of_beam_equivalent_c,
                    uid_unbilled_amount_retail_c AS uid_unbilled_amount_retail_formula_c,
                    uid_unbilled_amount_wholesale_c AS uid_unbilled_amount_wholesale_new_c,
                    wholesale_billing_entered_c AS wholesale_billing_entered_day_c,
                    uid_billed_amount_retail_c AS uid_billed_amount_retail_rollup_c,
                    uid_billed_amount_wholesale_c AS uid_billed_amount_wholesale_rollup_c,
                    po_amount_c,
                    CAST(
                        CONCAT(
                            CAST(CAST(lease_start_date_c AS DATE) AS STRING),
                            'T',
                            LEFT(CAST(lease_start_time_c AS STRING), 12)
                        ) AS DATETIME
                    ) AS start_date_of_current_lease,
                    CAST(
                        CONCAT(
                            CAST(CAST(end_date_c AS DATE) AS STRING),
                            'T',
                            LEFT(CAST(lease_end_time_c AS STRING), 12)
                        ) AS DATETIME
                    ) AS end_date_of_current_lease,
                    account_manager_c AS zcode_account_manager,
                    id AS zcode_id,
                    CASE
                        WHEN CONTAINS_SUBSTR(price_plan_c, 'Take or Pay')
                        OR CONTAINS_SUBSTR(lease_type_c, 'Flex') THEN 'Call Off Lease - Please refer to Call Off Tracker'
                        ELSE ''
                    END AS call_off_lease
                FROM
                    inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c
                WHERE
                    contract_number_c IS NOT NULL
            ),
            user AS (
                SELECT
                    id AS user_id,
                    name AS account_manager,
                    division AS account_manager_division
                FROM
                    inm-iar-data-warehouse-dev.sdp_salesforce_src.user
            ),
            leasing_request_user AS (
                SELECT
                    *
                FROM
                    leasing_request_c lr
                    LEFT JOIN user u ON lr.zcode_account_manager = u.user_id
            ),
            -- Billing data is the first and least processed output of the original
            -- workflow. In needed, this may need to be used to create a separate table
            -- as it has a distinct format to the final output.
            billing_data AS (
                SELECT
                    lru.ssp_number,
                    lru.lsp,
                    lru.lsr,
                    lru.end_customer,
                    lru.account_number_c AS account_number,
                    lru.land_aero_maritime,
                    i.wholesale_or_retail_c AS invoice_wholesale_or_retail,
                    i.invoice_number,
                    i.name AS invoice_name,
                    i.wholesale_credit_rebill_c AS invoice_wholesale_credit_rebill,
                    i.billing_source_c AS invoice_billing_source,
                    lru.service_type,
                    lru.start_date_of_current_lease,
                    lru.end_date_of_current_lease,
                    i.bill_period_start_c AS invoice_bill_period_start,
                    i.bill_period_end_c AS invoice_bill_period_end,
                    i.billing_status_c AS invoice_billing_status,
                    i.billed_amount_c AS invoice_billed_amount,
                    i.created_date AS invoice_created_date
                FROM
                    leasing_request_user lru
                    JOIN invoice_c i ON lru.zcode_id = i.leasing_request_c
            ),
            -- Pivot and concatenation performed on retail and wholesale invoice
            -- numbers to get a single value for each SSP number, as per the
            -- the original workflow.
            retail_wholesale_invoice_nos AS (
                SELECT
                    ssp_number,
                    retail AS retail_invoice_id,
                    wholesale AS wholesale_invoice_id
                FROM
                    (
                        SELECT
                            *
                        FROM
                            billing_data
                    ) PIVOT(
                        STRING_AGG(invoice_number, '; ')
                        FOR invoice_wholesale_or_retail IN ('Retail', 'Wholesale')
                    )
            ),
            -- Final output creates the data format that populates the various
            -- tabs of the original Excel spreadsheet. The exception is that
            -- that each tab contains a subset of fields, whereas here all fields
            -- are retained.
            final_output AS (
                SELECT
                    rwinv.retail_invoice_id,
                    rwinv.wholesale_invoice_id,
                    lru.*,
                    CASE
                        WHEN lease_update_status = 'Commercials completed' THEN 1
                        WHEN lease_update_status = 'Lease Cancelled' THEN 9999999
                        WHEN lease_update_status = 'Billing' THEN 2
                        WHEN lease_update_status = 'Solution Engineering' THEN 3
                        WHEN lease_update_status = 'Pricing' THEN 4
                        WHEN lease_update_status = 'Contract' THEN 5
                        ELSE 1000000000
                    END AS lease_update_status_code,
                    CASE
                        WHEN CONTAINS_SUBSTR(lru.ssp_number, 'GX') THEN lru.ssp_number
                        ELSE NULL
                    END AS new_ssp_number,
                    CURRENT_DATE AS current_month
                FROM
                    leasing_request_user lru
                    LEFT JOIN retail_wholesale_invoice_nos rwinv ON lru.ssp_number = rwinv.ssp_number
                ORDER BY
                    ssp_number ASC,
                    lease_update_status_code ASC
            )
            -- The final statement adds flags to the final output allowing
            -- the data to be easily filtered to the subsets of data populating
            -- the different tabs of the original Excel workbook.
        SELECT
            *,
            CASE
                WHEN lease_update_status != 'Lease Cancelled' THEN 1
                ELSE 0
            END AS raw_data_tab_ind,
            CASE
                WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND internal_external = 'External'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND NOT CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
                ELSE 0
            END AS whs_external_tab_ind,
            CASE
                WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND internal_external = 'External'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
                ELSE 0
            END AS gx_wholesale_external_tab_ind,
            CASE
                WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND internal_external = 'Internal'
                AND lsp = 'Inmarsat Solutions (Canada) Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free') THEN 1
                ELSE 0
            END AS rtl_tab_ind,
            CASE
                WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND lsp = 'Inmarsat Government Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND NOT CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
                ELSE 0
            END AS segovia_tab_ind,
            CASE
                WHEN NOT CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND lsp = 'Inmarsat Government Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease <= current_month
                AND NOT CONTAINS_SUBSTR(ssp_number, 'Free')
                AND CONTAINS_SUBSTR(ssp_number, 'GXL') THEN 1
                ELSE 0
            END AS gx_segovia_tab_ind,
            CASE
                WHEN CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND lsp = 'Inmarsat Solutions (Canada) Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease < current_month
                AND end_date_of_current_lease < current_month THEN 1
                ELSE 0
            END AS call_off_rtl_tab_ind,
            CASE
                WHEN CONTAINS_SUBSTR(call_off_lease, 'Call Off')
                AND lsp != 'Inmarsat Solutions (Canada) Inc.'
                AND revenue_recognition_basis != 'Flex'
                AND lease_update_status != 'Lease Cancelled'
                AND wholesale_billing_status_c NOT IN (
                    'Billed',
                    'Billed - Manually',
                    'Billed - SV',
                    'Billing Not Required'
                )
                AND start_date_of_current_lease < current_month
                AND end_date_of_current_lease < current_month THEN 1
                ELSE 0
            END AS call_off_whs_external_tab_ind
        FROM
            final_output
    );