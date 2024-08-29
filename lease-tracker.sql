CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker` AS (
    WITH
    invoice AS (
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
            bill_period_end_c AS bill_period_end,
            bill_period_start_c AS bill_period_start,
            billed_amount_c AS billed_amount,
            billing_source_c AS billing_source,
            leasing_request_c AS leasing_request,
            wholesale_credit_rebill_c AS wholesale_credit_rebill,
            wholesale_or_retail_c AS wholesale_or_retail,
            COALESCE(invoice_number_c, external_invoice_id_c, internal_invoice_id_c) AS invoice_number,
            COALESCE(billing_status_c, external_billing_status_c, internal_billing_status_c) AS billing_status
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.invoice_c`
        WHERE
            is_deleted = FALSE
    ),

    -- This CTE replicates a calcualtion that is done in Salesforce
    -- but is not loaded into GCP.
    business_unit_formula AS (
        SELECT
            contract_number_c,
            CASE
                WHEN business_unit_c IN ('Aviation', 'Enterprise', 'Maritime') THEN business_unit_c
                WHEN business_unit_c = 'USG' THEN 'US Government'
                WHEN business_unit_c = 'G2' THEN 'Global Government'
            END AS business_unit
        FROM
            (
                SELECT DISTINCT
                    contract_number_c,
                    COALESCE(business_unit_1_c, c.name) AS business_unit_c
                FROM
                    `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS a
                LEFT JOIN inm-iar-data-warehouse-dev.sdp_salesforce_src.account AS b ON a.account_c = b.id
                LEFT JOIN inm-iar-data-warehouse-dev.sdp_salesforce_src.record_type AS c ON b.record_type_id = c.id
                WHERE
                    contract_number_c IS NOT NULL
            )
    ),

    -- IFNULL()s ensure that final flags are applied correctly,
    -- as null NOT IN ('value') evaluates to FALSE, whereas we
    -- want it to evaluate to TRUE.
    leasing_request AS (
        SELECT
            account_manager_c AS account_manager,
            account_number_c AS account_number,
            account_period_of_first_invoice_yyyymm_c AS account_period_of_first_wholesale_invoice_yyyymm,
            approval_document_c AS approval_document,
            approval_document_id_c AS approval_document_id,
            approval_document_status_c AS approval_document_status,
            approval_status_change_date_c AS approval_status_change_date,
            assessment_comments_c AS assessment_comments,
            billing_acknowledgement_of_approval_docu_c AS billing_acknowledgement_of_approval_docu,
            bupa_c AS bupa,
            b.business_unit,
            can_be_accommodated_c AS can_be_accommodated,
            channel_no_2_c AS channel_no_2,
            channel_weeks_charge_c AS channel_weeks_charge,
            channel_weeks_trust_comm_l_tac_c AS channel_weeks_trust_comm_l_tac,
            commercials_confirmed_c AS commercials_confirmed,
            contract_duration_c AS contract_duration,
            a.contract_number_c AS ssp_number,
            contract_progress_c AS contract_progress,
            contract_progress_comments_c AS contract_progress_comments,
            contract_sent_to_lsp_c AS contract_sent_to_lsp,
            contract_signed_by_lsp_c AS contract_signed_by_lsp,
            contracts_acknowledgement_of_approval_do_c AS contracts_acknowledgement_of_approval_do,
            conus_discount_trust_comm_l_tac_c AS conus_discount_trust_comm_l_tac,
            created_date,
            credit_check_comments_c AS credit_check_comments,
            credit_limit_approved_c AS credit_limit_approved,
            credit_rejected_c AS credit_rejected,
            customer_name_c AS customer_name,
            disaster_recovery_charge_c AS disaster_recovery_charge,
            disaster_recovery_with_spectrum_c AS disaster_recovery_with_spectrum,
            disaster_recovery_without_spectrum_c AS disaster_recovery_without_spectrum,
            document_approval_status_changed_date_c AS document_approval_status_changed_date,
            due_date_c AS due_date,
            end_user_organisation_c AS end_user_organisation,
            escalation_reason_c AS escalation_reason,
            forward_bandwidth_k_hz_c AS forward_bandwidth_k_hz,
            forward_data_rates_kbps_c AS forward_data_rates_kbps,
            gx_email_alert_recursion_stop_c AS gx_email_alert_recursion_stop,
            host_radio_3_c AS host_radio_3,
            id,
            initial_request_date_c AS initial_request_date,
            is_this_application_subject_to_c AS is_this_application_subject_to,
            is_deleted,
            isol_gm_c AS isol_gm,
            land_or_maritime_c AS land_aero_maritime,
            last_modified_date,
            lease_end_time_c AS lease_end_time,
            lease_request_number_c AS lease_request_number,
            lease_service_type_c AS service_type,
            lease_start_time_c AS lease_start_time,
            lease_type_c AS lease_type,
            leasing_cc_emails_c AS leasing_cc_emails,
            lsp_c AS lsp,
            lspa_contract_no_c AS lspa_contract_no,
            lsr_c AS lsr,
            name AS end_customer,
            number_of_beams_c AS number_of_beams,
            other_technical_details_required_c AS other_technical_details_required,
            pmp_volume_discount_c AS pmp_volume_discount,
            power_d_bw_c AS power_d_bw,
            pre_sales_required_c AS pre_sales_required,
            price_plan_c AS price_plan,
            pricing_acknowledgement_of_approval_docu_c AS pricing_acknowledgement_of_approval_docu,
            primary_les_sas_c AS primary_les_sas,
            project_id_c AS project_id,
            project_name_c AS project_name,
            record_type_id,
            retail_bill_interval_c AS retail_bill_interval,
            retail_bill_interval_comments_c AS retail_bill_interval_comments,
            retail_billing_entered_c AS retail_billing_date_entered,
            retail_contract_value_c AS retail_contract_value,
            retail_periodic_payment_amount_c AS retail_periodic_payment_amount,
            retail_pricing_comments_c AS retail_pricing_comments,
            return_bandwidth_k_hz_c AS return_bandwidth_khz,
            return_data_rates_kbps_c AS return_data_rates_kbps,
            satellite_c AS satellite,
            secondary_les_sas_c AS secondary_les_sas,
            send_lrf_to_ops_c AS send_lrf_to_ops,
            send_to_contracts_c AS send_to_contracts,
            special_comments_c AS special_comments,
            special_terms_and_conditions_applicable_c AS special_terms_and_conditions_applicable,
            spot_beam_equivalent_c AS spot_beam_equivalent,
            spot_beam_equivalent_4_c AS spot_beam_equivalent_4,
            status_changed_date_c AS status_changed_date,
            system_modstamp,
            term_discount_c AS term_discount,
            total_value_of_the_order_c AS total_value_of_the_order,
            trust_comm_weeks_c AS trust_comm_weeks,
            type_of_beam_c AS type_of_beam,
            wholesale_bill_interval_c AS wholesale_bill_interval,
            wholesale_bill_interval_comments_c AS wholesale_bill_interval_comments,
            wholesale_billing_comments_c AS wholesale_billing_comments,
            wholesale_billing_engine_c AS wholesale_billing_engine,
            wholesale_billing_entered_c AS wholesale_billing_entered,
            wholesale_contract_value_c AS wholesale_contract_value,
            wholesale_monthly_minimum_charge_c AS wholesale_monthly_minimum_charge,
            wholesale_or_retail_c AS wholease_or_retail_formula,
            wholesale_periodic_payment_amount_c AS wholesale_periodic_payment_amount,
            wholesale_pricing_comments_c AS wholesale_pricing_comments,
            billing_completed_c AS billing_completed,
            disaster_recovery_frequency_reservation_c AS disaster_recovery_frequency_reservation,
            forward_bandwidth_mhz_c AS forward_bandwidth_mhz,
            forward_data_rates_mbps_c AS forward_data_rates_mbps,
            reserved_capacity_c AS reserved_capacity,
            return_bandwidth_mhz_c AS return_bandwidth_mhz,
            return_data_rates_mbps_c AS return_data_rates_mbps,
            optional_renewal_years_pricing_the_same_c AS optional_renewal_years_pricing_the_same,
            billing_interval_c AS standard_billing_interval,
            year_10_c AS year_10,
            year_1_c AS year_1,
            year_2_c AS year_2,
            year_3_c AS year_3,
            year_4_c AS year_4,
            year_5_c AS year_5,
            year_6_c AS year_6,
            year_7_c AS year_7,
            year_8_c AS year_8,
            year_9_c AS year_9,
            approval_document_status_date_capture_c AS approval_document_status_date_capture,
            approvl_doc_status_date_capture_empty_c AS approvl_doc_status_date_capture_empty,
            lease_update_status_solution_engineering_c AS lease_update_status_solution_engineering,
            lease_update_status_pricing_c AS lease_update_status_pricing,
            type_of_beam_equivalent_c AS type_of_beam_equivalent,
            uid_unbilled_amount_retail_c AS uid_unbilled_amount_retail_formula,
            uid_unbilled_amount_wholesale_c AS uid_unbilled_amount_wholesale_new,
            wholesale_billing_entered_c AS wholesale_billing_date_entered,
            uid_billed_amount_retail_c AS retail_uid_billed_amount,
            uid_billed_amount_wholesale_c AS wholesale_uid_billed_amount,
            po_amount_c AS po_amount,
            CASE
                WHEN lsp_c IN ('Inmarsat Government Inc.', 'Inmarsat Solutions (Canada) Inc.') THEN 'Internal'
                ELSE 'External'
            END AS internal_external,
            COALESCE(lease_update_status_c, '') AS lease_update_status,
            COALESCE(retail_billing_status_c, '') AS retail_billing_status,
            COALESCE(revenue_recognition_c, '') AS revenue_recognition_basis,
            COALESCE(wholesale_billing_status_c, '') AS wholesale_billing_status,
            CAST(
                CONCAT(
                    CAST(CAST(lease_start_date_c AS DATE) AS STRING), 'T', LEFT(CAST(lease_start_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS start_date_of_current_lease,
            CAST(
                CONCAT(
                    CAST(CAST(end_date_c AS DATE) AS STRING), 'T', LEFT(CAST(lease_end_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS end_date_of_current_lease,
            CASE
                WHEN CONTAINS_SUBSTR(price_plan_c, 'Take or Pay')
                    OR CONTAINS_SUBSTR(lease_type_c, 'Flex') THEN 'Call Off Lease - Please refer to Call Off Tracker'
                ELSE ''
            END AS call_off_lease
        -- The following fields are imported from Salesforce in the original workflow,
        -- but are not available from the leasing request_c GCP table. 
        -- company_id_c,
        -- consumed_amount_c,
        -- contract_daily_rate_c,
        -- contract_sla_c,
        -- count_c,
        -- credit_sla_c,
        -- internal_sale_profit_center,
        -- external_sale_profit_center_c,
        -- material_code_c,
        -- pricing_sla_c,
        -- retail_account_codes_c,
        -- retail_consumed_amount_c,
        -- retail_consumed_amount_per_c,
        -- status_report_c,
        -- retail_margin_c,
        -- technical_bd_sla_c,
        -- wholesale_account_code_c,
        -- contract_days_time_difference_c,
        -- contract_no_of_days_c,
        -- contract_date_difference_c,
        -- isol_gm_formula_c,
        -- retail_contract_daily_value_c,
        -- wholesale_contract_daily_value_c,
        -- approval_doc_status_empty_to_value_c,
        -- completed_in_billing_sla_c,
        -- lease_status_solution_to_pricing_timedif_c,
        -- retail_consumed_amount_left_c,
        -- wholesale_consumed_amount_left_c,
        -- pg_bd_sla_c,
        -- retail_billing_completed_in_sla_c
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS a
        LEFT JOIN business_unit_formula AS b ON a.contract_number_c = b.contract_number_c
        WHERE
            a.contract_number_c IS NOT NULL
    ),

    user AS (
        SELECT
            id AS user_id,
            name AS account_manager_name,
            division AS account_manager_division
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.user`
    ),

    leasing_request_user AS (
        SELECT
            *
        FROM
            leasing_request AS lr
        LEFT JOIN user AS u ON lr.account_manager = u.user_id
    ),

    -- Billing data is the first and least processed output of the original
    -- workflow. If needed, this may need to be used to create a separate table
    -- as it has a distinct format to the final output.
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
            leasing_request_user AS lru
        INNER JOIN invoice AS i ON lru.id = i.leasing_request
    ),

    -- Pivot and concatenation performed on retail and wholesale invoice
    -- numbers to get a single value for each SSP number, as per the
    -- original workflow.
    retail_wholesale_invoice_nos AS (
        SELECT
            ssp_number,
            retail AS retail_invoice_id,
            wholesale AS wholesale_invoice_id
        FROM
            (
                SELECT
                    ssp_number,
                    invoice_wholesale_or_retail,
                    STRING_AGG(invoice_number, '; ') AS invoice_number
                FROM
                    billing_data
                GROUP BY
                    ssp_number,
                    invoice_wholesale_or_retail
            ) PIVOT (
            MAX(invoice_number)
            FOR invoice_wholesale_or_retail IN ('Retail' AS retail, 'Wholesale' AS wholesale)
        )
    ),

    -- final_output creates a format that contains all fields
    -- used in all tabs of the original Excel spreadsheet.
    final_output AS (
        SELECT
            lru.*,
            rwinv.retail_invoice_id,
            rwinv.wholesale_invoice_id,
            CASE
                WHEN lease_update_status = 'Commercials completed' THEN 1
                WHEN lease_update_status = 'Lease Cancelled' THEN 9999999
                WHEN lease_update_status = 'Billing' THEN 2
                WHEN lease_update_status = 'Solution Engineering' THEN 3
                WHEN lease_update_status = 'Pricing' THEN 4
                WHEN lease_update_status = 'Contract' THEN 5
                ELSE 1000000000
            END AS lease_update_status_code,
            (
                DATE_DIFF(DATE_ADD(end_date_of_current_lease, INTERVAL 1 DAY), start_date_of_current_lease, DAY)
                / 365.25
            ) * 12 AS total_no_of_months,
            CASE
                WHEN CONTAINS_SUBSTR(lru.ssp_number, 'GX') THEN lru.ssp_number
            END AS new_ssp_number,
            CURRENT_DATE() AS current_month
        FROM
            leasing_request_user AS lru
        LEFT JOIN retail_wholesale_invoice_nos AS rwinv ON lru.ssp_number = rwinv.ssp_number
        ORDER BY
            ssp_number ASC,
            lease_update_status_code ASC
    )

    -- This statement adds flags to final_output, allowing the data
    -- to be easily filtered to the subsets of data populating each
    -- tab of the original Excel workbook.
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
                AND retail_billing_status NOT IN ('Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required')
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
                AND retail_billing_status NOT IN ('Billed', 'Billed - Manually', 'Billed - SV', 'Billing Not Required')
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
        final_output
);
