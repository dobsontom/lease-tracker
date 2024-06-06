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
            bill_period_end_c bill_period_start_c,
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
            ) AS invoice_number_c,
            IFNULL(
                billing_status_c,
                (
                    IFNULL(
                        external_billing_status_c,
                        internal_billing_status_c
                    )
                )
            ) AS invoice_number_c
        FROM
            inm-bi.sdp_salesforce_src.invoice_c
        WHERE
            is_deleted = FALSE
    ),
    user AS (
        SELECT
            *
        FROM
            inm-bi.sdp_salesforce_src.user
     )--,

    -- leasing_request_c AS(
        SELECT
            *,
            FORMAT_DATE('%Y-%m-%d %H:%M:%S', DATE(lease_start_date_c + " " + lease_start_time_c)) AS start_date_of_current_lease,
            FORMAT_DATE('%Y-%m-%d %H:%M:%S', DATE(lease_end_date_c + " " + lease_end_time_c)) AS end_date_of_current_lease,

        FROM
            inm-bi.sdp_salesforce_src.leasing_request_c
    WHERE contract_number_c IS NOT NULL
    LIMIT 1000
    -- )



        -- SELECT
        --     account_c,
        --     account_manager_c,!
        --     account_number_c,
        --     account_period_of_first_invoice_YYYYMM_C,
        --     approval_document_c,
        --     approval_document_id_c,
        --     approval_document_status_c,
        --     approval_status_change_date_c,
        --     assessment_comments_c,
        --     billing_acknowledgement_of_approval_docu_c,
        --     bupa_c,
        --     business_unit_1_c AS business_unit,
        --     can_be_accomodated_c,
        --     channel_no2_c,
        --     channel_weeks_charge_c,
        --     channel_weeks_trustcomm_l_tac_c,
        --     commercials_confirmed_c,
        --     company_id_c,
        --     consumed_amount_c,
        --     consumed_amount_per_c,
        --     contract_daily_rate_c,
        --     contract_duration_days,
        --     contract_number_c AS spp_number,
        --     contract_progess_c,
        --     contract_progress_comments_c,
        --     contract_sent_to_lsp_c,
        --     contract_signed_by_lsp_c,
        --     contract_sla_c,
        --     contracts_acknowledgement_of_approval_do_c,
        --     conus_discount_trustcomm_l_tac_c,
        --     count_c,
        --     created_by_id,
        --     created_by_date,
        --     credit_check_comments_c,
        --     credit_limit_approved_c,
        --     credit_rejected_c,
        --     credit_sla_c,
        --     customer_name_c,
        --     disaster_recovery_charge_c,
        --     disaster_recovery_with_spectrum_c,
        --     disaster_recovery_without_spectrum_c,
        --     document_approval_status_changed_date_c,
        --     due_date_c,
        --     end_date_c,
        --     end_user_organisation_c,
        --     escalation_reason_c,
        --     external_sale_profit_center_c,
        --     forward_bandwidth_khz_c,
        --     forward_data_rates_kbps_c,
        --     gx_email_alert_recursion_stop_c,
        --     host_radio3_c,
        --     id,!
        --     initial_request_date_c,
        --     internal_external_formula_c,
        --     internal_sale_profit_center_c,
        --     is_this_application_subject_to_c,
        --     is_deleted,
        --     isol_gm_c,
        --     laf_published_c,
        --     land_or_maritime_c,
            
            


            
        --     contract_number_c AS ssp_number,!
        --     FORMAT_DATE('%Y-%m-%d %H:%M:%S', DATE(lease_start_date_c + " " + lease_start_time_c)) AS start_date_of_current_lease,
        --     FORMAT_DATE('%Y-%m-%d %H:%M:%S', DATE(lease_end_date_c + " " + lease_end_time_c)) AS end_date_of_current_lease,
        --     lsp_c AS lsp,
        --     lst_c AS lsr,
            
        --     land_or_maritime_c AS land_aero_maritime,
        --     lease_service_type_c AS service_type,
        --     lsp_c AS lsp,
        --     lst_c AS lsr,
        --     name AS end_customer


        --     approval_document_id_c AS approval_docuement_id,
        --     business_unit_1_c AS business_unit,
        --     internal_external_formula_c AS internal_or_external,
        --     lease_update_status_c AS lease_update_status,
        --     retail_bill_interval_c AS retail_bill_interval,
        --     retail_contract_value_c AS retail_contract_value,
        --     retail_periodic_payment_amount_c AS retail_periodic_payment_amount,
        --     retial_pricing_comments_c AS retail_pricing_comments,
        --     revenue_recognition_c AS revenue_recognition_basis,
        --     wholesale_bill_interval_c AS wholesale_bill_interval,
        --     wholesale_contract_value_c AS wholesale_contract_value,
        --     wholesale_periodic_payment_amount_c AS wholesale_periodic_payment_amount,
        --     wholesale_pricing_comments_c AS wholesale_pricing_comments,

        --     account_number_c,
        --     invoice wholesale or retail,
        --     invoice number,
        --     invoice name,
        --     invoice wholesale credit rebill,
        --     invoice billing source,
        --     invoice bill period start,
        --     invoice bill period end,
        --     invoice billing status,
        --     invoice billed amount,
        --     invoice created date