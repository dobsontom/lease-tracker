CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker_base` AS (
    WITH lease_call_off_block_data AS (
        SELECT
            lr.id,
            cob.any_other_technical_details_required_cob_c,
            lr.approval_document_id_c,
            lr.business_unit_c,
            lr.name AS end_customer,
            lr.lsp_c,
            lr.lsr_c,
            lr.project_name_c,
            lr.contract_number_c,
            lr.total_value_of_the_order_c,
            lr.wholesale_pricing_comments_c,
            cob.cob_start_time_c,
            cob.cob_end_time_c,
            cob.forward_bandwidth_k_hz_cob_c,
            cob.power_d_bw_cob_c,
            cob.price_plan_cob_c,
            cob.retail_contract_value_cob_c,
            cob.return_bandwidth_k_hz_cob_c,
            cob.id AS cob_id,
            cob.name AS call_off_block_name,
            cob.status_cob_c,
            cob.wholesale_contract_value_cob_c,
            cob.daily_usage_charge_cob_c,
            cob.daily_usage_charge_retail_c,
            COALESCE(lr.project_id_c, '') AS project_id,
            DATE(cob.cob_start_date_c) AS start_date,
            DATE(cob.cob_end_date_c) AS end_date,
            FORMAT_TIMESTAMP(
                '%Y-%m-%d %H:%M:%S', TIMESTAMP(CONCAT(DATE(cob.cob_start_date_c), ' ', cob.cob_start_time_c))
            ) AS start_date_time,
            FORMAT_TIMESTAMP(
                '%Y-%m-%d %H:%M:%S', TIMESTAMP(CONCAT(DATE(cob.cob_end_date_c), ' ', cob.cob_end_time_c))
            ) AS end_date_time,
            CAST(
                CONCAT(
                    CAST(CAST(lr.lease_start_date_c AS DATE) AS STRING),
                    'T',
                    LEFT(CAST(lr.lease_start_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS start_date_of_current_lease,
            CAST(
                CONCAT(
                    CAST(CAST(lr.end_date_c AS DATE) AS STRING), 'T', LEFT(CAST(lr.lease_end_time_c AS STRING), 12)
                ) AS DATETIME
            ) AS end_date_of_current_lease,
            COALESCE(cob.retail_contract_value_cob_c, cob.wholesale_contract_value_cob_c) AS call_off_block_value
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS lr
        INNER JOIN
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.call_off_block_c` AS cob
            ON lr.id = cob.leasing_request_cob_c
    ),

    daily_charges AS (
        SELECT
            lscob.*,
            COALESCE(
                lscob.daily_usage_charge_retail_c,
                lscob.daily_usage_charge_cob_c,
                chrg.retail_periodic_payment_amount,
                chrg.wholesale_periodic_payment_amount
            ) AS daily_charge
        FROM
            lease_call_off_block_data AS lscob
        LEFT JOIN
            `inm-iar-data-warehouse-dev.call_off_tracker.call_off_blocks_needing_daily_charge_for_sf_upload` AS chrg
            ON lscob.id = chrg.cob_id
            AND lscob.contract_number_c = chrg.ssp_number
            AND lscob.call_off_block_name = chrg.call_off_block_name
        ORDER BY
            lscob.project_id ASC,
            lscob.start_date_time ASC
    ),

    project_summaries AS (
        SELECT
            project_id,
            MAX(end_date_time) AS max_end_date_time,
            SUM(call_off_block_value) AS sum_call_off_block_value
        FROM
            daily_charges
        GROUP BY
            project_id
    ),

    join_project_summaries AS (
        SELECT
            dc.*,
            ps.max_end_date_time,
            ps.sum_call_off_block_value
        FROM
            daily_charges AS dc
        INNER JOIN project_summaries AS ps ON dc.project_id = ps.project_id
        WHERE
            dc.status_cob_c != 'Cancelled'
    ),

    final_data AS (
        SELECT
            project_id,
            max_end_date_time,
            sum_call_off_block_value,
            project_name_c,
            contract_number_c,
            status_cob_c,
            price_plan_cob_c,
            business_unit_c,
            lsp_c,
            lsr_c,
            end_customer,
            approval_document_id_c,
            forward_bandwidth_k_hz_cob_c,
            return_bandwidth_k_hz_cob_c,
            power_d_bw_cob_c,
            total_value_of_the_order_c,
            wholesale_contract_value_cob_c,
            retail_contract_value_cob_c,
            start_date,
            end_date,
            start_date_time,
            end_date_time,
            any_other_technical_details_required_cob_c,
            call_off_block_name,
            call_off_block_value,
            daily_usage_charge_retail_c,
            daily_charge,
            wholesale_pricing_comments_c,
            start_date_of_current_lease,
            end_date_of_current_lease,
            LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS accrual_date,
            CURRENT_TIMESTAMP() AS last_refresh_time,
            CASE
                WHEN lsp_c = 'Inmarsat Solutions (Canada) Inc.'
                    AND end_date_of_current_lease >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
                    AND start_date_of_current_lease <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                    THEN 1
                ELSE 0
            END AS rtl_flag,
            CASE
                WHEN lsp_c != 'Inmarsat Solutions (Canada) Inc.'
                    AND end_date_of_current_lease >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
                    AND start_date_of_current_lease <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                    THEN 1
                ELSE 0
            END AS whs_flag
        FROM
            join_project_summaries
    )

    SELECT
        project_id AS `Project ID`,
        max_end_date_time AS `Max_End Date Time`,
        sum_call_off_block_value AS `Sum_Call Off Block Value`,
        project_name_c AS `Project Name`,
        contract_number_c AS `SSP number`,
        status_cob_c AS `Status`,
        price_plan_cob_c AS `Price Plan`,
        business_unit_c AS `Business Unit`,
        lsp_c AS `LSP`,
        lsr_c AS `LSR`,
        end_customer AS `End Customer`,
        approval_document_id_c AS `Approval Document ID`,
        forward_bandwidth_k_hz_cob_c AS `Forward Bandwidth kHz`,
        return_bandwidth_k_hz_cob_c AS `Return Bandwidth kHz`,
        power_d_bw_cob_c AS `Power dBW`,
        total_value_of_the_order_c AS `Total Value`,
        wholesale_contract_value_cob_c AS `Wholesale Block Value`,
        retail_contract_value_cob_c AS `Retail Block Value`,
        start_date AS `Start Date`,
        end_date AS `End Date`,
        start_date_time AS `Start Date Time`,
        end_date_time AS `End Date Time`,
        any_other_technical_details_required_cob_c AS `Any_other_Technical_Details_Required_COB__c`,
        call_off_block_name AS `Call Off Block Name`,
        call_off_block_value AS `Call Off Block Value`,
        daily_usage_charge_retail_c AS `Daily_Usage_Charge_Retail__c`,
        daily_charge AS `Daily Charge`,
        wholesale_pricing_comments_c AS `Wholesale Pricing Comments`,
        end_date_of_current_lease AS `End Date of Current Lease`,
        start_date_of_current_lease AS `Start Date of Current Lease`,
        accrual_date,
        rtl_flag,
        whs_flag,
        last_refresh_time AS `Last Refresh Time`
    FROM
        final_data
);
