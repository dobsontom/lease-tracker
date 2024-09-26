CREATE OR REPLACE VIEW `inm-iar-data-warehouse-dev.lease_tracker.call_off_tracker` AS (
    WITH leasing_request_call_off_block AS (
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
                '%Y-%m-%d %H:%M:%S',
                TIMESTAMP(CONCAT(DATE(cob.cob_start_date_c), ' ', cob.cob_start_time_c))
            ) AS start_date_time,
            FORMAT_TIMESTAMP(
                '%Y-%m-%d %H:%M:%S',
                TIMESTAMP(CONCAT(DATE(cob.cob_end_date_c), ' ', cob.cob_end_time_c))
            ) AS end_date_time,
            COALESCE(cob.retail_contract_value_cob_c, cob.wholesale_contract_value_cob_c)
                AS call_off_block_value
        FROM
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.leasing_request_c` AS lr
        INNER JOIN
            `inm-iar-data-warehouse-dev.sdp_salesforce_src.call_off_block_c` AS cob
            ON lr.id = cob.leasing_request_cob_c
    ),

    daily_charges AS (
        SELECT
            a.*,
            COALESCE(
                a.daily_usage_charge_retail_c,
                a.daily_usage_charge_cob_c,
                b.retail_periodic_payment_amount,
                b.wholesale_periodic_payment_amount
            ) AS daily_charge
        FROM
            leasing_request_call_off_block AS a
        LEFT JOIN
            `inm-iar-data-warehouse-dev.call_off_tracker.call_off_blocks_needing_daily_charge_for_sf_upload`
                AS b
            ON a.id = b.cob_id
            AND a.contract_number_c = b.ssp_number
            AND a.call_off_block_name = b.call_off_block_name
        ORDER BY
            a.project_id ASC,
            a.start_date_time ASC
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
            a.*,
            ps.max_end_date_time,
            ps.sum_call_off_block_value
        FROM
            daily_charges AS a
        INNER JOIN project_summaries AS ps ON a.project_id = ps.project_id
        WHERE
            a.status_cob_c != 'Cancelled'
    ),

    final_data AS (
        SELECT
            a.project_id,
            a.max_end_date_time,
            a.sum_call_off_block_value,
            a.project_name_c,
            a.contract_number_c,
            a.status_cob_c,
            a.price_plan_cob_c,
            a.business_unit_c,
            a.lsp_c,
            a.lsr_c,
            a.end_customer,
            a.approval_document_id_c,
            a.forward_bandwidth_k_hz_cob_c,
            a.return_bandwidth_k_hz_cob_c,
            a.power_d_bw_cob_c,
            a.total_value_of_the_order_c,
            a.wholesale_contract_value_cob_c,
            a.retail_contract_value_cob_c,
            a.start_date,
            a.end_date,
            a.start_date_time,
            a.end_date_time,
            a.any_other_technical_details_required_cob_c,
            a.call_off_block_name,
            a.call_off_block_value,
            a.daily_usage_charge_retail_c,
            a.daily_charge,
            a.wholesale_pricing_comments_c,
            b.start_date_of_current_lease,
            b.end_date_of_current_lease,
            CASE
                WHEN a.lsp_c = 'Inmarsat Solutions (Canada) Inc.'
                    AND b.end_date_of_current_lease
                    >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
                    AND b.start_date_of_current_lease
                    <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                    THEN 1
                ELSE 0
            END AS rtl_flag,
            CASE
                WHEN a.lsp_c != 'Inmarsat Solutions (Canada) Inc.'
                    AND b.end_date_of_current_lease
                    >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
                    AND b.start_date_of_current_lease
                    <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                    THEN 1
                ELSE 0
            END AS whs_flag
        FROM
            join_project_summaries AS a
        INNER JOIN
            `inm-iar-data-warehouse-dev.lease_tracker.lease_tracker` AS b
            ON a.contract_number_c = b.ssp_number
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
        rtl_flag,
        whs_flag
    FROM
        final_data
);
