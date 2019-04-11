DROP VIEW IF EXISTS public.daily_alerts;
CREATE OR REPLACE VIEW daily_alerts AS (
    SELECT DISTINCT
        oh.ord_no,
        CASE
            WHEN
                oh.ord_status NOT IN ('C', 'F', 'I')
                AND oh.ord_type NOT IN (9, 10)
                AND EXISTS (
                    SELECT orl_id
                    FROM order_line ol
                    WHERE ol.ord_no = oh.ord_no
                    AND ol.orl_quantity = 0
                    AND ol.prt_no <> ''
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS zero_quantity,

        CASE
            WHEN
                EXISTS (
                    SELECT oh.ord_no
                    FROM order_header oh
                    WHERE oh.ord_no = ord_no
                    AND oh.ord_status NOT IN ('C', 'F', 'I')
                    AND oh.ord_type = 9
                    AND EXISTS (
                        SELECT ol.ord_no
                        FROM order_line ol
                        WHERE ol.ord_no = oh.ord_no
                        AND ol.prt_no <> ''
                        GROUP BY ol.ord_no
                        HAVING COUNT(ol.orl_id) = (
                            SELECT COUNT(ol2.orl_id)
                            FROM order_line ol2
                            WHERE ol2.orl_quantity = 0
                            AND ol2.ord_no = oh.ord_no
                            AND ol2.prt_no <> ''
                        )
                    )
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS completed_blanket,

        CASE
            WHEN
                oh.ord_status <> 'E'
                AND oh.ord_type <> 9
                AND oh.ord_type <> 10
                AND EXISTS (
                    SELECT ol.orl_id
                    FROM order_line ol
                    WHERE ol.ord_no = oh.ord_no
                    AND ol.orl_quantity <> ol.orl_reserved_qty
                    AND ol.orl_quantity <> ol.orl_ship_qty
                    AND ol.orl_quantity <> ol.orl_qty_ready + ol.orl_ship_qty + ol.orl_reserved_qty
                    AND ol.orl_quantity <> ol.orl_ship_qty + ol.orl_reserved_qty
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS not_reserved,

        CASE
            WHEN
                (
                    oh.ord_status = 'A'
                    OR (oh.ord_status = 'B' AND oh.ord_bo_accptd <> 'true')
                )
                AND EXISTS (
                    SELECT ol.ord_no
                    FROM order_line ol
                    WHERE EXISTS (
                        SELECT ol2.orl_req_dt
                        FROM order_line ol2
                        WHERE ol.orl_id <> ol2.orl_id
                        AND ol.ord_no = ol2.ord_no
                        AND ol2.prt_no <> ''
                    )
                    AND ol.orl_req_dt NOT IN (
                        SELECT ol2.orl_req_dt
                        FROM order_line ol2
                        WHERE ol.orl_id <> ol2.orl_id
                        AND ol.ord_no = ol2.ord_no
                        AND ol2.prt_no <> ''
                    )
                    AND oh.ord_no = ol.ord_no
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS line_dates,

        CASE
            WHEN
                oh.ord_pmt_term = 4
                and oh.ord_status IN ('A', 'B', 'E')
                and oh.ord_type = 1
                and oh.ord_pkg_cost = 0
            THEN
                TRUE
            ELSE
                FALSE
        END AS bank_fees,

        CASE
            WHEN
                oh.ord_status NOT IN ('C', 'F', 'I')
                AND oh.ord_type = 1
                AND EXISTS (
                    SELECT ol.ord_no
                    FROM order_line ol
                    WHERE oh.ord_no = ol.ord_no
                    AND ol.prt_no <> ''
                    AND ol.prt_no IN (
                        SELECT prt_no
                        FROM order_line ol2
                        JOIN order_header oh2 ON oh2.ord_no = ol2.ord_no
                        WHERE ol2.ord_no <> ol.ord_no
                        AND oh2.ord_type = 9
                        AND oh.cli_id = oh2.cli_id
                        AND prt_no <> ''
                    )
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS existing_blanket,

        CASE
            WHEN
                oh.ord_status NOT IN ('C', 'F', 'I')
                AND EXISTS (
                    WITH children AS (
                        SELECT pkt_master_prt_id, prt_id
                        FROM part_kit
                        WHERE pkt_master_prt_id IN (
                            SELECT DISTINCT ol.prt_id
                            FROM order_line ol
                            LEFT JOIN part_kit pk ON pk.prt_id = ol.prt_id
                            LEFT JOIN part_kit pk2 ON pk2.pkt_master_prt_id = ol.prt_id
                            WHERE ord_no = oh.ord_no
                            AND orl_kitmaster_id = 0
                        )
                    )
                    SELECT DISTINCT ord_no
                    FROM order_line ol
                    LEFT JOIN part_kit pk ON pk.prt_id = ol.prt_id
                    LEFT JOIN part_kit pk2 ON pk2.pkt_master_prt_id = ol.prt_id
                    WHERE ord_no = oh.ord_no
                    AND orl_kitmaster_id = 0
                    AND EXISTS (
                        SELECT prt_id
                        FROM children
                        WHERE pkt_master_prt_id = ol.prt_id
                        AND prt_id NOT IN (
                            SELECT prt_id
                            FROM order_line
                            WHERE orl_kitmaster_id = ol.orl_id
                        )
                    )
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS missing_component,

        CASE
            WHEN
                oh.ord_status NOT IN ('C', 'F', 'I')
                AND EXISTS (
                    SELECT DISTINCT ord_no
                    FROM order_line ol
                    LEFT JOIN part_kit pk ON pk.prt_id = ol.prt_id
                    LEFT JOIN part_kit pk2 ON pk2.pkt_master_prt_id = ol.prt_id
                    WHERE ol.ord_no = oh.ord_no
                    AND pk.pkt_qty * (
                        SELECT orl_quantity
                        FROM order_line ol2
                        WHERE ol2.ord_no = ol.ord_no
                        AND ol2.orl_id = ol.orl_kitmaster_id
                    ) <> ol.orl_quantity
                    AND pk.pkt_master_prt_id IN (
                        SELECT prt_id
                        FROM order_line ol2
                        WHERE ol2.ord_no = ol.ord_no
                    )
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS component_multiplier,

        CASE
            WHEN
                EXISTS (
                    SELECT i.inv_no
                    FROM invoicing i
                    WHERE i.inv_status = 'E'
                    AND i.inv_ship_amnt = 0
                    AND i.inv_subtotal >= 0
                    AND i.ord_no = oh.ord_no
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS shipping_cost,

        CASE
            WHEN
                EXISTS (
                    SELECT i.inv_no
                    FROM invoicing i
                    AND i.inv_inv_email_sent = 'F'
                    AND i.inv_no <> 0
                    AND i.inv_subtotal >= 0
                    AND i.ord_no = oh.ord_no
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS unsent_email

--        CASE
--            WHEN
--                1 = 1
--            THEN
--                TRUE
--            ELSE
--                FALSE
--        END AS column_name

    FROM order_header oh

    ORDER BY ord_no
)