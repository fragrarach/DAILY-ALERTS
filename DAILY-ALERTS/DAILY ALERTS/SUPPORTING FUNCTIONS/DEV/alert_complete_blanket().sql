CREATE OR REPLACE FUNCTION alert_complete_blanket(IN INTEGER)
  RETURNS TABLE(ord_no INTEGER) AS
$BODY$ 
BEGIN
RETURN QUERY

SELECT oh.ord_no
FROM order_header oh
WHERE oh.ord_status NOT IN ('C', 'F', 'I')
AND oh.ord_type = 9
AND EXISTS (
    SELECT ol.ord_no
    FROM order_line ol
    WHERE ol.ord_no = oh.ord_no
    AND ol.prt_no <> ''
    GROUP BY ol.ord_no
    HAVING COUNT(ol.orl_id) = (
        SELECT COUNT(orl_id)
        FROM order_line
        WHERE orl_quantity = 0
        AND order_line.ord_no = oh.ord_no
        AND prt_no <> ''
    )
)

;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION alert_complete_blanket(INTEGER)
  OWNER TO "SIGM";