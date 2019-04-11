CREATE OR REPLACE FUNCTION alert_zero_quantity(IN INTEGER)
  RETURNS TABLE(ord_no INTEGER) AS
$BODY$ 
BEGIN
RETURN QUERY

SELECT oh.ord_no
FROM order_header oh
WHERE oh.ord_no = $1
AND oh.ord_status NOT IN ('C', 'F', 'I')
AND oh.ord_type NOT IN (9, 10)
AND EXISTS (
        SELECT ol.orl_id
        FROM order_line ol
        WHERE ol.ord_no = $1
        AND ol.orl_quantity = 0
        AND ol.prt_no <> ''
)

;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION alert_zero_quantity(INTEGER)
  OWNER TO "SIGM";