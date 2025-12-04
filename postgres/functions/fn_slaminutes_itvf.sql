-- FUNCTION: public.fn_slaminutes_itvf(timestamp without time zone, timestamp without time zone, boolean)

-- DROP FUNCTION IF EXISTS public.fn_slaminutes_itvf(timestamp without time zone, timestamp without time zone, boolean);

CREATE OR REPLACE FUNCTION public.fn_slaminutes_itvf(
	p_from timestamp without time zone,
	p_to timestamp without time zone,
	p_includeweekends boolean)
    RETURNS TABLE(businessminutes integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    from_date DATE;
    to_date   DATE;
    first_day_minutes INT := 0;
    last_day_minutes  INT := 0;
    middle_days       INT := 0;
    total_days        INT := 0;
    remainder_days    INT := 0;
    weekday_start     INT := 0;
BEGIN
    IF p_from IS NULL OR p_to IS NULL OR p_to <= p_from THEN
        businessminutes := 0;
        RETURN NEXT;
        RETURN;
    END IF;

    -- include weekends → simple difference
    IF p_includeweekends THEN
        businessminutes := (EXTRACT(EPOCH FROM p_to)::int / 60) - (EXTRACT(EPOCH FROM p_from)::int / 60);
        RETURN NEXT;
        RETURN;
    END IF;

    from_date := DATE(p_from);
    to_date   := DATE(p_to);

    -- same calendar date
    IF from_date = to_date THEN
        IF (CASE WHEN EXTRACT(DOW FROM from_date)=0 THEN 7 ELSE EXTRACT(DOW FROM from_date)::int END) BETWEEN 1 AND 5 THEN
            businessminutes := (EXTRACT(EPOCH FROM p_to)::int / 60) - (EXTRACT(EPOCH FROM p_from)::int / 60);
        ELSE
            businessminutes := 0;
        END IF;
        RETURN NEXT;
        RETURN;
    END IF;

    -- first day partial
    IF (CASE WHEN EXTRACT(DOW FROM from_date)=0 THEN 7 ELSE EXTRACT(DOW FROM from_date)::int END) BETWEEN 1 AND 5 THEN
        first_day_minutes := (EXTRACT(EPOCH FROM (date_trunc('day', p_from) + INTERVAL '1 day'))::int / 60)
                           - (EXTRACT(EPOCH FROM p_from)::int / 60);
    END IF;

    -- last day partial
    IF (CASE WHEN EXTRACT(DOW FROM to_date)=0 THEN 7 ELSE EXTRACT(DOW FROM to_date)::int END) BETWEEN 1 AND 5 THEN
        last_day_minutes := (EXTRACT(EPOCH FROM p_to)::int / 60)
                          - (EXTRACT(EPOCH FROM date_trunc('day', p_to))::int / 60);
    END IF;

    -- middle full days
    IF (from_date + INTERVAL '1 day') <= (to_date - INTERVAL '1 day') THEN
        total_days := ((to_date - INTERVAL '1 day')::date - (from_date + INTERVAL '1 day')::date) + 1;
        middle_days := (total_days / 7) * 5;
        remainder_days := total_days % 7;
        weekday_start := (CASE WHEN EXTRACT(DOW FROM (from_date + INTERVAL '1 day'))=0 THEN 7 ELSE EXTRACT(DOW FROM (from_date + INTERVAL '1 day'))::int END);

        FOR i IN 0..6 LOOP
            EXIT WHEN i >= remainder_days;
            IF (((weekday_start + i - 1) % 7) + 1) BETWEEN 1 AND 5 THEN
                middle_days := middle_days + 1;
            END IF;
        END LOOP;
    END IF;

    businessminutes := first_day_minutes + last_day_minutes + (middle_days * 1440);
    RETURN NEXT;
END;
$BODY$;

ALTER FUNCTION public.fn_slaminutes_itvf(timestamp without time zone, timestamp without time zone, boolean)
    OWNER TO postgres;

