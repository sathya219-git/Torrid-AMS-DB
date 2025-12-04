-- FUNCTION: public.sp_importincidentsfromupload(bigint)

-- DROP FUNCTION IF EXISTS public.sp_importincidentsfromupload(bigint);

CREATE OR REPLACE FUNCTION public.sp_importincidentsfromupload(
	p_upload_id bigint)
    RETURNS TABLE(stagingrowcount integer, insertedcount integer, updatedcount integer, matchednotupdatedcount integer, skippedmissingnumber integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    v_filepath TEXT;
BEGIN
    --------------------------------------------------------------------
    -- 1) Get file path from uploadhistory
    --------------------------------------------------------------------
    SELECT filepath
    INTO v_filepath
    FROM uploadhistory
    WHERE id = p_upload_id;

    IF v_filepath IS NULL THEN
        RAISE EXCEPTION 'UploadHistory row not found for ID %', p_upload_id;
    END IF;

    --------------------------------------------------------------------
    -- 2) Clear staging table
    --------------------------------------------------------------------
    TRUNCATE TABLE stagingincidents;

    --------------------------------------------------------------------
    -- 3) Load CSV into staging using COPY
    --------------------------------------------------------------------
    BEGIN
        EXECUTE format(
            'COPY stagingincidents
             FROM %L
             WITH (FORMAT csv, HEADER true)',
            v_filepath
        );
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'COPY into stagingincidents failed: %', SQLERRM;
    END;

    --------------------------------------------------------------------
    -- 4) Prepare parsed source data from staging
    --------------------------------------------------------------------
    WITH src AS (
        SELECT
            trim(number) AS number,

            -- Updated_dt
            CASE
                WHEN NULLIF(updated, '') IS NULL THEN NULL
                WHEN updated ~* 'AM|PM' THEN
                    to_timestamp(updated, 'FMMM/FMDD/YYYY FMHH12:MI:SS AM')
                WHEN updated LIKE '__-__-____ __:__:__' THEN
                    to_timestamp(updated, 'MM-DD-YYYY HH24:MI:SS')
                WHEN updated LIKE '____-__-__ __:__:__' THEN
                    to_timestamp(updated, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END AS updated_dt,

            -- Opened_dt
            CASE
                WHEN NULLIF(opened, '') IS NULL THEN NULL
                WHEN opened ~* 'AM|PM' THEN
                    to_timestamp(opened, 'FMMM/FMDD/YYYY FMHH12:MI:SS AM')
                WHEN opened LIKE '__-__-____ __:__:__' THEN
                    to_timestamp(opened, 'MM-DD-YYYY HH24:MI:SS')
                WHEN opened LIKE '____-__-__ __:__:__' THEN
                    to_timestamp(opened, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END AS opened_dt,

            short_description,
            NULLIF(caller, '') AS caller_trunc,
            NULLIF(priority, '') AS priority_trunc,
            NULLIF(state, '') AS state_trunc,
            NULLIF(category, '') AS category_trunc,
            NULLIF(assignment_group, '') AS assignment_group_trunc,
            NULLIF(assigned_to, '') AS assigned_to_trunc,
            NULLIF(updated_by, '') AS updated_by_trunc,

            CASE
                WHEN NULLIF(child_incidents, '') IS NULL THEN NULL
                ELSE replace(child_incidents, ',', '')::int
            END AS child_incidents_int,

            CASE
                WHEN NULLIF(sla_due, '') IS NULL THEN NULL
                ELSE replace(replace(sla_due, ',', ''), '$', '')::numeric(10,2)
            END AS sla_due_dec,

            NULLIF(severity, '') AS severity_trunc,
            NULLIF(subcategory, '') AS subcategory_trunc,
            resolution_notes,

            -- Resolved_dt
            CASE
                WHEN NULLIF(resolved, '') IS NULL THEN NULL
                WHEN resolved ~* 'AM|PM' THEN
                    to_timestamp(resolved, 'FMMM/FMDD/YYYY FMHH12:MI:SS AM')
                WHEN resolved LIKE '__-__-____ __:__:__' THEN
                    to_timestamp(resolved, 'MM-DD-YYYY HH24:MI:SS')
                WHEN resolved LIKE '____-__-__ __:__:__' THEN
                    to_timestamp(resolved, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END AS resolved_dt,

            CASE
                WHEN NULLIF(sla_calculation, '') IS NULL THEN NULL
                ELSE replace(replace(sla_calculation, ',', ''), '$', '')::numeric(10,2)
            END AS sla_calc_dec,

            CASE
                WHEN NULLIF(parent_incident, '') IS NULL THEN NULL
                ELSE replace(replace(parent_incident, ',', ''), '$', '')::numeric(18,2)
            END AS parent_incident_dec,

            NULLIF(parent, '') AS parent_trunc,
            NULLIF(task_type, '') AS task_type_trunc

        FROM stagingincidents
        WHERE trim(number) IS NOT NULL
    ),

    --------------------------------------------------------------------
    -- 5) UPSERT into incidents
    --------------------------------------------------------------------
    upserted AS (
        INSERT INTO incidents (
            number,
            opened,
            short_description,
            caller,
            priority,
            state,
            category,
            assignment_group,
            assigned_to,
            updated,
            updated_by,
            child_incidents,
            sla_due,
            severity,
            subcategory,
            resolution_notes,
            resolved,
            sla_calculation,
            parent_incident,
            parent,
            task_type
        )
        SELECT
            s.number,
            s.opened_dt,
            s.short_description,
            s.caller_trunc,
            s.priority_trunc,
            s.state_trunc,
            s.category_trunc,
            s.assignment_group_trunc,
            s.assigned_to_trunc,
            s.updated_dt,
            s.updated_by_trunc,
            s.child_incidents_int,
            s.sla_due_dec,
            s.severity_trunc,
            s.subcategory_trunc,
            s.resolution_notes,
            s.resolved_dt,
            s.sla_calc_dec,
            s.parent_incident_dec,
            s.parent_trunc,
            s.task_type_trunc
        FROM src s
        ON CONFLICT (number)
        DO UPDATE SET
            opened            = COALESCE(EXCLUDED.opened, incidents.opened),
            short_description = COALESCE(EXCLUDED.short_description, incidents.short_description),
            caller            = COALESCE(EXCLUDED.caller, incidents.caller),
            priority          = COALESCE(EXCLUDED.priority, incidents.priority),
            state             = COALESCE(EXCLUDED.state, incidents.state),
            category          = COALESCE(EXCLUDED.category, incidents.category),
            assignment_group  = COALESCE(EXCLUDED.assignment_group, incidents.assignment_group),
            assigned_to       = COALESCE(EXCLUDED.assigned_to, incidents.assigned_to),
            updated           = EXCLUDED.updated,
            updated_by        = COALESCE(EXCLUDED.updated_by, incidents.updated_by),
            child_incidents   = COALESCE(EXCLUDED.child_incidents, incidents.child_incidents),
            sla_due           = COALESCE(EXCLUDED.sla_due, incidents.sla_due),
            severity          = COALESCE(EXCLUDED.severity, incidents.severity),
            subcategory       = COALESCE(EXCLUDED.subcategory, incidents.subcategory),
            resolution_notes  = COALESCE(EXCLUDED.resolution_notes, incidents.resolution_notes),
            resolved          = COALESCE(EXCLUDED.resolved, incidents.resolved),
            sla_calculation   = COALESCE(EXCLUDED.sla_calculation, incidents.sla_calculation),
            parent_incident   = COALESCE(EXCLUDED.parent_incident, incidents.parent_incident),
            parent            = COALESCE(EXCLUDED.parent, incidents.parent),
            task_type         = COALESCE(EXCLUDED.task_type, incidents.task_type)
        WHERE
            EXCLUDED.updated IS NOT NULL
            AND (
                incidents.updated IS NULL
                OR EXCLUDED.updated > incidents.updated
            )
        RETURNING
            xmax = 0 AS inserted_flag,
            1 AS affected_row
    ),

    agg AS (
        SELECT
            COALESCE(SUM(CASE WHEN inserted_flag THEN 1 ELSE 0 END), 0) AS ins_count,
            COALESCE(SUM(CASE WHEN NOT inserted_flag THEN 1 ELSE 0 END), 0) AS upd_count
        FROM upserted
    )

    --------------------------------------------------------------------
    -- 6) Final summary output
    --------------------------------------------------------------------
    SELECT
        (SELECT COUNT(*) FROM stagingincidents) AS stagingrowcount,
        (SELECT ins_count FROM agg)            AS insertedcount,
        (SELECT upd_count FROM agg)            AS updatedcount,
        0                                      AS matchednotupdatedcount,
        (SELECT COUNT(*) FROM stagingincidents WHERE trim(number) IS NULL) AS skippedmissingnumber
    INTO
        stagingrowcount,
        insertedcount,
        updatedcount,
        matchednotupdatedcount,
        skippedmissingnumber;

    RETURN NEXT;
END;
$BODY$;

ALTER FUNCTION public.sp_importincidentsfromupload(bigint)
    OWNER TO postgres;

