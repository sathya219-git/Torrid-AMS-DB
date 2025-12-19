-- FUNCTION: public.sp_getuploadhistory(text, text, text, integer, integer)

-- DROP FUNCTION IF EXISTS public.sp_getuploadhistory(text, text, text, integer, integer);

CREATE OR REPLACE FUNCTION public.sp_getuploadhistory(
	p_searchtext text DEFAULT NULL::text,
	p_sortby text DEFAULT 'UploadedDate'::text,
	p_sortdir text DEFAULT 'DESC'::text,
	p_pagenumber integer DEFAULT 1,
	p_pagesize integer DEFAULT 4)
    RETURNS TABLE(id bigint, filename text, filesize bigint, filepath text, uploaded_at timestamp without time zone, totalcount integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    IF p_pagenumber IS NULL OR p_pagenumber < 1 THEN
        p_pagenumber := 1;
    END IF;
    IF p_pagesize IS NULL OR p_pagesize < 1 THEN
        p_pagesize := 4;
    END IF;

    p_sortby  := upper(coalesce(p_sortby, 'UPLOADEDDATE'));
    p_sortdir := CASE WHEN upper(p_sortdir) = 'ASC' THEN 'ASC' ELSE 'DESC' END;

    RETURN QUERY
    WITH f AS (
        SELECT
            uh.id,
            uh.filename,
            uh.filesize,
            uh.filepath,
            uh.uploaded_at,
            COUNT(*) OVER()::int AS totalcount   -- cast to int
        FROM public.uploadhistory uh
        WHERE p_searchtext IS NULL
           OR (
                uh.filename ILIKE '%' || p_searchtext || '%'
             OR (p_searchtext ~ '^[0-9]+$' AND uh.id = p_searchtext::bigint)
             OR (p_searchtext ~ '^[0-9]+$' AND uh.filesize = p_searchtext::bigint)
             OR (p_searchtext ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                 AND uh.uploaded_at::date = to_date(p_searchtext, 'YYYY-MM-DD'))
           )
    )
    SELECT f.id, f.filename, f.filesize, f.filepath, f.uploaded_at, f.totalcount
    FROM f
    ORDER BY
        CASE WHEN p_sortby = 'ID'           AND p_sortdir = 'ASC'  THEN f.id END ASC,
        CASE WHEN p_sortby = 'ID'           AND p_sortdir = 'DESC' THEN f.id END DESC,
        CASE WHEN p_sortby = 'FILENAME'     AND p_sortdir = 'ASC'  THEN f.filename END ASC,
        CASE WHEN p_sortby = 'FILENAME'     AND p_sortdir = 'DESC' THEN f.filename END DESC,
        CASE WHEN p_sortby = 'FILESIZE'     AND p_sortdir = 'ASC'  THEN f.filesize END ASC,
        CASE WHEN p_sortby = 'FILESIZE'     AND p_sortdir = 'DESC' THEN f.filesize END DESC,
        CASE WHEN p_sortby = 'UPLOADEDDATE' AND p_sortdir = 'ASC'  THEN f.uploaded_at END ASC,
        CASE WHEN p_sortby = 'UPLOADEDDATE' AND p_sortdir = 'DESC' THEN f.uploaded_at END DESC
    OFFSET (p_pagenumber - 1) * p_pagesize
    LIMIT p_pagesize;
END;
$BODY$;

ALTER FUNCTION public.sp_getuploadhistory(text, text, text, integer, integer)
    OWNER TO postgres;

