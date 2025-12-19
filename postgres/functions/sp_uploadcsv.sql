-- FUNCTION: public.sp_uploadcsv(text, text, bigint)

-- DROP FUNCTION IF EXISTS public.sp_uploadcsv(text, text, bigint);

CREATE OR REPLACE FUNCTION public.sp_uploadcsv(
	p_filepath text,
	p_filename text,
	p_filesize bigint)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    upload_id BIGINT;
BEGIN
    INSERT INTO uploadhistory (filename, filesize, filepath)
    VALUES (p_filename, p_filesize, p_filepath)
    RETURNING id INTO upload_id;

    RETURN upload_id;
END;
$BODY$;

ALTER FUNCTION public.sp_uploadcsv(text, text, bigint)
    OWNER TO postgres;

