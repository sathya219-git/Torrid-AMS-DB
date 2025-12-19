-- FUNCTION: public.sp_login(text, text)

-- DROP FUNCTION IF EXISTS public.sp_login(text, text);

CREATE OR REPLACE FUNCTION public.sp_login(
	"@Email" text,
	"@Password" text)
    RETURNS TABLE("Message" text, "UserID" integer, "Username" character varying, "Email" character varying, "Role" character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    -- Check for successful login
    IF EXISTS (
        SELECT 1
        FROM "Users" u -- <<<--- ADDED ALIAS 'u'
        WHERE u."Email" = "@Email" -- <<<--- QUALIFIED COLUMN REFERENCE
          AND u."PasswordHash" = ENCODE(DIGEST("@Password", 'sha256'), 'escape')::bytea
    ) THEN
        -- Successful login: Return user details
        RETURN QUERY
        SELECT
            'Login successful'::text AS "Message",
            u."UserID",
            u."Username",
            u."Email",
            u."Role"
        FROM "Users" u
        WHERE u."Email" = "@Email"; -- <<<--- QUALIFIED COLUMN REFERENCE
    ELSE
        -- Failed login: Return only the error message
        RETURN QUERY
        SELECT
            'Invalid username or password'::text AS "Message",
            NULL::integer AS "UserID",
            NULL::varchar(100) AS "Username",
            NULL::varchar(255) AS "Email",
            NULL::varchar(50) AS "Role";
    END IF;
END;
$BODY$;

ALTER FUNCTION public.sp_login(text, text)
    OWNER TO postgres;

