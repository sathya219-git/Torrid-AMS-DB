-- FUNCTION: public.sp_updatepasswordbydefault(text, text, text)

-- DROP FUNCTION IF EXISTS public.sp_updatepasswordbydefault(text, text, text);

CREATE OR REPLACE FUNCTION public.sp_updatepasswordbydefault(
	"@DefaultPassword" text,
	"@NewPassword" text,
	"@ConfirmNewPassword" text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_user_exists boolean;
BEGIN
    -- Step 2: Check if default password matches
    SELECT EXISTS (
        SELECT 1
        FROM "Users"
        WHERE "DefaultPassword" = "@DefaultPassword"
    ) INTO v_user_exists;

    IF NOT v_user_exists THEN
        -- RAISERROR equivalent
        RAISE EXCEPTION 'Default password is incorrect.'
        USING HINT = 'Ensure the old default password is correct.';
        RETURN;
    END IF;

    -- Step 3: Check if new password and confirm password match
    IF "@NewPassword" <> "@ConfirmNewPassword" THEN
        -- RAISERROR equivalent
        RAISE EXCEPTION 'New password and confirmed password do not match.'
        USING HINT = 'The two new password fields must be identical.';
        RETURN;
    END IF;

    -- Step 4: Update the new password (hashing it for security)
    -- NOTE: Hashing uses pgcrypto's digest function
    UPDATE "Users"
    SET
        "PasswordHash" = ENCODE(DIGEST("@NewPassword", 'sha256'), 'escape')::bytea,
        "CreatedDate" = NOW() -- Optional: update timestamp
    WHERE "DefaultPassword" = "@DefaultPassword";

    -- PRINT equivalent
    RAISE NOTICE 'Password updated successfully.';
END;
$BODY$;

ALTER FUNCTION public.sp_updatepasswordbydefault(text, text, text)
    OWNER TO postgres;

