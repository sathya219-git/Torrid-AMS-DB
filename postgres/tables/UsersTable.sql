-- Table: public.Users

-- DROP TABLE IF EXISTS public."Users";

CREATE TABLE IF NOT EXISTS public."Users"
(
    "UserID" integer NOT NULL DEFAULT nextval('"Users_UserID_seq"'::regclass),
    "Username" character varying(100) COLLATE pg_catalog."default" NOT NULL,
    "PasswordHash" bytea NOT NULL,
    "Email" character varying(255) COLLATE pg_catalog."default",
    "Role" character varying(50) COLLATE pg_catalog."default",
    "CreatedDate" timestamp without time zone DEFAULT now(),
    "IsActive" boolean DEFAULT true,
    "DefaultPassword" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT "Users_pkey" PRIMARY KEY ("UserID"),
    CONSTRAINT "Users_Username_key" UNIQUE ("Username")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Users"
    OWNER to postgres;