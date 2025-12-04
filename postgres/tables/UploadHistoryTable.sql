-- Table: public.uploadhistory

-- DROP TABLE IF EXISTS public.uploadhistory;

CREATE TABLE IF NOT EXISTS public.uploadhistory
(
    id bigint NOT NULL DEFAULT nextval('uploadhistory_id_seq'::regclass),
    filename text COLLATE pg_catalog."default" NOT NULL,
    filesize bigint NOT NULL,
    filepath text COLLATE pg_catalog."default" NOT NULL,
    uploaded_at timestamp without time zone DEFAULT now(),
    CONSTRAINT uploadhistory_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.uploadhistory
    OWNER to postgres;