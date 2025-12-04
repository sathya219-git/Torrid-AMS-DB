-- Table: public.uploadhistory

-- DROP TABLE IF EXISTS public.uploadhistory;

CREATE TABLE IF NOT EXISTS public.uploadhistory
(
    id BIGSERIAL PRIMARY KEY,
    filename TEXT COLLATE pg_catalog."default" NOT NULL,
    filesize BIGINT NOT NULL,
    filepath TEXT COLLATE pg_catalog."default" NOT NULL,
    uploaded_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
)
TABLESPACE pg_default;