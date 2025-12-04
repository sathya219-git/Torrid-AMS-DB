-- Table: public.incidents

-- DROP TABLE IF EXISTS public.incidents;

CREATE TABLE IF NOT EXISTS public.incidents
(
    "number" text COLLATE pg_catalog."default" NOT NULL,
    opened timestamp without time zone,
    short_description text COLLATE pg_catalog."default",
    caller text COLLATE pg_catalog."default",
    priority text COLLATE pg_catalog."default",
    state text COLLATE pg_catalog."default",
    category text COLLATE pg_catalog."default",
    assignment_group text COLLATE pg_catalog."default",
    assigned_to text COLLATE pg_catalog."default",
    updated timestamp without time zone,
    updated_by text COLLATE pg_catalog."default",
    child_incidents integer,
    sla_due numeric(10,2),
    severity text COLLATE pg_catalog."default",
    subcategory text COLLATE pg_catalog."default",
    resolution_notes text COLLATE pg_catalog."default",
    resolved timestamp without time zone,
    sla_calculation numeric(10,2),
    parent_incident numeric(18,2),
    parent text COLLATE pg_catalog."default",
    task_type text COLLATE pg_catalog."default",
    CONSTRAINT incidents_pkey PRIMARY KEY ("number")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.incidents
    OWNER to postgres;