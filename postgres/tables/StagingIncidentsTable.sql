-- Table: public.stagingincidents

-- DROP TABLE IF EXISTS public.stagingincidents;

CREATE TABLE IF NOT EXISTS public.stagingincidents
(
    "number" text COLLATE pg_catalog."default",
    opened text COLLATE pg_catalog."default",
    short_description text COLLATE pg_catalog."default",
    caller text COLLATE pg_catalog."default",
    priority text COLLATE pg_catalog."default",
    state text COLLATE pg_catalog."default",
    category text COLLATE pg_catalog."default",
    assignment_group text COLLATE pg_catalog."default",
    assigned_to text COLLATE pg_catalog."default",
    updated text COLLATE pg_catalog."default",
    updated_by text COLLATE pg_catalog."default",
    child_incidents text COLLATE pg_catalog."default",
    sla_due text COLLATE pg_catalog."default",
    severity text COLLATE pg_catalog."default",
    subcategory text COLLATE pg_catalog."default",
    resolution_notes text COLLATE pg_catalog."default",
    resolved text COLLATE pg_catalog."default",
    sla_calculation text COLLATE pg_catalog."default",
    parent_incident text COLLATE pg_catalog."default",
    parent text COLLATE pg_catalog."default",
    task_type text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.stagingincidents
    OWNER to postgres;