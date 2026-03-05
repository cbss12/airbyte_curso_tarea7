-- obt_repositories.sql
-- One Big Table para análisis del portafolio de repositorios GitHub.
-- Consolida toda la información de clasificación y actividad en una
-- sola tabla lista para dashboards.

with repos_classified as (

    select * from {{ ref('int_repositories_classified') }}

)

select
    -- === IDENTIFICACIÓN ===
    repository_id,
    repository_name,
    organization_name,
    clone_url,

    -- === CLASIFICACIÓN ===
    repo_type,

    -- === ESTADO ===
    is_archived,
    is_active_repo,
    activity_status,

    -- === ACTIVIDAD ===
    updated_at,
    days_since_update,

    -- === METADATA ===
    extracted_at,
    current_timestamp                               as transformed_at

from repos_classified
order by is_active_repo desc, updated_at desc
