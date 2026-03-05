-- int_repositories_classified.sql
-- Clasifica repositorios de GitHub por tipo de proyecto según convenciones
-- de nomenclatura del ecosistema Airbyte. Prepara datos para el mart.

with repositories as (

    select * from {{ ref('stg_airbyte__repositories') }}

),

classified as (

    select
        repository_id,
        repository_name,
        organization_name,
        is_archived,
        clone_url,
        updated_at,
        extracted_at,

        -- Clasificar el tipo de repositorio según el prefijo del nombre
        case
            when repository_name like 'connector-%'   then 'Connector'
            when repository_name like 'tap-%'         then 'Singer Tap'
            when repository_name like 'target-%'      then 'Singer Target'
            when repository_name like 'airbyte-%'     then 'Airbyte Core'
            when repository_name like '%-demo%'       then 'Demo / Example'
            when repository_name like '%-workshop%'   then 'Workshop'
            when repository_name like 'dbt-%'         then 'dbt Integration'
            else 'Other'
        end                                             as repo_type,

        -- Estado de actividad basado en updated_at
        case
            when updated_at >= current_date - interval '90 days'  then 'Activo'
            when updated_at >= current_date - interval '365 days' then 'Mantenimiento'
            else 'Inactivo'
        end                                             as activity_status,

        -- Días desde la última actualización
        datediff(
            'day',
            updated_at,
            current_timestamp
        )                                               as days_since_update,

        -- Estado combinado: activo y no archivado
        (not is_archived
            and updated_at >= current_date - interval '365 days')
                                                        as is_active_repo

    from repositories

)

select * from classified
