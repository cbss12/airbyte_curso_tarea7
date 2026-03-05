-- stg_airbyte__repositories.sql
-- Limpieza y estandarización de repositorios de GitHub
-- Source: airbyte_curso.main.repositories (93 registros)

with source as (

    select * from {{ source('airbyte_raw', 'repositories') }}

),

renamed as (

    select
        -- Identificador del repositorio
        id                                          as repository_id,

        -- Extraer el nombre del repo desde la clone_url
        -- Ejemplo: https://github.com/airbytehq/tap-appstore.git → tap-appstore
        regexp_extract(
            clone_url,
            'github\.com/[^/]+/([^/]+?)(?:\.git)?$',
            1
        )                                           as repository_name,

        -- Extraer el owner/organización desde la clone_url
        regexp_extract(
            clone_url,
            'github\.com/([^/]+)/',
            1
        )                                           as organization_name,

        -- Estado del repositorio
        (archived = 'true' or archived = true)      as is_archived,

        -- URLs útiles
        clone_url,

        -- Timestamps
        cast(updated_at as timestamp)               as updated_at,

        -- Metadatos de Airbyte
        _airbyte_raw_id,
        _airbyte_extracted_at                       as extracted_at

    from source

)

select * from renamed
