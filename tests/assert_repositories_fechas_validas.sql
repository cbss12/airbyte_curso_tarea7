-- tests/assert_repositories_fechas_validas.sql
--
-- Test singular: verifica que ningún repositorio tenga una fecha de
-- actualización en el futuro o anterior al año 2008 (año de fundación de GitHub).
-- Fechas inválidas indican problemas en la extracción o transformación.
--
-- Retorna filas si falla (comportamiento estándar de singular tests en dbt).

with repos_con_fechas_invalidas as (

    select
        repository_id,
        repository_name,
        updated_at,
        case
            when updated_at > current_timestamp
                then 'Fecha en el futuro'
            when updated_at < '2008-01-01'::timestamp
                then 'Fecha anterior a la creación de GitHub (2008)'
            else null
        end as motivo_invalido

    from {{ ref('obt_repositories') }}

)

-- Devuelve solo los registros con fechas inválidas → test falla si hay filas
select
    repository_id,
    repository_name,
    updated_at,
    motivo_invalido

from repos_con_fechas_invalidas
where motivo_invalido is not null
