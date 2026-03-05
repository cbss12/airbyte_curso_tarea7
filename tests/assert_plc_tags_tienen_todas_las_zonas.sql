-- tests/assert_plc_tags_tienen_todas_las_zonas.sql
--
-- Test singular: verifica que existan variables en las 3 zonas del PLC.
-- Si alguna zona (Entrada, Salida, Marca) no tiene variables, el test falla
-- devolviendo las zonas faltantes.
--
-- Retorna filas si falla (comportamiento estándar de singular tests en dbt).

with zonas_esperadas as (

    select 'Entrada' as zone_label
    union all
    select 'Salida'
    union all
    select 'Marca'

),

zonas_presentes as (

    select distinct zone_label
    from {{ ref('obt_plc_tags') }}

)

-- Devuelve zonas esperadas que NO están en los datos → test falla si hay filas
select
    ze.zone_label as zona_faltante,
    'La zona no tiene ninguna variable PLC registrada' as motivo

from zonas_esperadas ze
left join zonas_presentes zp on ze.zone_label = zp.zone_label
where zp.zone_label is null
