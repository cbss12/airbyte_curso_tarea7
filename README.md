# airbyte_curso — Proyecto dbt

Proyecto dbt para transformación de datos del curso **Introducción a la Ingeniería de Datos** — MIA 03, Facultad Politécnica, UNA.

Transforma datos extraídos con **Airbyte** desde MotherDuck usando el modelo **One Big Table (OBT)**.

---

## 📁 Estructura del Proyecto

```
airbyte_curso_dbt/
├── packages.yml                          ← dbt-expectations package
├── models/
│   ├── staging/
│   │   ├── _sources.yml                  ← Definición de fuentes Airbyte
│   │   ├── _staging_models.yml           ← Docs y tests de staging
│   │   ├── stg_airbyte__plc_tags.sql
│   │   └── stg_airbyte__repositories.sql
│   ├── intermediate/
│   │   ├── _intermediate_models.yml
│   │   ├── int_plc_tags_enriched.sql
│   │   └── int_repositories_classified.sql
│   └── marts/
│       ├── _marts_models.yml
│       ├── obt_plc_tags.sql
│       └── obt_repositories.sql
├── tests/
│   ├── assert_plc_tags_tienen_todas_las_zonas.sql   ← Singular test 1
│   └── assert_repositories_fechas_validas.sql        ← Singular test 2
├── dbt_project.yml
└── .gitignore
```

### Linaje de datos (DAG)

```
source: airbyte_raw.plc_tags
    └── stg_airbyte__plc_tags
            └── int_plc_tags_enriched
                    └── obt_plc_tags  ✅

source: airbyte_raw.repositories
    └── stg_airbyte__repositories
            └── int_repositories_classified
                    └── obt_repositories  ✅
```

---

## 🧪 Tests incluidos

### Tests genéricos (5+)
| Test | Modelo | Columna |
|------|--------|---------|
| `not_null` | stg_airbyte__plc_tags | tag_name, data_type, logical_address, address_prefix, is_hmi_visible |
| `unique` | stg_airbyte__plc_tags | tag_name, logical_address |
| `accepted_values` | stg_airbyte__plc_tags | address_prefix (%I, %Q, %M) |
| `not_null` | stg_airbyte__repositories | repository_id, repository_name, organization_name |
| `unique` | stg_airbyte__repositories | repository_id |

### Tests dbt-expectations (3)
| Test | Modelo | Columna | Qué verifica |
|------|--------|---------|--------------|
| `expect_column_values_to_match_regex` | stg_airbyte__plc_tags | logical_address | Formato `%I0.2` válido |
| `expect_column_value_lengths_to_be_between` | stg_airbyte__repositories | repository_name | Longitud entre 1 y 100 caracteres |
| `expect_column_values_to_match_regex` | stg_airbyte__repositories | clone_url | URL válida de GitHub |

### Singular tests (2)
| Archivo | Qué verifica |
|---------|--------------|
| `assert_plc_tags_tienen_todas_las_zonas.sql` | Existen variables en las 3 zonas (Entrada, Salida, Marca) |
| `assert_repositories_fechas_validas.sql` | Ningún repo tiene fecha futura o anterior a 2008 |

---

## ⚙️ Configuración

### 1. Requisitos

```bash
pip install dbt-core dbt-duckdb
```

### 2. Configurar credenciales de MotherDuck

Crear archivo `profiles.yml` en la raíz del proyecto:

```yaml
airbyte_curso:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'md:airbyte_curso'
      token: "TOKEN_DE_MOTHERDUCK"
      schema: main
      threads: 4
```

> ⚠️ **Nunca subas `profiles.yml` a Git.** Está en `.gitignore`.

### 3. Instalar paquetes dbt

```bash
dbt deps
```

---

## 🚀 Uso

```bash
# Verificar conexión
dbt debug

# Instalar paquetes (dbt-expectations)
dbt deps

# Correr todos los modelos
dbt run

# Correr todos los tests
dbt test

# Correr modelos + tests juntos (recomendado para entrega)
dbt build

# Generar documentación y DAG
dbt docs generate
dbt docs serve
```

---

## 📚 Referencias

- [dbt Documentation](https://docs.getdbt.com)
- [dbt-expectations](https://github.com/calogica/dbt-expectations)
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb)
- [MotherDuck docs](https://motherduck.com/docs)
