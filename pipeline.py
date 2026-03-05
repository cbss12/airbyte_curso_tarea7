# pipeline.py
# Pipeline completo con Prefect 3
# Ejecuta dbt run + dbt test sobre los modelos de airbyte_curso

import os
import subprocess
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# Cargar variables de entorno desde .env
load_dotenv("pipeline.env")

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")


def run_dbt_command(command: list[str]) -> str:
    """Ejecuta un comando dbt y retorna el output."""
    logger = get_run_logger()

    full_command = [
        "dbt", *command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]

    logger.info(f"Ejecutando: {' '.join(full_command)}")

    env = os.environ.copy()
    env["MOTHERDUCK_TOKEN"] = MOTHERDUCK_TOKEN

    result = subprocess.run(
        full_command,
        capture_output=True,
        text=True,
        env=env
    )

    if result.returncode != 0:
        logger.error(result.stdout)
        logger.error(result.stderr)
        raise Exception(f"Comando dbt falló: {' '.join(command)}")

    logger.info(result.stdout)
    return result.stdout


# ── TASKS ──────────────────────────────────────────────────────────────────

@task(name="dbt deps", retries=1)
def task_dbt_deps():
    """Instala los paquetes dbt (dbt-expectations)."""
    return run_dbt_command(["deps"])


@task(name="dbt run - staging")
def task_dbt_run_staging():
    """Ejecuta los modelos de staging."""
    return run_dbt_command(["run", "--select", "staging.*"])


@task(name="dbt run - intermediate")
def task_dbt_run_intermediate():
    """Ejecuta los modelos intermediate."""
    return run_dbt_command(["run", "--select", "intermediate.*"])


@task(name="dbt run - marts")
def task_dbt_run_marts():
    """Ejecuta los modelos marts (OBT finales)."""
    return run_dbt_command(["run", "--select", "marts.*"])


@task(name="dbt test")
def task_dbt_test():
    """Ejecuta todos los tests del proyecto."""
    return run_dbt_command(["test"])


@task(name="dbt docs generate")
def task_dbt_docs():
    """Genera la documentación y el DAG."""
    return run_dbt_command(["docs", "generate"])


# ── FLOW ───────────────────────────────────────────────────────────────────

@flow(name="airbyte_curso - Pipeline dbt", log_prints=True)
def pipeline_dbt():
    """
    Pipeline completo del proyecto airbyte_curso.

    Orden de ejecución:
    1. dbt deps       → instala paquetes
    2. staging        → limpieza de datos crudos
    3. intermediate   → enriquecimiento y clasificación
    4. marts          → tablas finales OBT
    5. test           → validación de calidad
    6. docs generate  → documentación y DAG
    """
    logger = get_run_logger()
    logger.info("Iniciando pipeline airbyte_curso")

    # 1. Instalar paquetes
    task_dbt_deps()

    # 2. Staging
    task_dbt_run_staging()

    # 3. Intermediate
    task_dbt_run_intermediate()

    # 4. Marts
    task_dbt_run_marts()

    # 5. Tests
    task_dbt_test()

    # 6. Docs
    task_dbt_docs()

    logger.info("Pipeline completado exitosamente")


if __name__ == "__main__":
    pipeline_dbt()
