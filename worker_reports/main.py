"""
Orquestador del Worker Reports — BanAnalytics.

Punto de entrada único del microservicio. Implementa el pipeline completo
del RF-08 (Generación y Emisión Automática de Reporte Semanal):

    1.  Verificación de conexión a Neon (fail-fast).
    2.  Lectura del modo de ejecución (DRY_RUN desde variable de entorno).
    3.  Obtención de todas las tiendas activas con email registrado.
    4.  Por cada tienda:
        a. Consulta de predicciones de los próximos N días.
        b. Validación de datos mínimos (sin predicciones → warning + skip).
        c. Cálculo de métricas agregadas (WeeklyStats).
        d. Cálculo de breakdown por categoría (CategoryStats).
        e. Renderizado HTML → PDF en memoria (renderer.py).
        f. Envío del correo con PDF adjunto (mailer.py) — omitido en DRY_RUN.
        g. Registro del resultado (éxito/fallo) por tienda.
    5.  Resumen final del run con conteos de éxito/fallo.
    6.  Código de salida: sys.exit(0) si al menos un éxito; sys.exit(1) si
        todos fallaron o hubo error crítico irrecuperable.

Modo DRY_RUN (variable de entorno DRY_RUN=true):
    Activado mediante el input `dry_run` del workflow_dispatch en el YAML.
    En este modo el pipeline completo se ejecuta (consulta BD, agrega stats,
    renderiza PDF) pero el paso de envío SMTP se omite. Útil para validar
    el sistema en desarrollo, probar templates y verificar que las consultas
    a Neon devuelven datos correctos sin enviar correos reales a los tenderos.

Manejo de errores por tienda:
    Cada tienda se procesa en su propio bloque try/except. Un fallo en la
    tienda N no interrumpe el procesamiento de la tienda N+1. El principio
    de resiliencia operativa del ERS (RF-08 §3) se implementa aquí: el
    sistema registra el fallo, continúa y genera el resumen al final.

Código de salida:
    0 → al menos una tienda procesada exitosamente (GitHub Actions: verde).
    1 → error crítico irrecuperable (sin conexión a BD, sin tiendas activas,
        todos los envíos fallaron). GitHub Actions notifica por email.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from db.db_queries import (
    StoreRecord,
    WeeklyStats,
    compute_category_breakdown,
    compute_weekly_stats,
    get_all_active_stores,
    get_upcoming_predictions,
    verify_connection,
)
from config import get_settings
from db import (
    StoreRecord,
    WeeklyStats,
    compute_category_breakdown,
    compute_weekly_stats,
    get_all_active_stores,
    get_upcoming_predictions,
    verify_connection,
)
from services import render_report_pdf, send_report
from utils import get_logger

logger = get_logger(__name__)
_settings = get_settings()

# Separador visual para los logs de GitHub Actions

_SEP = "═" * 68


def _is_dry_run() -> bool:
    """
    Determina si el worker debe ejecutarse en modo prueba (sin envío SMTP).

    Lee la variable de entorno DRY_RUN establecida por el workflow de GitHub
    Actions cuando se ejecuta manualmente con el input dry_run=true. Acepta
    las variantes de string "true", "1" y "yes" (insensible a mayúsculas)
    para mayor robustez.

    Returns:
        True si DRY_RUN está activado, False en caso contrario.
    """
    raw = os.environ.get("DRY_RUN", "false").strip().lower()
    return raw in {"true", "1", "yes"}


def _format_timestamp(dt: datetime) -> str:
    """
    Formatea un datetime UTC al formato de visualización del reporte.

    Args:
        dt: Datetime en UTC (timezone-aware).

    Returns:
        String con formato "DD/MM/YYYY HH:MM UTC".
    """
    return dt.strftime("%d/%m/%Y %H:%M UTC")


def _process_store(
    store: StoreRecord,
    generated_at: str,
    dry_run: bool,
) -> bool:
    """
    Ejecuta el pipeline completo (consultar → agregar → renderizar → enviar)
    para una tienda individual.

    El aislamiento de cada tienda en su propia función garantiza que cualquier
    excepción no capturada sea contenida y no afecte al resto del pipeline.

    Args:
        store:        Datos de la tienda a procesar.
        generated_at: Timestamp formateado de inicio del run.
        dry_run:      Si True, omite el envío SMTP y loguea el resultado simulado.

    Returns:
        True si el reporte fue generado (y enviado correctamente en modo normal).
        False si ocurrió algún error en cualquier paso del pipeline.
    """
    store_label = f"Tienda {store.store_id} ({store.owner_name}, {store.city})"

    # Paso A: Consultar predicciones
    try:
        predictions = get_upcoming_predictions(
            store_id=store.store_id,
            days=_settings.report_days,
        )
    except Exception as exc:
        logger.error(f"  ❌ {store_label}: Error al consultar predicciones: {exc}")
        return False

    # Paso B: Validar datos mínimos
    if not predictions:
        logger.warning(
            f"  ⚠️  {store_label}: Sin predicciones disponibles para los "
            f"próximos {_settings.report_days} días. "
            "¿El worker ML se ejecutó correctamente esta semana? "
            "Se omite el envío del reporte para esta tienda."
        )
        return False

    # Paso C: Calcular métricas agregadas
    try:
        stats: WeeklyStats = compute_weekly_stats(
            predictions=predictions,
            max_featured=_settings.max_featured,
        )
    except Exception as exc:
        logger.error(f"  ❌ {store_label}: Error al calcular estadísticas: {exc}")
        return False

    # Paso D: Calcular breakdown por categoría
    try:
        category_breakdown = compute_category_breakdown(predictions)
    except Exception as exc:
        logger.error(
            f"  ❌ {store_label}: Error al calcular breakdown por categoría: {exc}"
        )
        return False

    # Paso E: Renderizar PDF en memoria
    try:
        pdf_bytes = render_report_pdf(
            store=store,
            stats=stats,
            category_breakdown=category_breakdown,
            generated_at=generated_at,
        )
    except FileNotFoundError as exc:
        logger.error(
            f"  ❌ {store_label}: Recurso del template no encontrado: {exc}. "
            "Verifique que templates/styles.css y templates/assets/logo_bananalytics.png "
            "existen en el directorio del worker."
        )
        return False
    except Exception as exc:
        logger.error(f"  ❌ {store_label}: Error al renderizar PDF: {exc}")
        return False

    # Paso F: Enviar correo (o simular en DRY_RUN)
    if dry_run:
        logger.info(
            f"  🧪 [DRY_RUN] Envío simulado para {store.email} — "
            f"PDF generado correctamente ({len(pdf_bytes) / 1024:.1f} KB). "
            "No se realizó ninguna conexión SMTP."
        )
        return True

    success = send_report(
        store=store,
        stats=stats,
        pdf_bytes=pdf_bytes,
        generated_at=generated_at,
    )

    if not success:
        logger.error(
            f"  ❌ {store_label}: El envío del correo falló. "
            "Verifique las credenciales SMTP en los secrets de GitHub Actions."
        )
        return False

    logger.info(
        f"  🏆 {store_label}: Pipeline completado exitosamente. "
        f"({stats.unique_products} productos, "
        f"{stats.unique_featured_products} alertas prioritarias)"
    )
    return True


def _print_run_summary(
    stores: list[StoreRecord],
    results: dict[int, bool],
    start_time: datetime,
    end_time: datetime,
    dry_run: bool,
) -> None:
    """
    Imprime el resumen final del run en los logs de GitHub Actions.

    Args:
        stores:     Lista completa de tiendas procesadas.
        results:    Mapa de store_id → éxito/fallo.
        start_time: Timestamp de inicio del run.
        end_time:   Timestamp de fin del run.
        dry_run:    True si se ejecutó en modo prueba.
    """
    duration_seconds = (end_time - start_time).total_seconds()
    duration_min = duration_seconds / 60

    successful = [sid for sid, ok in results.items() if ok]
    failed = [sid for sid, ok in results.items() if not ok]
    skipped = [s.store_id for s in stores if s.store_id not in results]

    dry_run_notice = " [MODO DRY_RUN — sin envíos SMTP reales]" if dry_run else ""

    logger.info(_SEP)
    logger.info(f"  🏁  RESUMEN DEL RUN — Worker Reports BanAnalytics{dry_run_notice}")
    logger.info(f"  📅  Inicio : {_format_timestamp(start_time)}")
    logger.info(f"  📅  Fin    : {_format_timestamp(end_time)}")
    logger.info(f"  ⏱   Duración: {duration_min:.1f} min ({duration_seconds:.0f} seg)")
    logger.info(f"  📊  Tiendas activas      : {len(stores)}")
    logger.info(f"  ✅  Reportes exitosos    : {len(successful)}")
    logger.info(f"  ❌  Fallos               : {len(failed)}")
    logger.info(f"  ⚠️   Omitidas (sin datos) : {len(skipped)}")

    if failed:
        failed_labels = [f"ID {sid}" for sid in failed]
        logger.warning(
            f"  Tiendas con fallo: {', '.join(failed_labels)}. "
            "Revisar logs individuales arriba para diagnóstico."
        )

    logger.info(_SEP)


def main() -> None:
    """
    Punto de entrada principal del Worker Reports.

    Implementa el pipeline completo del RF-08 con fail-fast en los pasos
    críticos (conexión a BD, existencia de tiendas) y resiliencia por tienda
    en el procesamiento individual.

    Código de salida:
        0 — al menos un reporte generado exitosamente.
        1 — fallo crítico o todos los envíos fallaron.
    """
    start_time = datetime.now(timezone.utc)
    generated_at = _format_timestamp(start_time)
    dry_run = _is_dry_run()

    logger.info(_SEP)
    logger.info(f"  🚀  BanAnalytics Worker Reports — {generated_at}")
    if dry_run:
        logger.info("  🧪  MODO DRY_RUN ACTIVO — Los PDFs se generarán pero NO se enviarán")
    logger.info(f"  📅  Horizonte del reporte    : {_settings.report_days} días")
    logger.info(f"  ⭐  Máx. alertas prioritarias: {_settings.max_featured}")
    logger.info(f"  📧  Remitente SMTP           : {_settings.smtp_from}")
    logger.info(f"  🔗  Servidor SMTP             : {_settings.smtp_host}:{_settings.smtp_port}")
    logger.info(_SEP)

    # Paso 1: Verificación de conexión a Neon
    if not verify_connection():
        logger.critical(
            "❌ Sin conexión a la base de datos Neon. "
            "El worker no puede continuar sin acceso a los datos de predicciones. "
            "Verifique DATABASE_URL en los secrets de GitHub Actions."
        )
        sys.exit(1)

    # Paso 2: Obtención de tiendas activas
    stores: list[StoreRecord] = get_all_active_stores()

    if not stores:
        logger.warning(
            "⚠️  No se encontraron tiendas con email registrado en stores_database. "
            "No hay reportes que generar. "
            "Verifique que el registro de tiendas se haya completado correctamente."
        )
        sys.exit(0)

    logger.info(
        f"📋 Procesando {len(stores)} tienda(s) activa(s) con email registrado..."
    )

    # Paso 3: Pipeline por tienda
    results: dict[int, bool] = {}

    for i, store in enumerate(stores, 1):
        logger.info(f"\n[{i:>3}/{len(stores)}] {'-' * 55}")
        logger.info(
            f"  📦 Procesando: {store.owner_name} "
            f"(Tienda {store.store_id}, {store.city})"
        )
        logger.info(f"  📧 Destinatario: {store.email}")

        try:
            success = _process_store(
                store=store,
                generated_at=generated_at,
                dry_run=dry_run,
            )
            results[store.store_id] = success

        except Exception as exc:
            # Captura de última instancia: garantiza que un error inesperado
            # en _process_store no detenga el loop principal.
            logger.error(
                f"  💥 Error inesperado procesando tienda {store.store_id} "
                f"({store.owner_name}): {exc}",
                exc_info=True,
            )
            results[store.store_id] = False

    # Paso 4: Resumen del run
    end_time = datetime.now(timezone.utc)
    _print_run_summary(
        stores=stores,
        results=results,
        start_time=start_time,
        end_time=end_time,
        dry_run=dry_run,
    )

    # Paso 5: Código de salida
    successful_count = sum(1 for ok in results.values() if ok)

    if successful_count == 0 and len(results) > 0:
        logger.critical(
            "💀 Todos los reportes fallaron. "
            "Revisar credenciales SMTP, configuración de red y estado de Neon."
        )
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()

