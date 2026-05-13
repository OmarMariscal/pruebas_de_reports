"""
Script de prueba local — Worker Reports BanAnalytics.

Replica el pipeline exacto de producción (main.py) para UNA tienda específica,
con control granular sobre cada paso: persistencia en Neon, envío SMTP y
guardado en disco del PDF, todos activables de forma independiente.

─── MODOS DE USO ─────────────────────────────────────────────────────────────

  # 1. Ver qué tiendas existen en la base de datos:
  python test_single_report.py --list-stores

  # 2. Ver los reportes ya almacenados en Neon para una tienda:
  python test_single_report.py --store-id 1 --list-reports

  # 3. Solo generar el PDF y guardarlo en disco (cero efectos secundarios):
  python test_single_report.py --store-id 1 --save-pdf

  # 4. Generar + enviar correo (sin escribir en Neon):
  python test_single_report.py --store-id 1 --to yo@gmail.com

  # 5. Generar + persistir en Neon (sin enviar correo):
  python test_single_report.py --store-id 1 --persist-db

  # 6. Pipeline completo identico a produccion:
  python test_single_report.py --store-id 1 --to yo@gmail.com --persist-db --save-pdf

─── COMPORTAMIENTO POR DEFECTO ───────────────────────────────────────────────

  · --persist-db está DESACTIVADO por defecto. Debes activarlo explícitamente
    para escribir en reports_database. Esto protege Neon de datos de prueba
    generados durante iteraciones de desarrollo.

  · --to es opcional. Sin él, el correo se envía al email registrado de la
    tienda en stores_database. Con él, se sobreescribe el destinatario para
    esa ejecución sin modificar la BD.

─── REQUISITOS ───────────────────────────────────────────────────────────────

  · Archivo .env en worker_reports/ con DATABASE_URL y credenciales SMTP.
    (SMTP solo requerido si se usa --to o el email registrado de la tienda).
  · Dependencias de sistema para WeasyPrint instaladas (ver README del proyecto).
  · venv activo con: pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

# ── Garantiza que los módulos del worker se encuentran en el path ─────────────
# Necesario al ejecutar el script desde cualquier directorio que no sea
# worker_reports/. El insert en índice 0 da prioridad a los módulos locales
# sobre cualquier paquete instalado con el mismo nombre.
sys.path.insert(0, str(Path(__file__).parent))

from db.db_queries import (
    ReportRecord,
    StoreRecord,
    compute_category_breakdown,
    compute_weekly_stats,
    ensure_reports_table_exists,
    get_all_active_stores,
    get_upcoming_predictions,
    save_report,
    verify_connection,
)
from utils.logger import get_logger
from services.mailer import send_report as send_report_email
from services.renderer import render_report_pdf
from config.settings import get_settings

logger = get_logger("test_single_report")
_SEP = "═" * 62


def _format_timestamp(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y %H:%M UTC")


# ─────────────────────────────────────────────────────────────────────────────
# COMANDO: --list-stores
# ─────────────────────────────────────────────────────────────────────────────

def cmd_list_stores() -> None:
    """
    Lista todas las tiendas con email registrado en stores_database.
    Muestra ID, propietario, ciudad y correo en formato tabular.
    """
    logger.info("Consultando tiendas activas en Neon...")

    if not verify_connection():
        logger.error("❌ Sin conexión a Neon. Verifica DATABASE_URL en .env")
        sys.exit(1)

    stores = get_all_active_stores()

    if not stores:
        logger.warning(
            "⚠️  No hay tiendas con email registrado en stores_database.\n"
            "   Verifica que el registro de tiendas se haya completado."
        )
        sys.exit(0)

    print("\n" + _SEP)
    print(f"  {'ID':>4}  {'Propietario':<26}  {'Ciudad':<16}  Email")
    print("─" * 62)
    for s in stores:
        print(f"  {s.store_id:>4}  {s.owner_name:<26}  {s.city:<16}  {s.email}")
    print(_SEP)
    print(f"  Total: {len(stores)} tienda(s)\n")


# ─────────────────────────────────────────────────────────────────────────────
# COMANDO: --list-reports
# ─────────────────────────────────────────────────────────────────────────────

def cmd_list_reports(store_id: int) -> None:
    """
    Lista los reportes almacenados en reports_database para una tienda.

    Consulta únicamente los campos de metadatos (sin leer el BYTEA pdf_content
    para no saturar la red). Muestra report_id, fecha de creación, período
    cubierto y tamaño del PDF, ordenados del más reciente al más antiguo.

    Esta función es el equivalente local de consultar el dashboard de Neon:
    permite verificar que save_report() funcionó correctamente sin necesidad
    de abrir el navegador ni acceder al panel de administración de la BD.

    Args:
        store_id: ID de la tienda a consultar.
    """
    from sqlalchemy import text as sqla_text
    from db_queries import get_session

    logger.info(f"Consultando reportes en Neon para tienda {store_id}...")

    if not verify_connection():
        logger.error("❌ Sin conexión a Neon. Verifica DATABASE_URL en .env")
        sys.exit(1)

    # Garantiza que la tabla existe antes de consultarla.
    # En un entorno de prueba limpio puede no existir todavía.
    try:
        ensure_reports_table_exists()
    except Exception as exc:
        logger.error(f"❌ No se pudo verificar reports_database: {exc}")
        sys.exit(1)

    query = sqla_text("""
        SELECT
            report_id,
            created_at AT TIME ZONE 'UTC'  AS created_at_utc,
            period_from,
            period_to,
            file_size_kb
        FROM reports_database
        WHERE store_id = :store_id
        ORDER BY created_at DESC
    """)

    with get_session() as session:
        result = session.execute(query, {"store_id": store_id})
        rows = result.fetchall()

    if not rows:
        logger.warning(
            f"⚠️  No hay reportes almacenados para la tienda {store_id}.\n"
            "   Ejecuta con --persist-db para generar y persistir el primero."
        )
        return

    print("\n" + _SEP)
    print(
        f"  Reportes en Neon — Tienda {store_id}  "
        f"({len(rows)} de máx. 13)\n"
    )
    print(
        f"  {'ID':>10}  {'Creado (UTC)':<20}  "
        f"{'Período cubierto':<26}  Tamaño"
    )
    print("─" * 62)

    for row in rows:
        report_id, created_at, period_from, period_to, size_kb = row
        created_str  = created_at.strftime("%d/%m/%Y %H:%M")
        period_str   = (
            f"{period_from.strftime('%d/%m/%Y')} → "
            f"{period_to.strftime('%d/%m/%Y')}"
        )
        print(
            f"  {report_id:>10}  {created_str:<20}  "
            f"{period_str:<26}  {size_kb} KB"
        )

    print(_SEP + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# COMANDO PRINCIPAL: pipeline de generación
# ─────────────────────────────────────────────────────────────────────────────

def cmd_run_pipeline(
    store_id:       int,
    override_email: str | None,
    save_pdf:       bool,
    persist_db:     bool,
) -> None:
    """
    Ejecuta el pipeline completo de generación de reporte para una tienda.

    Replica fielmente los pasos A–F de _process_store() en main.py, con la
    misma lógica de independencia entre E2 (persistencia) y F (correo):
    un fallo en E2 no bloquea F, y viceversa.

    Args:
        store_id:       ID de la tienda a procesar.
        override_email: Si se indica, sobreescribe el email de la tienda para
                        esta ejecución. El email en la BD no se modifica.
        save_pdf:       Si True, escribe el PDF en disco en worker_reports/.
        persist_db:     Si True, ejecuta el paso E2: INSERT en reports_database
                        y limpieza de reportes fuera de la ventana de retención.
    """
    _settings = get_settings()
    now = datetime.now(timezone.utc)
    generated_at = _format_timestamp(now)

    print("\n" + _SEP)
    logger.info("🧪 TEST — Worker Reports BanAnalytics")
    logger.info(f"📅 Generado          : {generated_at}")
    logger.info(f"🏪 Store ID          : {store_id}")
    logger.info(f"💾 Persistir en Neon : {'SI' if persist_db else 'NO (--persist-db para activar)'}")
    logger.info(f"📧 Enviar correo     : {'SI → ' + override_email if override_email else 'Al email de la tienda'}")
    logger.info(f"🖨  Guardar PDF       : {'SI' if save_pdf else 'NO'}")
    print("─" * 62 + "\n")

    # ── Paso 0: Verificar conexión ────────────────────────────────────────────
    if not verify_connection():
        logger.error("❌ Sin conexión a Neon. Verifica DATABASE_URL en .env")
        sys.exit(1)

    # ── Paso 0.5: Crear tabla si persist_db está activo ───────────────────────
    if persist_db:
        try:
            ensure_reports_table_exists()
        except Exception as exc:
            logger.error(
                f"❌ No se pudo verificar/crear reports_database: {exc}.\n"
                "   Verifica permisos DDL del usuario en DATABASE_URL."
            )
            sys.exit(1)

    # ── Paso A: Buscar la tienda ──────────────────────────────────────────────
    all_stores = get_all_active_stores()
    store: StoreRecord | None = next(
        (s for s in all_stores if s.store_id == store_id), None
    )

    if store is None:
        logger.error(
            f"❌ No se encontró ninguna tienda con ID={store_id} "
            "con email registrado.\n"
            "   Ejecuta: python test_single_report.py --list-stores"
        )
        sys.exit(1)

    logger.info(
        f"✅ Tienda encontrada: {store.owner_name} "
        f"({store.city}) — {store.email}"
    )

    # StoreRecord es frozen=True → se usa replace() para copia inmutable.
    if override_email:
        store = replace(store, email=override_email)
        logger.info(f"📧 Email sobreescrito para esta prueba: {store.email}")

    # ── Paso B: Consultar predicciones ────────────────────────────────────────
    logger.info(
        f"🔍 Consultando predicciones "
        f"(próximos {_settings.report_days} días)..."
    )
    predictions = get_upcoming_predictions(
        store_id=store.store_id,
        days=_settings.report_days,
    )

    if not predictions:
        logger.warning(
            "⚠️  Sin predicciones disponibles para esta tienda.\n"
            "   Posibles causas:\n"
            "   · El worker ML no se ha ejecutado aún esta semana.\n"
            "   · Todas las fechas de predicción ya pasaron.\n"
            "   · La tienda no tiene ventas en sales_database."
        )
        sys.exit(0)

    logger.info(f"✅ {len(predictions)} filas de predicción encontradas.")

    # ── Pasos C+D: Calcular estadísticas ──────────────────────────────────────
    logger.info("📊 Calculando métricas y breakdown por categoría...")
    stats = compute_weekly_stats(
        predictions=predictions,
        max_featured=_settings.max_featured,
    )
    category_breakdown = compute_category_breakdown(predictions)

    logger.info(
        f"   Período             : "
        f"{stats.date_range_start.strftime('%d/%m/%Y')} → "
        f"{stats.date_range_end.strftime('%d/%m/%Y')}\n"
        f"   Productos únicos    : {stats.unique_products}\n"
        f"   En Déficit          : {stats.deficit_products} productos\n"
        f"   En Superávit        : {stats.superavit_products} productos\n"
        f"   Alertas prioritarias: {stats.unique_featured_products} productos\n"
        f"   Categorías          : {len(category_breakdown)}"
    )

    # ── Paso E: Renderizar PDF en memoria ─────────────────────────────────────
    logger.info("🖨  Renderizando PDF con WeasyPrint...")
    try:
        pdf_bytes = render_report_pdf(
            store=store,
            stats=stats,
            category_breakdown=category_breakdown,
            generated_at=generated_at,
        )
    except FileNotFoundError as exc:
        logger.error(
            f"❌ Recurso del template no encontrado: {exc}.\n"
            "   Verifica que existen:\n"
            "   · templates/styles.css\n"
            "   · templates/assets/logo_bananalytics.png"
        )
        sys.exit(1)
    except Exception as exc:
        logger.error(f"❌ Error al renderizar PDF: {exc}")
        sys.exit(1)

    pdf_kb = len(pdf_bytes) / 1024
    logger.info(f"✅ PDF generado en memoria: {pdf_kb:.1f} KB")

    # ── Paso E (disco): Guardar PDF localmente ────────────────────────────────
    if save_pdf:
        pdf_filename = (
            f"test_reporte_tienda_{store.store_id}_"
            f"{now.strftime('%Y%m%d_%H%M%S')}.pdf"
        )
        pdf_path = Path(__file__).parent / pdf_filename
        pdf_path.write_bytes(pdf_bytes)
        logger.info(
            f"💾 PDF guardado en disco: {pdf_path.resolve()}\n"
            "   Ábrelo con cualquier visor PDF para inspeccionar el diseño."
        )

    # ── Paso E2: Persistir en Neon ────────────────────────────────────────────
    # Replica el comportamiento de _process_store() en main.py:
    # un fallo aquí es no bloqueante — el pipeline continúa hacia el correo.
    db_persisted: bool = False
    report_record: ReportRecord | None = None

    if persist_db:
        logger.info("💾 Persistiendo PDF en reports_database de Neon...")
        try:
            report_record = save_report(
                store_id=store.store_id,
                pdf_bytes=pdf_bytes,
                period_from=stats.date_range_start,
                period_to=stats.date_range_end,
            )
            db_persisted = True
            logger.info(
                f"✅ Reporte persistido en Neon:\n"
                f"   report_id  : {report_record.report_id}\n"
                f"   store_id   : {report_record.store_id}\n"
                f"   created_at : "
                f"{report_record.created_at.strftime('%d/%m/%Y %H:%M UTC')}\n"
                f"   Período    : "
                f"{report_record.period_from.strftime('%d/%m/%Y')} → "
                f"{report_record.period_to.strftime('%d/%m/%Y')}\n"
                f"   Tamaño     : {report_record.file_size_kb} KB\n"
                f"\n   Verifica con: python test_single_report.py "
                f"--store-id {store.store_id} --list-reports"
            )
        except Exception as exc:
            logger.error(
                f"❌ Fallo al persistir en Neon: {exc}\n"
                "   El pipeline continuará hacia el envío de correo."
            )
    else:
        logger.info(
            "⏭  Paso E2 omitido (--persist-db no activado).\n"
            "   Para escribir en Neon, agrega --persist-db al comando."
        )

    # ── Paso F: Enviar correo ─────────────────────────────────────────────────
    logger.info(f"\n📧 Enviando correo a {store.email}...")
    try:
        email_sent = send_report_email(
            store=store,
            stats=stats,
            pdf_bytes=pdf_bytes,
            generated_at=generated_at,
        )
    except Exception as exc:
        logger.error(f"❌ Excepción inesperada al enviar correo: {exc}")
        email_sent = False

    # ── Resumen final ─────────────────────────────────────────────────────────
    print("\n" + _SEP)
    logger.info("📋 RESUMEN DE LA PRUEBA")
    print("─" * 62)

    # Cada entrada: (nombre del paso, resultado: True/False/None=omitido)
    steps = [
        ("PDF generado en memoria",     True),
        ("PDF guardado en disco",        save_pdf   if save_pdf   else None),
        ("Persistido en Neon (E2)",      db_persisted if persist_db else None),
        ("Correo enviado (F)",           email_sent),
    ]

    for step_name, result in steps:
        if result is None:
            print(f"  ⏭  {step_name:<35} omitido")
        elif result:
            print(f"  ✅ {step_name:<35} OK")
        else:
            print(f"  ❌ {step_name:<35} FALLÓ")

    print(_SEP + "\n")

    if not email_sent:
        logger.warning(
            "⚠️  El correo no fue entregado. Verifica en .env:\n"
            "   · SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD\n"
            "   · Si usas Gmail, necesitas una App Password.\n"
            "   · Revisa los logs de error arriba para el motivo exacto."
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Prueba local del Worker Reports — BanAnalytics.\n"
            "Replica el pipeline de producción para UNA tienda, "
            "con control individual sobre cada paso."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python test_single_report.py --list-stores\n"
            "  python test_single_report.py --store-id 1 --list-reports\n"
            "  python test_single_report.py --store-id 1 --save-pdf\n"
            "  python test_single_report.py --store-id 1 --to yo@gmail.com\n"
            "  python test_single_report.py --store-id 1 --persist-db\n"
            "  python test_single_report.py --store-id 1 "
            "--to yo@gmail.com --persist-db --save-pdf\n"
        ),
    )

    parser.add_argument(
        "--list-stores",
        action="store_true",
        help=(
            "Lista todas las tiendas con email registrado en stores_database. "
            "Muestra los IDs disponibles para usar con --store-id."
        ),
    )
    parser.add_argument(
        "--store-id",
        type=int,
        default=None,
        metavar="ID",
        help=(
            "ID numérico de la tienda a procesar. "
            "Requerido para todos los modos excepto --list-stores."
        ),
    )
    parser.add_argument(
        "--list-reports",
        action="store_true",
        help=(
            "Lista los reportes almacenados en Neon para --store-id. "
            "Muestra report_id, fecha, período y tamaño sin leer el binario."
        ),
    )
    parser.add_argument(
        "--to",
        type=str,
        default=None,
        metavar="EMAIL",
        help=(
            "Dirección de correo destino para esta prueba. "
            "Sobreescribe el email registrado de la tienda sin modificar la BD."
        ),
    )
    parser.add_argument(
        "--persist-db",
        action="store_true",
        help=(
            "Activa el paso E2: INSERT del PDF en reports_database y limpieza "
            "de históricos según la política de retención (13 reportes/tienda). "
            "Desactivado por defecto para proteger Neon de datos de prueba."
        ),
    )
    parser.add_argument(
        "--save-pdf",
        action="store_true",
        help=(
            "Guarda el PDF generado en worker_reports/ con nombre "
            "test_reporte_tienda_{id}_{timestamp}.pdf."
        ),
    )

    args = parser.parse_args()

    # ── Enrutamiento ──────────────────────────────────────────────────────────
    if args.list_stores:
        cmd_list_stores()
        return

    if args.store_id is None:
        parser.print_help()
        print(
            f"\n{'─' * 62}\n"
            "❌ ERROR: Debes indicar --store-id.\n"
            "   Usa --list-stores para ver los IDs disponibles.\n"
        )
        sys.exit(1)

    if args.list_reports:
        cmd_list_reports(store_id=args.store_id)
        return

    cmd_run_pipeline(
        store_id=args.store_id,
        override_email=args.to,
        save_pdf=args.save_pdf,
        persist_db=args.persist_db,
    )


if __name__ == "__main__":
    main()