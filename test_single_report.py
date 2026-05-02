"""
Script de prueba local — Worker Reports BanAnalytics.

Ejecuta el pipeline completo de generación y envío de reporte para UNA
sola tienda, enviando el correo a la dirección que tú elijas.

Diseñado para validar el sistema en local antes de hacer el merge al
repositorio, sin necesidad de GitHub Actions ni de tocar código de producción.

USO:
    # Desde el directorio worker_reports/ con el venv activo:

    # Ver qué tiendas existen en la base de datos:
    python test_single_report.py --list-stores

    # Enviar reporte de la tienda con ID 1 a tu correo:
    python test_single_report.py --store-id 1 --to tu@correo.com

    # Generar PDF sin enviar (lo guarda en disco para inspección visual):
    python test_single_report.py --store-id 1 --save-pdf

    # Combinado: guardar PDF Y enviarlo:
    python test_single_report.py --store-id 1 --to tu@correo.com --save-pdf

REQUISITOS:
    · Archivo .env en este directorio con DATABASE_URL y credenciales SMTP.
    · Dependencias de sistema instaladas (WeasyPrint).
    · venv activo con pip install -r requirements.txt completado.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Garantiza que los módulos del worker se encuentran en el path
# Necesario al ejecutar el script directamente desde la raíz del repo o desde
# un directorio diferente a worker_reports/.
sys.path.insert(0, str(Path(__file__).parent))

from worker_reports.db_queries import (
    StoreRecord,
    compute_category_breakdown,
    compute_weekly_stats,
    get_all_active_stores,
    get_upcoming_predictions,
    verify_connection,
)
from worker_reports.logger import get_logger
from worker_reports.mailer import send_report
from worker_reports.renderer import render_report_pdf, render_report_html
from worker_reports.settings import get_settings

logger = get_logger("test_single_report")


def _format_timestamp(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y %H:%M UTC")


def cmd_list_stores() -> None:
    """
    Lista todas las tiendas disponibles en la base de datos con su email
    registrado. Útil para conocer los IDs antes de elegir cuál probar.
    """
    logger.info("Consultando tiendas activas en la base de datos...")

    if not verify_connection():
        logger.error("❌ No se pudo conectar a la base de datos. Verifica DATABASE_URL en .env")
        sys.exit(1)

    stores = get_all_active_stores()

    if not stores:
        logger.warning("⚠️  No hay tiendas con email registrado en stores_database.")
        sys.exit(0)

    print("\n" + "═" * 60)
    print(f"  {'ID':>4}  {'Propietario':<25}  {'Ciudad':<15}  Email")
    print("─" * 60)
    for s in stores:
        print(f"  {s.store_id:>4}  {s.owner_name:<25}  {s.city:<15}  {s.email}")
    print("═" * 60)
    print(f"  Total: {len(stores)} tienda(s)\n")


def cmd_send_report(
    store_id: int,
    override_email: str | None,
    save_pdf: bool,
) -> None:
    """
    Ejecuta el pipeline completo para una tienda específica.

    Args:
        store_id:       ID de la tienda a procesar.
        override_email: Si se indica, el correo se envía a esta dirección
                        en lugar del email registrado de la tienda.
        save_pdf:       Si True, guarda el PDF en disco para inspección visual.
    """
    _settings = get_settings()
    now = datetime.now(timezone.utc)
    generated_at = _format_timestamp(now)

    print("\n" + "═" * 60)
    logger.info(f"🧪 TEST — Worker Reports BanAnalytics")
    logger.info(f"📅 Generado: {generated_at}")
    logger.info(f"🏪 Store ID objetivo: {store_id}")
    if override_email:
        logger.info(f"📧 Correo destino (override): {override_email}")
    if save_pdf:
        logger.info("💾 El PDF será guardado en disco para inspección visual")
    print("─" * 60 + "\n")

    # 1. Verificar conexión
    if not verify_connection():
        logger.error("❌ Sin conexión a Neon. Verifica DATABASE_URL en .env")
        sys.exit(1)

    # 2. Buscar la tienda solicitada
    all_stores = get_all_active_stores()
    store: StoreRecord | None = next(
        (s for s in all_stores if s.store_id == store_id), None
    )

    if store is None:
        logger.error(
            f"❌ No se encontró ninguna tienda con ID={store_id} que tenga "
            "email registrado.\n"
            "   Ejecuta: python test_single_report.py --list-stores\n"
            "   para ver los IDs disponibles."
        )
        sys.exit(1)

    logger.info(
        f"✅ Tienda encontrada: {store.owner_name} "
        f"({store.city}) — {store.email}"
    )

    # 3. Si se indica override_email, crear un StoreRecord modificado
    # StoreRecord es frozen=True, así que se crea una copia con dataclasses.replace
    # para no modificar el objeto original de la base de datos.
    if override_email:
        from dataclasses import replace
        store = replace(store, email=override_email)
        logger.info(f"📧 Email sobreescrito para esta prueba: {store.email}")

    # 4. Obtener predicciones
    logger.info(f"🔍 Consultando predicciones (próximos {_settings.report_days} días)...")
    predictions = get_upcoming_predictions(
        store_id=store.store_id,
        days=_settings.report_days,
    )

    if not predictions:
        logger.warning(
            "⚠️  No hay predicciones disponibles para esta tienda en el período "
            f"solicitado ({_settings.report_days} días).\n"
            "   Posibles causas:\n"
            "   · El worker ML no se ha ejecutado aún.\n"
            "   · Las predicciones existentes tienen fechas ya pasadas.\n"
            "   · La tienda no tiene ventas registradas en sales_database."
        )
        sys.exit(0)

    logger.info(f"✅ {len(predictions)} filas de predicción encontradas.")

    # 5. Calcular estadísticas
    logger.info("📊 Calculando métricas agregadas...")
    stats = compute_weekly_stats(
        predictions=predictions,
        max_featured=_settings.max_featured,
    )
    category_breakdown = compute_category_breakdown(predictions)

    logger.info(
        f"   Productos únicos   : {stats.unique_products}\n"
        f"   Déficit (productos): {stats.deficit_products}\n"
        f"   Superávit (prods.) : {stats.superavit_products}\n"
        f"   Alertas prioritarias: {stats.unique_featured_products}"
    )

    # 6. Renderizar PDF
    logger.info("🖨  Renderizando PDF...")
    pdf_bytes = render_report_pdf(
        store=store,
        stats=stats,
        category_breakdown=category_breakdown,
        generated_at=generated_at,
    )
    logger.info(f"✅ PDF generado: {len(pdf_bytes) / 1024:.1f} KB")

    # 7. Guardar PDF en disco (opcional)
    if save_pdf:
        pdf_filename = (
            f"test_reporte_tienda_{store.store_id}_"
            f"{now.strftime('%Y%m%d_%H%M%S')}.pdf" # <-- Volver a poner .pdf
        )
        pdf_path = Path(__file__).parent / pdf_filename
        pdf_path.write_bytes(pdf_bytes)
        logger.info(f"💾 PDF guardado en: {pdf_path.resolve()}")
        logger.info("   Ábrelo con cualquier visor PDF para verificar el diseño.")

    # 8. Enviar correo
    logger.info(f"\n📧 Enviando correo a {store.email}...")
    success = send_report(
        store=store,
        stats=stats,
        pdf_bytes=pdf_bytes,
        generated_at=generated_at,
    )

    print("\n" + "═" * 60)
    if success:
        logger.info(
            f"🎉 ¡PRUEBA EXITOSA!\n"
            f"   Correo enviado a: {store.email}\n"
            f"   PDF adjunto    : {len(pdf_bytes) / 1024:.1f} KB\n"
            f"   Revisa tu bandeja de entrada (y el spam si no aparece)."
        )
    else:
        logger.error(
            "❌ El envío del correo falló.\n"
            "   Verifica en .env:\n"
            "   · SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD\n"
            "   · Si usas Gmail, asegúrate de usar una App Password.\n"
            "   · Revisa los logs de error arriba para el motivo exacto."
        )
        sys.exit(1)
    print("═" * 60 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Prueba local del Worker Reports — BanAnalytics.\n"
            "Genera y envía el reporte de UNA tienda a tu correo."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python test_single_report.py --list-stores\n"
            "  python test_single_report.py --store-id 1 --to yo@gmail.com\n"
            "  python test_single_report.py --store-id 1 --save-pdf\n"
            "  python test_single_report.py --store-id 1 --to yo@gmail.com --save-pdf\n"
        ),
    )

    subparsers = parser.add_subparsers(dest="command")

    # Subcomando: --list-stores 
    # También disponible como flag directo para comodidad
    parser.add_argument(
        "--list-stores",
        action="store_true",
        help="Lista todas las tiendas con email registrado en la base de datos.",
    )

    # Argumentos para envío 
    parser.add_argument(
        "--store-id",
        type=int,
        default=None,
        metavar="ID",
        help="ID numérico de la tienda a procesar (usa --list-stores para verlos).",
    )
    parser.add_argument(
        "--to",
        type=str,
        default=None,
        metavar="EMAIL",
        help=(
            "Dirección de correo destino para esta prueba. "
            "Si no se indica, se usa el email registrado de la tienda en la BD."
        ),
    )
    parser.add_argument(
        "--save-pdf",
        action="store_true",
        help=(
            "Guarda el PDF generado en disco (en el directorio worker_reports/) "
            "además de enviarlo por correo. Útil para inspección visual del diseño."
        ),
    )

    args = parser.parse_args()

    # Enrutamiento de comandos
    if args.list_stores:
        cmd_list_stores()
        return

    if args.store_id is None:
        parser.print_help()
        print(
            "\n❌ ERROR: Debes indicar --store-id o --list-stores.\n"
            "   Ejemplo: python test_single_report.py --store-id 1 --to yo@gmail.com\n"
        )
        sys.exit(1)

    cmd_send_report(
        store_id=args.store_id,
        override_email=args.to,
        save_pdf=args.save_pdf,
    )


if __name__ == "__main__":
    main()