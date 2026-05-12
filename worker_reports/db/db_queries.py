"""
Capa de acceso a datos del Worker Reports — BanAnalytics.

Responsabilidades:
  1. Conexión a Neon (PostgreSQL serverless) con NullPool, coherente con la
     filosofía del worker_ml: cada operación abre y cierra su propia conexión,
     ideal para CRON jobs que corren una vez al día.
  2. Definición de estructuras de datos tipadas (dataclasses) que representan
     exactamente lo que el renderizador y el mailer necesitan.
  3. Consultas SQL parametrizadas y seguras (sin concatenación de strings).
  4. Funciones de agregación puras que operan sobre los datos ya extraídos,
     manteniendo la lógica de negocio fuera de las consultas SQL.
  5. Gestión del ciclo de vida de reports_database: creación idempotente de la
     tabla, inserción de nuevos reportes y limpieza automática de históricos
     según la política de retención de 3 meses (~13 reportes por tienda).

─── Estructura de prediction_database (campos consultados) ───────────────────
  store_id, barcode, product_name, category, image_url,
  objetive_date, prediction, feature, type, percentage_average_deviation

  Los campos product_name, category e image_url están desnormalizados en la
  tabla de predicciones (decisión de diseño del worker_ml), por lo que este
  microservicio NO necesita hacer JOINs con product_database.

─── Estructura de reports_database (gestionada por este módulo) ──────────────
  report_id    BIGSERIAL PK  — Identificador único autoincremental.
  store_id     INTEGER FK    — Referencia a stores_database.store_id.
  created_at   TIMESTAMPTZ   — Marca temporal UTC de generación (criterio de
                               ordenación; no se usa booleano de "activo").
  period_from  DATE          — Primer día del horizonte de predicción cubierto.
  period_to    DATE          — Último día del horizonte de predicción cubierto.
  pdf_content  BYTEA         — Contenido binario del PDF (~150-300 KB/reporte).
  file_size_kb INTEGER       — Tamaño en KB (desnormalizado para logs/monitoreo).

  Política de retención: se conservan los 13 reportes más recientes por tienda
  (≈ 3 meses de generación semanal). El report más reciente se obtiene siempre
  con ORDER BY created_at DESC LIMIT 1, sin flags booleanos.

  Estimación de almacenamiento por tienda:
    13 reportes × 250 KB promedio = ~3.25 MB/tienda
    Muy por debajo del límite acordado de ~7 MB/tienda.
"""

from __future__ import annotations

import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from utils.logger import get_logger
from config.settings import get_settings

logger = get_logger(__name__)
_settings = get_settings()

# ── Motor de base de datos ──────────────────────────────────────────────────

# NullPool: Neon es serverless. Conexiones persistentes se cobran y pueden
# agotarse. NullPool abre y cierra una conexión por operación, perfecta para
# este worker que ejecuta un conjunto finito de consultas y termina.
_engine = create_engine(
    _settings.database_url.unicode_string(),
    poolclass=NullPool,
    echo=False,
)
_LocalSession = sessionmaker(bind=_engine, autocommit=False, autoflush=False)

# ── Política de retención de reportes ────────────────────────────────────────

# Número máximo de reportes a conservar por tienda en reports_database.
#
# Cálculo: 3 meses × 4.33 semanas/mes = 13 reportes.
# Se usa 13 (no 12) para garantizar que el trimestre completo quede cubierto
# incluso en meses de 5 semanas, evitando que el reporte más antiguo del
# trimestre se elimine prematuramente.
#
# Estimación de almacenamiento resultante:
#   13 reportes × ~250 KB = ~3.25 MB/tienda  →  dentro del límite de 7 MB.
#
# Este valor es consultado exclusivamente por save_report(). Si en el futuro
# se desea ampliar o reducir la retención, este es el único punto de cambio.
_KEEP_REPORTS_PER_STORE: int = 13


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager de sesión SQLAlchemy con commit/rollback/close automático.

    Uso:
        with get_session() as session:
            result = session.execute(query, params)
    """
    session: Session = _LocalSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Estructuras de datos tipadas ─────────────────────────────────────────────

@dataclass(frozen=True)
class StoreRecord:
    """Representa una tienda registrada en stores_database."""
    store_id: int
    owner_name: str
    email: str
    city: str


@dataclass(frozen=True)
class PredictionRow:
    """
    Representa una fila de prediction_database.

    Cada fila es la predicción de ventas de UN producto para UNA fecha futura
    en UNA tienda. Una tienda con 30 productos y 7 días de horizonte tendrá
    hasta 210 filas.
    """
    barcode: str
    product_name: str
    category: str
    objetive_date: date
    prediction: int                       # Unidades enteras predichas
    feature: bool                         # True = producto destacado (RF-05)
    prediction_type: str                  # "superavit" | "deficit" | "none"
    percentage_average_deviation: float   # Variación % vs promedio mensual


@dataclass
class WeeklyStats:
    """
    Métricas agregadas del período de predicción para una tienda.

    Se construye a partir de la lista de PredictionRow con compute_weekly_stats()
    y se pasa directamente al renderizador para poblar los KPI cards del reporte.

    Distinción importante entre *_rows y *_products:
      · deficit_rows          — Número total de filas de predicción con tipo "deficit"
                                (puede incluir múltiples días del mismo producto).
      · deficit_products      — Número de productos ÚNICOS clasificados como "deficit"
                                (lo que el tendero necesita ver en el KPI card).
      La misma distinción aplica para superavit_rows / superavit_products.
    """
    total_prediction_rows: int
    deficit_rows: int
    superavit_rows: int
    neutral_rows: int
    featured_rows: int
    unique_products: int
    unique_featured_products: int
    # Conteos de productos únicos por tipo (para KPI cards del reporte)
    deficit_products: int
    superavit_products: int
    neutral_products: int
    date_range_start: date
    date_range_end: date
    # Filas representativas (peor caso por producto), ordenadas por |desviación|
    summary_rows: list[PredictionRow] = field(default_factory=list)
    # Alertas prioritarias: feature=True, ordenadas por |desviación| DESC
    featured_rows_list: list[PredictionRow] = field(default_factory=list)


@dataclass(frozen=True)
class CategoryStats:
    """Resumen de predicciones agrupadas por categoría de producto."""
    category: str
    total_products: int
    deficit_count: int
    superavit_count: int
    neutral_count: int
    featured_count: int


# ── Dataclass para metadatos de reporte almacenado ──────────────────────────

@dataclass(frozen=True)
class ReportRecord:
    """
    Representa los metadatos de un reporte semanal almacenado en reports_database.

    El campo pdf_content se omite intencionalmente: esta estructura se usa
    para logging y auditoría, no para transportar el binario en memoria.
    El binario viaja directamente como `bytes` desde renderer.py hasta
    save_report(), sin pasar por esta clase para evitar copias innecesarias.
    """
    report_id:   int
    store_id:    int
    created_at:  datetime
    period_from: date
    period_to:   date
    file_size_kb: int


# ── Consultas a la base de datos ─────────────────────────────────────────────


def verify_connection() -> bool:
    """
    Verifica la conectividad a Neon con un ping mínimo.

    Se llama al inicio de main() para hacer fail-fast antes de intentar
    consultas reales, evitando errores silenciosos a mitad del pipeline.
    """
    try:
        with get_session() as session:
            session.execute(text("SELECT 1"))
        logger.info("✅ Conexión a Neon verificada correctamente.")
        return True
    except Exception as exc:
        logger.error(f"❌ Fallo en la conexión a Neon: {exc}")
        return False


def get_all_active_stores() -> list[StoreRecord]:
    """
    Obtiene todas las tiendas registradas con email válido.

    Solo se seleccionan las columnas necesarias para el reporte. La presencia
    de un email válido es condición necesaria para el envío; tiendas sin
    dirección de correo son omitidas con un warning.

    Returns:
        Lista ordenada por store_id de StoreRecords, posiblemente vacía.
    """
    query = text("""
        SELECT store_id, owner_name, email, city
        FROM stores_database
        WHERE email IS NOT NULL
          AND TRIM(email) <> ''
          AND email LIKE '%@%'
        ORDER BY store_id ASC
    """)

    records: list[StoreRecord] = []
    with get_session() as session:
        result = session.execute(query)
        rows = result.fetchall()

    for row in rows:
        records.append(StoreRecord(
            store_id=int(row[0]),
            owner_name=str(row[1]),
            email=str(row[2]).strip(),
            city=str(row[3]),
        ))

    logger.info(f"📋 Tiendas activas con email registrado: {len(records)}")
    return records


def get_upcoming_predictions(store_id: int, days: int = 7) -> list[PredictionRow]:
    """
    Obtiene todas las predicciones de los próximos N días para una tienda.

    La cláusula objetive_date >= CURRENT_DATE garantiza que solo se incluyen
    predicciones futuras, no datos históricos residuales. La ordenación
    prioriza productos destacados y luego por magnitud de desviación,
    lo que facilita la construcción de la sección de alertas prioritarias.

    Args:
        store_id: Identificador único de la tienda.
        days:     Horizonte temporal en días (generalmente 7).

    Returns:
        Lista de PredictionRow ordenada por feature DESC, |desviación| DESC.
        Puede ser vacía si el worker_ml no generó predicciones aún.
    """
    today = date.today()
    end_date = today + timedelta(days=days)

    query = text("""
        SELECT
            barcode,
            COALESCE(product_name, 'Producto Desconocido') AS product_name,
            COALESCE(category, 'Sin categoría')            AS category,
            objetive_date,
            prediction,
            feature,
            type,
            percentage_average_deviation
        FROM prediction_database
        WHERE store_id   = :store_id
          AND objetive_date >= :today
          AND objetive_date <= :end_date
        ORDER BY
            feature DESC,
            ABS(percentage_average_deviation) DESC,
            objetive_date ASC
    """)

    predictions: list[PredictionRow] = []
    with get_session() as session:
        result = session.execute(query, {
            "store_id": store_id,
            "today": today,
            "end_date": end_date,
        })
        rows = result.fetchall()

    for row in rows:
        predictions.append(PredictionRow(
            barcode=str(row[0]),
            product_name=str(row[1]),
            category=str(row[2]),
            objetive_date=row[3],
            prediction=int(row[4]),
            feature=bool(row[5]),
            prediction_type=str(row[6]),
            percentage_average_deviation=float(row[7] or 0.0),
        ))

    logger.debug(
        f"  Tienda {store_id}: {len(predictions)} filas de predicción "
        f"(próximos {days} días, desde {today} hasta {end_date})"
    )
    return predictions


# ── Funciones de agregación puras ────────────────────────────────────────────

def _get_summary_rows(predictions: list[PredictionRow]) -> list[PredictionRow]:
    """
    Reduce la lista de predicciones (múltiples días por producto) a una fila
    representativa por producto: aquella con la mayor desviación absoluta
    respecto al promedio histórico.

    Esta es la fila "peor caso de la semana" para cada producto, que
    representa el riesgo máximo que el tendero debe considerar al hacer
    su pedido semanal.

    Args:
        predictions: Todas las filas de predicción del período.

    Returns:
        Lista de PredictionRow, una por barcode único, ordenada por
        magnitud de desviación descendente.
    """
    worst_by_barcode: dict[str, PredictionRow] = {}

    for row in predictions:
        existing = worst_by_barcode.get(row.barcode)
        if existing is None or abs(row.percentage_average_deviation) > abs(existing.percentage_average_deviation):
            worst_by_barcode[row.barcode] = row

    return sorted(
        worst_by_barcode.values(),
        key=lambda r: abs(r.percentage_average_deviation),
        reverse=True,
    )


def compute_weekly_stats(
    predictions: list[PredictionRow],
    max_featured: int = 10,
) -> WeeklyStats:
    """
    Construye el objeto WeeklyStats a partir de la lista completa de predicciones.

    Agrupa y cuenta por tipo de clasificación, extrae los productos destacados
    (feature=True) y genera las filas de resumen para la tabla principal del reporte.

    Args:
        predictions:  Lista completa de PredictionRow para la tienda.
        max_featured: Máximo de alertas prioritarias a incluir en el reporte.

    Returns:
        WeeklyStats completamente poblado, listo para el renderizador.
    """
    if not predictions:
        today = date.today()
        return WeeklyStats(
            total_prediction_rows=0,
            deficit_rows=0,
            superavit_rows=0,
            neutral_rows=0,
            featured_rows=0,
            unique_products=0,
            unique_featured_products=0,
            deficit_products=0,
            superavit_products=0,
            neutral_products=0,
            date_range_start=today,
            date_range_end=today + timedelta(days=7),
            summary_rows=[],
            featured_rows_list=[],
        )

    deficit_rows = sum(1 for r in predictions if r.prediction_type == "deficit")
    superavit_rows = sum(1 for r in predictions if r.prediction_type == "superavit")
    neutral_rows = sum(1 for r in predictions if r.prediction_type == "none")
    featured_rows = sum(1 for r in predictions if r.feature)

    unique_barcodes = {r.barcode for r in predictions}
    featured_barcodes = {r.barcode for r in predictions if r.feature}

    # Conteos de productos únicos por tipo: se usa la clasificación del "peor
    # caso" de cada producto (mayor desviación absoluta de la semana).
    # Esto garantiza que cada barcode se cuenta exactamente una vez, y que
    # el tipo asignado es el más crítico observado en el horizonte semanal.
    summary_rows = _get_summary_rows(predictions)
    deficit_products = sum(1 for r in summary_rows if r.prediction_type == "deficit")
    superavit_products = sum(1 for r in summary_rows if r.prediction_type == "superavit")
    neutral_products = sum(1 for r in summary_rows if r.prediction_type == "none")

    dates = [r.objetive_date for r in predictions]

    # Alertas prioritarias: feature=True, ordenadas por |desviación| DESC
    featured_list: list[PredictionRow] = sorted(
        [r for r in predictions if r.feature],
        key=lambda r: abs(r.percentage_average_deviation),
        reverse=True,
    )
    # Deduplicar por barcode (conservar el peor caso) y limitar a max_featured
    seen: set[str] = set()
    featured_deduped: list[PredictionRow] = []
    for row in featured_list:
        if row.barcode not in seen:
            seen.add(row.barcode)
            featured_deduped.append(row)
        if len(featured_deduped) >= max_featured:
            break

    return WeeklyStats(
        total_prediction_rows=len(predictions),
        deficit_rows=deficit_rows,
        superavit_rows=superavit_rows,
        neutral_rows=neutral_rows,
        featured_rows=featured_rows,
        unique_products=len(unique_barcodes),
        unique_featured_products=len(featured_barcodes),
        deficit_products=deficit_products,
        superavit_products=superavit_products,
        neutral_products=neutral_products,
        date_range_start=min(dates),
        date_range_end=max(dates),
        summary_rows=summary_rows,
        featured_rows_list=featured_deduped,
    )


def compute_category_breakdown(predictions: list[PredictionRow]) -> list[CategoryStats]:
    """
    Genera un resumen de predicciones agrupadas por categoría de producto.

    Para evitar contar el mismo producto múltiples veces (una por día), se
    usa una representación de "peor caso por barcode" antes de agregar.
    Esto garantiza que cada producto cuenta exactamente una vez por categoría.

    Args:
        predictions: Lista completa de PredictionRow para la tienda.

    Returns:
        Lista de CategoryStats ordenada por total de productos descendente.
    """
    # Reducir a una fila por barcode (peor caso) antes de agrupar
    summary_rows = _get_summary_rows(predictions)

    categories: dict[str, dict[str, int]] = {}
    for row in summary_rows:
        cat = row.category
        if cat not in categories:
            categories[cat] = {
                "total": 0, "deficit": 0, "superavit": 0,
                "neutral": 0, "featured": 0,
            }
        categories[cat]["total"] += 1
        if row.prediction_type == "deficit":
            categories[cat]["deficit"] += 1
        elif row.prediction_type == "superavit":
            categories[cat]["superavit"] += 1
        else:
            categories[cat]["neutral"] += 1
        if row.feature:
            categories[cat]["featured"] += 1

    result = [
        CategoryStats(
            category=cat,
            total_products=data["total"],
            deficit_count=data["deficit"],
            superavit_count=data["superavit"],
            neutral_count=data["neutral"],
            featured_count=data["featured"],
        )
        for cat, data in categories.items()
    ]

    return sorted(result, key=lambda c: c.total_products, reverse=True)


# ── Gestión del ciclo de vida de reports_database ────────────────────────────

def ensure_reports_table_exists() -> None:
    """
    Crea la tabla reports_database en Neon si aún no existe.

    Esta función implementa el patrón DDL idempotente: puede llamarse en cada
    ejecución del worker sin efectos secundarios si la tabla ya existe.
    Se invoca una única vez al inicio de main(), inmediatamente después de
    verify_connection(), garantizando que cualquier ejecución del worker —
    incluida la primera — encuentra la tabla lista antes de intentar escribir.

    Decisiones de diseño de la tabla:
      · BIGSERIAL en report_id: anticipación de volumen a largo plazo.
      · TIMESTAMPTZ en created_at: almacena zona horaria (UTC en producción).
        Es el único criterio de ordenación para determinar el reporte más
        reciente. No se usa ningún flag booleano "is_latest".
      · BYTEA en pdf_content: PostgreSQL admite hasta 1 GB por campo BYTEA.
        Los PDFs de este sistema (~150-300 KB) son órdenes de magnitud
        inferiores a ese límite.
      · file_size_kb INTEGER: campo desnormalizado para facilitar el monitoreo
        del uso de almacenamiento sin necesidad de leer el BYTEA completo.
      · FK store_id → stores_database: garantiza integridad referencial.
        ON DELETE CASCADE: si una tienda es eliminada, sus reportes se borran
        automáticamente sin dejar huérfanos.
      · Índice (store_id, created_at DESC): optimiza la consulta más frecuente
        del endpoint de la API — "dame el reporte más reciente de la tienda X".
        La dirección DESC en el índice evita un sort explícito en el plan de
        ejecución de PostgreSQL.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: si la conexión falla o Neon devuelve
        un error DDL inesperado. El llamador (main.py) lo captura y aborta.
    """
    ddl_table = text("""
        CREATE TABLE IF NOT EXISTS reports_database (
            report_id    BIGSERIAL    PRIMARY KEY,
            store_id     INTEGER      NOT NULL
                             REFERENCES stores_database(store_id)
                             ON DELETE CASCADE,
            created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            period_from  DATE         NOT NULL,
            period_to    DATE         NOT NULL,
            pdf_content  BYTEA        NOT NULL,
            file_size_kb INTEGER      NOT NULL
        )
    """)

    # Índice compuesto para la consulta "último reporte por tienda".
    # CREATE INDEX IF NOT EXISTS es idempotente desde PostgreSQL 9.5.
    # La dirección DESC en created_at garantiza que el plan de ejecución
    # de la query ORDER BY created_at DESC LIMIT 1 use el índice directamente
    # sin un paso de sort adicional.
    ddl_index = text("""
        CREATE INDEX IF NOT EXISTS ix_reports_store_created
            ON reports_database (store_id, created_at DESC)
    """)

    with get_session() as session:
        session.execute(ddl_table)
        session.execute(ddl_index)

    logger.info("✅ Tabla reports_database verificada/creada en Neon.")


def save_report(
    store_id:    int,
    pdf_bytes:   bytes,
    period_from: date,
    period_to:   date,
) -> ReportRecord:
    """
    Persiste un nuevo reporte PDF en reports_database y aplica la política
    de retención eliminando los reportes más antiguos de esa tienda.

    Ambas operaciones — INSERT del nuevo reporte y DELETE de los históricos
    fuera de la ventana de retención — se ejecutan dentro de una única
    transacción ACID. Si cualquiera de las dos falla, el rollback automático
    del context manager get_session() garantiza que Neon no quede en un estado
    parcialmente modificado: o se persiste el reporte completo y se limpian
    los antiguos, o no ocurre ningún cambio.

    Política de retención (DELETE):
      Se conservan los `_KEEP_REPORTS_PER_STORE` reportes más recientes
      por tienda (13 = ~3 meses de generación semanal). Los reportes fuera
      de esa ventana se eliminan usando una subconsulta que identifica los
      report_id a borrar mediante ORDER BY + OFFSET, que es la forma más
      segura y portable de implementar "borrar todo excepto los N más
      recientes" en PostgreSQL sin cursores ni CTEs adicionales.

    Args:
        store_id:    Identificador de la tienda propietaria del reporte.
        pdf_bytes:   Contenido binario del PDF generado por renderer.py.
                     Se almacena directamente en el campo BYTEA sin
                     transformaciones intermedias.
        period_from: Primer día del horizonte de predicción cubierto.
        period_to:   Último día del horizonte de predicción cubierto.

    Returns:
        ReportRecord con los metadatos del reporte recién insertado,
        incluyendo el report_id asignado por la secuencia BIGSERIAL de Neon.
        El campo pdf_content NO se incluye en el dataclass retornado para
        evitar mantener el binario en memoria más allá de lo necesario.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: propagado al llamador si la
        transacción falla. main.py lo captura por tienda sin abortar el
        pipeline completo.
    """
    file_size_kb = max(1, len(pdf_bytes) // 1024)
    now_utc = datetime.now(timezone.utc)

    # ── INSERT: nuevo reporte ─────────────────────────────────────────────────
    # RETURNING report_id permite recuperar el ID asignado por BIGSERIAL
    # en la misma sentencia, sin necesidad de un SELECT adicional posterior.
    insert_query = text("""
        INSERT INTO reports_database
            (store_id, created_at, period_from, period_to, pdf_content, file_size_kb)
        VALUES
            (:store_id, :created_at, :period_from, :period_to, :pdf_content, :file_size_kb)
        RETURNING report_id
    """)

    # ── DELETE: política de retención ─────────────────────────────────────────
    # Subconsulta:
    #   1. Selecciona los report_id de esta tienda, ordenados del más reciente
    #      al más antiguo.
    #   2. Salta los primeros _KEEP_REPORTS_PER_STORE (OFFSET), que son los
    #      que queremos conservar.
    #   3. Los restantes (si los hay) son candidatos a eliminación.
    #
    # El DELETE solo se ejecuta si existen más de _KEEP_REPORTS_PER_STORE
    # reportes, lo que es el caso normal después de ~3 meses de operación.
    # Las primeras 13 semanas el DELETE no elimina nada (subconsulta vacía).
    cleanup_query = text("""
        DELETE FROM reports_database
        WHERE report_id IN (
            SELECT report_id
            FROM   reports_database
            WHERE  store_id = :store_id
            ORDER BY created_at DESC
            OFFSET :keep_count
        )
    """)

    with get_session() as session:
        # Paso 1: INSERT
        result = session.execute(insert_query, {
            "store_id":    store_id,
            "created_at":  now_utc,
            "period_from": period_from,
            "period_to":   period_to,
            "pdf_content": pdf_bytes,
            "file_size_kb": file_size_kb,
        })
        new_report_id: int = result.scalar_one()

        # Paso 2: DELETE de históricos fuera de la ventana de retención.
        # Se ejecuta en la misma sesión y transacción que el INSERT, garantizando
        # que el nuevo reporte ya cuenta como parte de los _KEEP_REPORTS_PER_STORE
        # antes de que se evalúe el OFFSET de la subconsulta de limpieza.
        delete_result = session.execute(cleanup_query, {
            "store_id":   store_id,
            "keep_count": _KEEP_REPORTS_PER_STORE,
        })
        deleted_count: int = delete_result.rowcount

    # ── Log detallado ─────────────────────────────────────────────────────────
    period_label = (
        f"{period_from.strftime('%d/%m/%Y')} → {period_to.strftime('%d/%m/%Y')}"
    )
    logger.info(
        f"  💾 Reporte persistido en Neon — "
        f"report_id={new_report_id} | "
        f"tienda={store_id} | "
        f"período={period_label} | "
        f"tamaño={file_size_kb} KB"
    )
    if deleted_count > 0:
        logger.info(
            f"  🗑   Limpieza de retención — "
            f"{deleted_count} reporte(s) antiguo(s) eliminado(s) "
            f"para tienda {store_id} "
            f"(política: conservar últimos {_KEEP_REPORTS_PER_STORE})"
        )

    return ReportRecord(
        report_id=new_report_id,
        store_id=store_id,
        created_at=now_utc,
        period_from=period_from,
        period_to=period_to,
        file_size_kb=file_size_kb,
    )