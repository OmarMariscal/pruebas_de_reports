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

─── Estructura de prediction_database (campos consultados) ───────────────────
  store_id, barcode, product_name, category, image_url,
  objective_date, prediction, feature, type, percentage_average_deviation

  Los campos product_name, category e image_url están desnormalizados en la
  tabla de predicciones (decisión de diseño del worker_ml), por lo que este
  microservicio NO necesita hacer JOINs con product_database.
"""

from __future__ import annotations

import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from logger import get_logger
from settings import get_settings

logger = get_logger(__name__)
_settings = get_settings()

# Motor de base de datos 

# NullPool: Neon es serverless. Conexiones persistentes se cobran y pueden
# agotarse. NullPool abre y cierra una conexión por operación, perfecta para
# este worker que ejecuta un conjunto finito de consultas y termina.
_engine = create_engine(
    _settings.database_url.unicode_string(),
    poolclass=NullPool,
    echo=False,
)
_LocalSession = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


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


# Estructuras de datos tipadas 

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
    objective_date: date
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


# Consultas a la base de datos

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

    La cláusula objective_date >= CURRENT_DATE garantiza que solo se incluyen
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
            objective_date,
            prediction,
            feature,
            type,
            percentage_average_deviation
        FROM prediction_database
        WHERE store_id   = :store_id
          AND objective_date >= :today
          AND objective_date <= :end_date
        ORDER BY
            feature DESC,
            ABS(percentage_average_deviation) DESC,
            objective_date ASC
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
            objective_date=row[3],
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


# Funciones de agregación puras

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

    dates = [r.objective_date for r in predictions]

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
