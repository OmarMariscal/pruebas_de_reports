"""
Logger estructurado para el Worker Reports — BanAnalytics.

Sigue exactamente el mismo patrón establecido en worker_ml/utils/logger.py
para mantener consistencia de formato en los logs de GitHub Actions, donde
todos los runs son monitoreados en la pestaña de Actions del repositorio.

El formato incluye timestamp UTC, nivel de severidad, nombre del módulo
y el mensaje, lo que facilita el debug post-mortem cuando el worker
corre de forma autónoma a las 5:00 AM sin supervisión humana.
"""

from __future__ import annotations

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Retorna un logger configurado y vinculado al módulo solicitado.

    Es idempotente: múltiples llamadas con el mismo nombre devuelven el mismo
    logger sin acumular handlers duplicados, lo que evita líneas repetidas en
    los logs de GitHub Actions cuando distintos módulos se importan en cadena.

    Args:
        name: Nombre del módulo, convencionalmente pasado como ``__name__``.

    Returns:
        logging.Logger configurado con output a stdout y nivel DEBUG.
    """
    logger = logging.getLogger(name)

    # Idempotencia: si ya está configurado, devolverlo directamente
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-35s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    # Deshabilitar propagación para evitar duplicación con el logger raíz
    logger.propagate = False

    return logger
