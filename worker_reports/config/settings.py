"""
Configuración central del Worker Reports — BanAnalytics.

Lee variables de entorno (o archivo .env local para desarrollo) con validación
estricta de tipos en el arranque, garantizando un fallo rápido y explícito antes
de intentar conectar a la base de datos o al servidor SMTP.

Variables requeridas (deben existir en GitHub Actions Secrets):
  - DATABASE_URL   : Cadena de conexión PostgreSQL (Neon).
  - SMTP_HOST      : Hostname del servidor de correo saliente.
  - SMTP_USER      : Usuario/dirección de autenticación SMTP.
  - SMTP_PASSWORD  : Contraseña de autenticación SMTP.

Variables opcionales (pueden definirse como GitHub Actions Variables o Secrets):
  - SMTP_PORT      : Puerto SMTP (587 para STARTTLS, 465 para SSL). Default: 587
  - SMTP_FROM      : Dirección "De:" del correo. Default: reportes@bananalytics.com
  - SMTP_FROM_NAME : Nombre visible del remitente. Default: BanAnalytics Reportes
  - SMTP_USE_TLS   : Usar STARTTLS en puerto 587. Default: true
  - REPORT_DAYS    : Días de predicción futura a incluir en el reporte. Default: 7
  - MAX_FEATURED   : Máx. de alertas prioritarias mostradas en el reporte. Default: 10
  - DRY_RUN        : true/false — Si true, omite el envío SMTP. Default: false

Comportamiento en modo DRY_RUN:
  Cuando DRY_RUN=true, las credenciales SMTP (smtp_host, smtp_user, smtp_password)
  pueden estar vacías o ausentes: la validación de esos campos se omite porque
  el módulo mailer nunca es invocado. DATABASE_URL sigue siendo requerida siempre,
  ya que las consultas a Neon se realizan independientemente del modo.

  Fundamento técnico del problema que esto resuelve:
  GitHub Actions expande secrets no configurados como strings vacíos (""). Pydantic
  intenta convertir "" a int para smtp_port, fallando con ValidationError antes de
  llegar a main(). El pre-validador `_coerce_smtp_port` intercepta el string vacío
  y lo reemplaza con el default (587) antes de la conversión de tipo.
"""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import PostgresDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuración validada para el microservicio de reportes.

    Todos los campos son leídos directamente desde variables de entorno.
    La validación ocurre al construir el objeto, permitiendo un fallo rápido
    antes de iniciar el pipeline — excepto los campos SMTP en modo DRY_RUN,
    donde se omiten intencionalmente para permitir pruebas sin credenciales.
    """

    # Base de datos (siempre requerida)
    database_url: PostgresDsn

    # SMTP (requeridos solo cuando DRY_RUN=false)
    # Usa str con default "" para que Pydantic no rechace strings vacíos
    # provenientes de secrets no configurados en GitHub Actions.
    # La validación de presencia se realiza en el model_validator `check_smtp`.
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "monkeycodeinc@gmail.com"
    smtp_from_name: str = "BanAnalytics Reportes"
    smtp_use_tls: bool = True

    # Parámetros del reporte
    report_days: int = 7       # Días de predicciones futuras a incluir
    max_featured: int = 10     # Máx. filas en la sección "Alertas Prioritarias"

    # Lectura de configuración
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",         # Ignora variables de entorno no declaradas aquí
    )

    # Pre-validadores (mode="before"): operan sobre el valor raw del env

    @field_validator("smtp_port", mode="before")
    @classmethod
    def _coerce_smtp_port(cls, v: object) -> object:
        """
        Intercepta el string vacío que GitHub Actions pasa cuando el secret
        SMTP_PORT no está configurado, sustituyéndolo por el default 587
        antes de que Pydantic intente convertirlo a int.

        Sin este validador, "" → int("") lanza ValueError y el worker aborta
        antes de llegar a main(), incluso en modo DRY_RUN donde SMTP no se usa.
        """
        if isinstance(v, str) and v.strip() == "":
            return 587
        return v

    # Validadores de campo

    @field_validator("smtp_port")
    @classmethod
    def _validate_smtp_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError(
                f"smtp_port debe estar entre 1 y 65535. Recibido: {v}"
            )
        return v

    @field_validator("report_days")
    @classmethod
    def _validate_report_days(cls, v: int) -> int:
        if not 1 <= v <= 31:
            raise ValueError(
                f"report_days debe estar entre 1 y 31. Recibido: {v}"
            )
        return v

    @field_validator("max_featured")
    @classmethod
    def _validate_max_featured(cls, v: int) -> int:
        if not 1 <= v <= 50:
            raise ValueError(
                f"max_featured debe estar entre 1 y 50. Recibido: {v}"
            )
        return v

    @field_validator("smtp_from")
    @classmethod
    def _validate_smtp_from(cls, v: str) -> str:
        # Solo valida el formato si no está vacío; la presencia real
        # se verifica en el model_validator `check_smtp` más abajo.
        if v and "@" not in v:
            raise ValueError(
                f"smtp_from debe ser una dirección de correo válida. Recibido: '{v}'"
            )
        return v

    # Validador de modelo (mode="after"): opera sobre el objeto completo

    @model_validator(mode="after")
    def _check_smtp_when_not_dry_run(self) -> "Settings":
        """
        Valida que las credenciales SMTP estén presentes cuando DRY_RUN=false.

        Este validador se ejecuta después de que todos los campos individuales
        han sido validados. Lee DRY_RUN directamente desde os.environ para
        decidir si la validación SMTP es obligatoria.

        Lógica:
          · DRY_RUN=true  → SMTP no se usará; credenciales opcionales.
          · DRY_RUN=false → SMTP se invocará; smtp_host/user/password requeridos.

        Raises:
            ValueError: Si alguna credencial SMTP está vacía en modo no-dry-run.
        """
        dry_run_raw = os.environ.get("DRY_RUN", "false").strip().lower()
        is_dry_run = dry_run_raw in {"true", "1", "yes"}

        if not is_dry_run:
            missing = []
            if not self.smtp_host.strip():
                missing.append("SMTP_HOST")
            if not self.smtp_user.strip():
                missing.append("SMTP_USER")
            if not self.smtp_password.strip():
                missing.append("SMTP_PASSWORD")
            if not self.smtp_from.strip():
                missing.append("SMTP_FROM")

            if missing:
                raise ValueError(
                    f"Las siguientes variables SMTP son requeridas cuando DRY_RUN=false "
                    f"pero están vacías o no configuradas: {', '.join(missing)}. "
                    "Verifique los Secrets y Variables en la configuración del repositorio "
                    "de GitHub Actions."
                )

        return self


@lru_cache
def get_settings() -> Settings:
    """
    Singleton de configuración cacheado.

    La caché de lru_cache garantiza que Settings() se construye una única vez
    por ejecución, evitando re-parseos innecesarios y lecturas repetidas del
    sistema de archivos. El fallo en el primer acceso es intencional: si la
    configuración es inválida, el worker debe abortar inmediatamente.
    """
    return Settings()
