"""
Módulo de envío de correo electrónico — BanAnalytics Worker Reports.

Responsabilidades:
  1. Construir un mensaje MIME multipart con:
       · Parte text/plain: resumen legible para clientes sin soporte HTML.
       · Parte text/html:  versión enriquecida del cuerpo del correo.
       · Adjunto application/pdf: el reporte semanal generado por renderer.py.
  2. Establecer conexión SMTP con soporte STARTTLS (puerto 587) o SSL (puerto 465).
  3. Autenticarse y enviar el mensaje.
  4. Cerrar la conexión limpiamente, incluso ante excepciones.

Decisiones de diseño:
  - `smtplib` de la biblioteca estándar: cero dependencias adicionales para
    la capa de transporte. Es suficientemente robusto para el volumen de este
    worker (≤ 500 tiendas por ejecución semanal).
  - Separación entre construcción del mensaje (_build_message) y envío
    (_send_message): facilita el testing unitario del cuerpo del correo sin
    necesitar un servidor SMTP real.
  - Cuerpo HTML del correo en colores de marca, congruente con el PDF adjunto.
  - Timeout de 30 segundos por conexión: evita bloqueos indefinidos si el
    servidor SMTP no responde, garantizando que el pipeline siga procesando
    otras tiendas.

Referencia ERS:
  RF-08 §3 — "Si el envío de correo falla, el sistema debe encolar el envío
  para más tarde y mostrar una notificación de escritorio como respaldo."
  En el contexto de este microservicio en la nube, el mecanismo equivalente
  es: log del error, retorno de False, y el orquestador (main.py) registra
  la tienda fallida en el resumen final del run sin abortar las demás.
"""

from __future__ import annotations

import smtplib
import ssl
from datetime import date
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate

from worker_reports.db_queries import StoreRecord, WeeklyStats
from worker_reports.logger import get_logger
from worker_reports.settings import get_settings

logger = get_logger(__name__)
_settings = get_settings()

# Constante de timeout de conexión SMTP

_SMTP_TIMEOUT_SECONDS: int = 30


# Construcción del mensaje MIME

def _build_plain_text_body(
    store: StoreRecord,
    stats: WeeklyStats,
    generated_at: str,
) -> str:
    """
    Genera el cuerpo en texto plano del correo.

    Es el fallback para clientes de correo sin soporte HTML.
    Contiene la información más crítica: conteo de alertas y
    período cubierto por el reporte.

    Args:
        store:        Datos de la tienda destinataria.
        stats:        Métricas agregadas del período.
        generated_at: Timestamp formateado de generación.

    Returns:
        String con el cuerpo del correo en formato texto plano.
    """
    period_start = stats.date_range_start.strftime("%d/%m/%Y")
    period_end = stats.date_range_end.strftime("%d/%m/%Y")

    lines = [
        f"Hola {store.owner_name},",
        "",
        "Le enviamos su Reporte Semanal Predictivo de BanAnalytics.",
        "",
        f"Período: {period_start} al {period_end}",
        f"Generado: {generated_at}",
        "",
        "── RESUMEN DE PREDICCIONES ──────────────────",
        f"  Productos analizados  : {stats.unique_products}",
        f"  Alertas prioritarias  : {stats.unique_featured_products}",
        f"  Productos en Déficit  : {stats.deficit_products}",
        f"  Productos en Superávit: {stats.superavit_products}",
        f"  Productos en Normal   : {stats.neutral_products}",
        "",
    ]

    if stats.featured_rows_list:
        lines.append("── TOP ALERTAS PRIORITARIAS ─────────────────")
        for i, row in enumerate(stats.featured_rows_list[:5], 1):
            sign = "+" if row.percentage_average_deviation > 0 else ""
            dev = f"{sign}{row.percentage_average_deviation:.1f}%"
            lines.append(
                f"  {i}. {row.product_name} "
                f"[{row.prediction_type.upper()}] "
                f"Desv: {dev} — "
                f"Pred: {row.prediction} unid."
            )
        lines.append("")

    lines += [
        "El reporte completo se encuentra adjunto en formato PDF.",
        "",
        "──────────────────────────────────────────────",
        "BanAnalytics · Desarrollado por MonkeyCode",
        "Universidad de Guadalajara · Ingeniería de Software",
        "Soporte: support@bananalytics.com",
        "",
        "Este correo fue generado automáticamente. Por favor no responda.",
    ]

    return "\n".join(lines)


def _build_html_body(
    store: StoreRecord,
    stats: WeeklyStats,
    generated_at: str,
) -> str:
    """
    Genera el cuerpo HTML del correo con los colores de marca BanAnalytics.

    El HTML del cuerpo del correo es deliberadamente más simple que el PDF,
    ya que los clientes de correo tienen soporte CSS muy limitado.
    Se usan estilos inline exclusivamente para garantizar compatibilidad
    con Gmail, Outlook y Apple Mail.

    Args:
        store:        Datos de la tienda destinataria.
        stats:        Métricas agregadas del período.
        generated_at: Timestamp formateado de generación.

    Returns:
        String con el HTML del cuerpo del correo.
    """
    period_start = stats.date_range_start.strftime("%d/%m/%Y")
    period_end = stats.date_range_end.strftime("%d/%m/%Y")

    # Filas de alertas prioritarias
    featured_rows_html = ""
    if stats.featured_rows_list:
        rows_html_parts = []
        for row in stats.featured_rows_list[:5]:
            if row.prediction_type == "deficit":
                badge_bg, badge_color, label = "#FFEBEE", "#C62828", "DÉFICIT"
            elif row.prediction_type == "superavit":
                badge_bg, badge_color, label = "#E8F5E9", "#1B5E20", "SUPERÁVIT"
            else:
                badge_bg, badge_color, label = "#ECEFF1", "#455A64", "NORMAL"

            sign = "+" if row.percentage_average_deviation > 0 else ""
            dev_str = f"{sign}{row.percentage_average_deviation:.1f}%"
            dev_color = (
                "#1B5E20" if row.percentage_average_deviation > 0
                else "#C62828" if row.percentage_average_deviation < 0
                else "#455A64"
            )

            rows_html_parts.append(f"""
            <tr>
                <td style="padding:8px 10px; border-bottom:1px solid #F0E6D6;
                           font-family:Arial,sans-serif; font-size:12px;
                           color:#2D2114; font-weight:bold;">
                    {row.product_name}
                </td>
                <td style="padding:8px 10px; border-bottom:1px solid #F0E6D6;
                           font-family:Arial,sans-serif; font-size:11px;
                           color:#8D7A66; font-style:italic;">
                    {row.category}
                </td>
                <td style="padding:8px 10px; border-bottom:1px solid #F0E6D6;
                           text-align:center;">
                    <span style="background:{badge_bg}; color:{badge_color};
                                 font-family:Arial,sans-serif; font-size:10px;
                                 font-weight:900; padding:3px 8px;
                                 border-radius:10px; white-space:nowrap;">
                        {label}
                    </span>
                </td>
                <td style="padding:8px 10px; border-bottom:1px solid #F0E6D6;
                           text-align:right; font-family:Arial,sans-serif;
                           font-size:12px; font-weight:bold; color:{dev_color};">
                    {dev_str}
                </td>
                <td style="padding:8px 10px; border-bottom:1px solid #F0E6D6;
                           text-align:right; font-family:Arial,sans-serif;
                           font-size:12px; color:#2D2114; font-weight:bold;">
                    {row.prediction}&nbsp;unid.
                </td>
            </tr>""")

        featured_rows_html = f"""
        <table width="100%" cellpadding="0" cellspacing="0"
               style="border-collapse:collapse; margin-top:16px;">
            <thead>
                <tr style="background:#2D2114;">
                    <th style="padding:8px 10px; text-align:left; color:#FFFFFF;
                               font-family:Arial,sans-serif; font-size:10px;
                               font-weight:bold; letter-spacing:1px;
                               text-transform:uppercase;">
                        Producto
                    </th>
                    <th style="padding:8px 10px; text-align:left; color:#FFFFFF;
                               font-family:Arial,sans-serif; font-size:10px;
                               font-weight:bold; letter-spacing:1px;
                               text-transform:uppercase;">
                        Categoría
                    </th>
                    <th style="padding:8px 10px; text-align:center; color:#FFFFFF;
                               font-family:Arial,sans-serif; font-size:10px;
                               font-weight:bold; letter-spacing:1px;
                               text-transform:uppercase;">
                        Estado
                    </th>
                    <th style="padding:8px 10px; text-align:right; color:#FFFFFF;
                               font-family:Arial,sans-serif; font-size:10px;
                               font-weight:bold; letter-spacing:1px;
                               text-transform:uppercase;">
                        Desviación
                    </th>
                    <th style="padding:8px 10px; text-align:right; color:#FFFFFF;
                               font-family:Arial,sans-serif; font-size:10px;
                               font-weight:bold; letter-spacing:1px;
                               text-transform:uppercase;">
                        Predicción
                    </th>
                </tr>
            </thead>
            <tbody>
                {"".join(rows_html_parts)}
            </tbody>
        </table>"""

    else:
        featured_rows_html = """
        <p style="font-family:Arial,sans-serif; font-size:13px; color:#8D7A66;
                  font-style:italic; text-align:center; padding:16px;
                  background:#FDFBF9; border:1px dashed #F0E6D6;
                  border-radius:4px; margin-top:16px;">
            ✅ No se detectaron alertas prioritarias para este período.
        </p>"""

    # HTML completo del correo
    return f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Reporte Semanal BanAnalytics</title>
</head>
<body style="margin:0; padding:0; background:#F5F0E8; font-family:Arial,sans-serif;">

    <!-- Contenedor exterior -->
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#F5F0E8; padding:24px 0;">
        <tr>
            <td align="center">

                <!-- Tarjeta principal -->
                <table width="600" cellpadding="0" cellspacing="0"
                       style="background:#FFFFFF; border-radius:8px;
                              box-shadow:0 2px 12px rgba(0,0,0,0.08);
                              overflow:hidden;">

                    <!-- Encabezado de marca -->
                    <tr>
                        <td style="background:#2D2114; padding:24px 32px;">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td>
                                        <p style="margin:0; font-size:22px;
                                                  font-weight:bold; color:#FFFFFF;
                                                  letter-spacing:-0.3px;">
                                            Ban<span style="color:#C38441;">Analytics</span>
                                        </p>
                                        <p style="margin:4px 0 0 0; font-size:10px;
                                                  font-weight:bold; color:#8D7A66;
                                                  letter-spacing:2px;
                                                  text-transform:uppercase;">
                                            Reporte Predictivo Semanal
                                        </p>
                                    </td>
                                    <td align="right">
                                        <p style="margin:0; font-size:11px;
                                                  color:#8D7A66;">
                                            {period_start} — {period_end}
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Saludo personalizado -->
                    <tr>
                        <td style="padding:28px 32px 0 32px;">
                            <p style="margin:0; font-size:18px; font-weight:bold;
                                      color:#2D2114;">
                                Hola, {store.owner_name} 👋
                            </p>
                            <p style="margin:8px 0 0 0; font-size:13px;
                                      color:#8D7A66; line-height:1.6;">
                                Le presentamos el resumen predictivo semanal de su tienda
                                en <strong style="color:#2D2114;">{store.city}</strong>.
                                El reporte completo se encuentra adjunto en PDF.
                            </p>
                        </td>
                    </tr>

                    <!-- KPI Grid -->
                    <tr>
                        <td style="padding:24px 32px;">
                            <table width="100%" cellpadding="0" cellspacing="0"
                                   style="border-collapse:separate;
                                          border-spacing:8px;">
                                <tr>
                                    <td width="25%" style="background:#FDF3E7;
                                               border-top:3px solid #C38441;
                                               border-radius:6px; padding:14px;
                                               text-align:center;">
                                        <p style="margin:0; font-size:28px;
                                                  font-weight:bold; color:#C38441;">
                                            {stats.unique_products}
                                        </p>
                                        <p style="margin:4px 0 0 0; font-size:9px;
                                                  font-weight:bold; color:#8D7A66;
                                                  text-transform:uppercase;
                                                  letter-spacing:0.8px;">
                                            Productos<br/>analizados
                                        </p>
                                    </td>
                                    <td width="25%" style="background:#FFEBEE;
                                               border-top:3px solid #C62828;
                                               border-radius:6px; padding:14px;
                                               text-align:center;">
                                        <p style="margin:0; font-size:28px;
                                                  font-weight:bold; color:#C62828;">
                                            {stats.deficit_products}
                                        </p>
                                        <p style="margin:4px 0 0 0; font-size:9px;
                                                  font-weight:bold; color:#8D7A66;
                                                  text-transform:uppercase;
                                                  letter-spacing:0.8px;">
                                            Productos<br/>en Déficit
                                        </p>
                                    </td>
                                    <td width="25%" style="background:#E8F5E9;
                                               border-top:3px solid #1B5E20;
                                               border-radius:6px; padding:14px;
                                               text-align:center;">
                                        <p style="margin:0; font-size:28px;
                                                  font-weight:bold; color:#1B5E20;">
                                            {stats.superavit_products}
                                        </p>
                                        <p style="margin:4px 0 0 0; font-size:9px;
                                                  font-weight:bold; color:#8D7A66;
                                                  text-transform:uppercase;
                                                  letter-spacing:0.8px;">
                                            Productos<br/>en Superávit
                                        </p>
                                    </td>
                                    <td width="25%" style="background:#F9F5F0;
                                               border-top:3px solid #2D2114;
                                               border-radius:6px; padding:14px;
                                               text-align:center;">
                                        <p style="margin:0; font-size:28px;
                                                  font-weight:bold; color:#2D2114;">
                                            {stats.unique_featured_products}
                                        </p>
                                        <p style="margin:4px 0 0 0; font-size:9px;
                                                  font-weight:bold; color:#8D7A66;
                                                  text-transform:uppercase;
                                                  letter-spacing:0.8px;">
                                            Alertas<br/>prioritarias
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Top alertas -->
                    <tr>
                        <td style="padding:0 32px 28px 32px;">
                            <p style="margin:0 0 4px 0; font-size:14px;
                                      font-weight:bold; color:#2D2114;">
                                Top Alertas Prioritarias
                            </p>
                            <p style="margin:0; font-size:11px; color:#8D7A66;
                                      font-style:italic;">
                                Productos con mayor desviación respecto al promedio histórico
                            </p>
                            {featured_rows_html}
                        </td>
                    </tr>

                    <!-- CTA: ver PDF -->
                    <tr>
                        <td style="background:#FDF3E7; padding:20px 32px;
                                   border-top:1px solid #F0E6D6;
                                   text-align:center;">
                            <p style="margin:0; font-size:13px; color:#8D7A66;">
                                El reporte completo con todas las predicciones,
                                análisis por categoría y glosario de términos
                                se encuentra en el archivo
                                <strong style="color:#C38441;">PDF adjunto</strong>.
                            </p>
                        </td>
                    </tr>

                    <!-- Pie de correo -->
                    <tr>
                        <td style="background:#2D2114; padding:16px 32px;">
                            <table width="100%" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td>
                                        <p style="margin:0; font-size:11px;
                                                  color:#8D7A66; line-height:1.5;">
                                            <strong style="color:#C38441;">BanAnalytics</strong>
                                            · Desarrollado por MonkeyCode<br/>
                                            Universidad de Guadalajara
                                            · Ingeniería de Software
                                        </p>
                                    </td>
                                    <td align="right">
                                        <p style="margin:0; font-size:10px;
                                                  color:#8D7A66;">
                                            Generado el {generated_at}
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Aviso legal -->
                    <tr>
                        <td style="padding:12px 32px; text-align:center;">
                            <p style="margin:0; font-size:10px; color:#B0BEC5;
                                      line-height:1.5;">
                                Este correo fue generado automáticamente.
                                Por favor no responda a este mensaje.<br/>
                                Para soporte escriba a
                                <a href="mailto:support@bananalytics.com"
                                   style="color:#C38441; text-decoration:none;">
                                    support@bananalytics.com
                                </a>
                            </p>
                        </td>
                    </tr>

                </table>
                <!-- / Tarjeta principal -->

            </td>
        </tr>
    </table>

</body>
</html>"""


def _build_message(
    store: StoreRecord,
    stats: WeeklyStats,
    pdf_bytes: bytes,
    generated_at: str,
    period_label: str,
) -> MIMEMultipart:
    """
    Construye el objeto MIMEMultipart completo con cuerpo y adjunto.

    Args:
        store:        Datos de la tienda destinataria.
        stats:        Métricas para el cuerpo del correo.
        pdf_bytes:    Contenido binario del PDF generado por renderer.py.
        generated_at: Timestamp formateado de generación.
        period_label: Etiqueta del período (ej. "29-03-2026 al 04-04-2026").

    Returns:
        MIMEMultipart listo para ser enviado vía smtplib.
    """
    msg = MIMEMultipart("mixed")

    #  Encabezados del mensaje
    msg["Subject"] = (
        f"📊 Reporte Semanal BanAnalytics — {store.owner_name} ({period_label})"
    )
    msg["From"] = formataddr((_settings.smtp_from_name, _settings.smtp_from))
    msg["To"] = formataddr((store.owner_name, store.email))
    msg["Date"] = formatdate(localtime=False)
    msg["X-Mailer"] = "BanAnalytics Worker Reports v1.0"

    # Parte alternativa (plain + html)
    alt_part = MIMEMultipart("alternative")

    plain_text = _build_plain_text_body(store, stats, generated_at)
    html_body = _build_html_body(store, stats, generated_at)

    # El cliente de correo elige la "mejor" versión disponible.
    # Según RFC 2046, la última parte del multipart/alternative es la preferida,
    # por lo que plain va primero y html va después.
    alt_part.attach(MIMEText(plain_text, "plain", "utf-8"))
    alt_part.attach(MIMEText(html_body, "html", "utf-8"))

    msg.attach(alt_part)

    # Adjunto PDF
    filename = (
        f"BanAnalytics_Reporte_{store.store_id}_"
        f"{date.today().strftime('%Y-%m-%d')}.pdf"
    )
    pdf_attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    pdf_attachment.add_header(
        "Content-Disposition",
        "attachment",
        filename=filename,
    )
    msg.attach(pdf_attachment)

    return msg


def send_report(
    store: StoreRecord,
    stats: WeeklyStats,
    pdf_bytes: bytes,
    generated_at: str,
) -> bool:
    """
    Envía el reporte PDF al correo registrado de la tienda.

    Maneja automáticamente los dos modos de conexión SMTP:
      - STARTTLS (puerto 587, smtp_use_tls=True):  más común, recomendado.
      - SSL/TLS  (puerto 465, smtp_use_tls=False): conexión segura directa.

    Si el envío falla por cualquier razón (red, autenticación, buzón lleno,
    dirección inválida), loguea el error de forma detallada y retorna False
    sin propagar la excepción, permitiendo que el orquestador continúe
    procesando las demás tiendas.

    Args:
        store:        Datos de la tienda destinataria (incluye email).
        stats:        Métricas del período para el cuerpo del correo.
        pdf_bytes:    Bytes del PDF a adjuntar.
        generated_at: Timestamp de generación del reporte.

    Returns:
        True si el envío fue exitoso, False en caso contrario.
    """
    period_label = (
        f"{stats.date_range_start.strftime('%d-%m-%Y')} "
        f"al {stats.date_range_end.strftime('%d-%m-%Y')}"
    )

    logger.info(
        f"  📧 Enviando reporte a {store.email} "
        f"(tienda {store.store_id}, {store.owner_name})..."
    )

    msg = _build_message(
        store=store,
        stats=stats,
        pdf_bytes=pdf_bytes,
        generated_at=generated_at,
        period_label=period_label,
    )

    try:
        if _settings.smtp_use_tls:
            # Modo STARTTLS: conexión sin cifrar que se eleva a TLS con EHLO+STARTTLS
            with smtplib.SMTP(
                host=_settings.smtp_host,
                port=_settings.smtp_port,
                timeout=_SMTP_TIMEOUT_SECONDS,
            ) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                server.login(_settings.smtp_user, _settings.smtp_password)
                server.sendmail(
                    from_addr=_settings.smtp_from,
                    to_addrs=[store.email],
                    msg=msg.as_bytes(),
                )
        else:
            # Modo SSL directo: conexión cifrada desde el inicio
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(
                host=_settings.smtp_host,
                port=_settings.smtp_port,
                context=context,
                timeout=_SMTP_TIMEOUT_SECONDS,
            ) as server:
                server.login(_settings.smtp_user, _settings.smtp_password)
                server.sendmail(
                    from_addr=_settings.smtp_from,
                    to_addrs=[store.email],
                    msg=msg.as_bytes(),
                )

        pdf_kb = len(pdf_bytes) / 1024
        logger.info(
            f"  ✅ Correo enviado exitosamente a {store.email} "
            f"(PDF: {pdf_kb:.1f} KB)"
        )
        return True

    except smtplib.SMTPAuthenticationError as exc:
        logger.error(
            f"  ❌ Error de autenticación SMTP para {store.email}: {exc}. "
            "Verifique SMTP_USER y SMTP_PASSWORD en los secrets de GitHub Actions."
        )
    except smtplib.SMTPRecipientsRefused as exc:
        logger.error(
            f"  ❌ Dirección rechazada por el servidor: {store.email}. "
            f"Detalle: {exc}"
        )
    except smtplib.SMTPException as exc:
        logger.error(
            f"  ❌ Error SMTP al enviar a {store.email}: {exc}"
        )
    except TimeoutError:
        logger.error(
            f"  ❌ Timeout ({_SMTP_TIMEOUT_SECONDS}s) al conectar con "
            f"{_settings.smtp_host}:{_settings.smtp_port}"
        )
    except OSError as exc:
        logger.error(
            f"  ❌ Error de red al enviar a {store.email}: {exc}"
        )

    return False
