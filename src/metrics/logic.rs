//! Integración de métricas de telemetría y monitoreo con Prometheus.
//!
//! Este módulo configura e inicializa el recolector global de métricas del sistema,
//! permitiendo registrar eventos de la capa de red (como conexiones mTLS,
//! duración de peticiones HTTP y errores).
//!
//! Expone el estado interno del servidor en un formato compatible con Prometheus
//! a través de un endpoint HTTP estándar (`/metrics`), el cual puede ser consumido
//! por herramientas de observabilidad como Prometheus y Grafana.


use axum::{http::StatusCode, response::IntoResponse};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;
use tracing::info;


/// Almacenamiento global y seguro frente a hilos para el manejador de Prometheus.
///
/// Utiliza `OnceLock` para garantizar que el recolector se inicialice una única vez
/// durante el arranque del sistema y proporcione acceso concurrente de solo lectura
/// al motor de renderizado de métricas en las subsecuentes peticiones web.
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();


/// Configura el exportador, instala el recolector global y define los metadatos.
///
/// Esta función inicializa el `PrometheusBuilder` y registra la metadata (descripciones)
/// estática para todas las métricas que el router emitirá durante su ciclo de vida.
///
/// # Panics
///
/// Esta función hará un `panic!` si se invoca más de una vez durante la vida útil
/// de la aplicación, ya que el motor de `metrics` de Rust solo permite instalar
/// un recolector global.
///
/// # Retorna
///
/// Retorna un `PrometheusHandle` clonado que permite renderizar el estado actual
/// de las métricas a un string de texto plano.
pub fn init_prometheus() -> PrometheusHandle {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("Error: no se pudo instalar recorder de Prometheus");

    // Registrar métricas con descripciones
    metrics::describe_counter!(
        "http_requests_total",
        "Total de requests HTTP recibidos"
    );
    metrics::describe_histogram!(
        "http_request_duration_seconds",
        "Duración de requests HTTP en segundos"
    );
    metrics::describe_gauge!(
        "active_connections",
        "Conexiones mTLS activas en este momento"
    );
    metrics::describe_counter!(
        "mtls_auth_failures_total",
        "Total de fallos de autenticación mTLS"
    );

    info!("Info: exporter de Prometheus inicializado");
    handle.clone()
}


/// Endpoint HTTP para la recolección externa de métricas (`GET /metrics`).
///
/// Este *handler* de Axum es el punto de acceso para que el servidor de Prometheus
/// raspe (scrape) los datos. Accede al recolector global a través del `OnceLock`
/// y renderiza el estado de los contadores, gauges e histogramas en el formato
/// de texto oficial de Prometheus.
///
/// # Respuestas
///
/// * `200 OK` - Acompañado del payload de texto con las métricas si el recolector está activo.
/// * `503 Service Unavailable` - Si por algún motivo el endpoint es invocado antes
/// de que el sistema global haya sido inicializado.
pub async fn metrics_handler() -> impl IntoResponse {
    match PROMETHEUS_HANDLE.get() {
        Some(handle) => (StatusCode::OK, handle.render()),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Métricas no inicializadas".to_string(),
        ),
    }
}


/// Punto de entrada principal para activar el subsistema de métricas.
///
/// Debe ser invocado al inicio de la aplicación (usualmente en la función de arranque
/// del servidor). Instala el recolector subyacente y guarda de forma segura el
/// handle de renderizado en la estructura estática global `PROMETHEUS_HANDLE`.
pub fn init_global() {
    let handle = init_prometheus();
    PROMETHEUS_HANDLE.set(handle).ok();
}