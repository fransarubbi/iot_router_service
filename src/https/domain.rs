//! Capa de transporte HTTPS y mTLS para el Router Service.
//!
//! Este módulo define el servidor web asíncrono responsable de recibir conexiones
//! desde los dispositivos Hubs en Bypass. Actúa como un puente que recibe peticiones
//! HTTP seguras, valida sesiones en el registro de memoria, decodifica paquetes
//! binarios (MessagePack) y los inyecta en el bus de mensajes principal
//! (gRPC dispatcher) del sistema IoT.

use crate::connections::logic::ConnectionRegistry;
use crate::error::domain::AppError;
use crate::grpc;
use crate::grpc::{ToDataSaver, to_data_saver};
use crate::message::domain::{AlertAir, AlertTh};
use crate::metrics::logic::metrics_handler;
use crate::middleware::logic::{record_metrics, request_id};
use crate::router::domain::RouterMessage;
use anyhow::Result;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::{
    Router,
    extract::{ConnectInfo, State},
    middleware as axum_middleware,
    response::Json,
    routing::get,
};
use serde_json::{Value, json};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

/// Estado global compartido para todos los handlers de la interfaz HTTPS.
///
/// Encapsula el registro de conexiones activas para el seguimiento de sesiones mTLS
/// y el canal de transmisión (`mpsc::Sender`) para enrutar los mensajes validados
/// hacia el núcleo de procesamiento gRPC.
#[derive(Clone)]
pub struct HttpsService {
    /// Registro en memoria (concurrente) de los dispositivos conectados.
    pub registry: ConnectionRegistry,
    /// Canal de comunicación principal para despachar mensajes al router interno.
    pub central_tx: mpsc::Sender<RouterMessage>,
}

impl HttpsService {
    /// Construye una nueva instancia del servicio HTTPS.
    ///
    /// # Argumentos
    /// * `central_tx` - Extremo transmisor del canal asíncrono hacia el Dispatcher.
    pub fn new(central_tx: mpsc::Sender<RouterMessage>) -> Self {
        Self {
            // Se define un tiempo de inactividad de 60 segundos antes de considerar muerta una conexión
            registry: ConnectionRegistry::new(Duration::from_secs(60)),
            central_tx,
        }
    }
}

// ── Construcción del Router ───────────────────────────────────────────────────

/// Ensambla el enrutador HTTP de Axum con todas sus rutas y middlewares.
///
/// Define los endpoints de negocio, inyecta el estado compartido (`HttpsService`),
/// y aplica una pila de middlewares de seguridad, observabilidad y métricas de
/// abajo hacia arriba.
///
/// # Argumentos
/// * `state` - El estado global que será accesible por los handlers.
pub fn build_router(state: HttpsService) -> Router {
    Router::new()
        // Rutas de negocio
        .route("/connect", get(handle_connection))
        .route("/status", get(handle_status))
        .route("/connections", get(handle_connections_list))
        // Ruta de métricas (idealmente en puerto separado en producción real)
        .route("/metrics", get(metrics_handler))
        // Ruta de alertas publicadas por los Hubs en Bypass
        .route("/alerts", post(handle_telemetry))
        // Estado compartido
        .with_state(state)
        // Capas de middleware (se aplican de abajo hacia arriba)
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
        .layer(DefaultBodyLimit::max(1 * 1024 * 1024))
        .layer(axum_middleware::from_fn(record_metrics))
        .layer(axum_middleware::from_fn(request_id))
        .layer(TraceLayer::new_for_http())
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// Endpoint principal de conexión (`GET /connect`).
///
/// Es el primer punto de contacto para un dispositivo. Extrae la IP de origen,
/// registra la nueva sesión en el `ConnectionRegistry` y devuelve un identificador
/// único para la conexión confirmando el handshake mTLS.
async fn handle_connection(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<HttpsService>,
) -> Json<Value> {
    let client_cn = format!("client@{}", addr.ip());
    let conn_id = state.registry.register(client_cn.clone(), addr.to_string());
    state.registry.touch(&conn_id);

    Json(json!({
        "status": "ok",
        "connection_id": conn_id,
        "client_cn": client_cn,
        "message": "Conexión mTLS establecida y registrada"
    }))
}

/// Endpoint de recepción de telemetría (`POST /alerts`).
///
/// Recibe datos de sensores empaquetados en formato binario (MessagePack).
/// Intenta deserializar el payload a la estructura de dominio interna (`Message`),
/// la convierte a su equivalente en Protobuf (gRPC), y la despacha al enrutador central.
///
/// # Errores
/// Retorna `AppError::AlertError` (Código HTTP 400) si el formato MessagePack
/// es inválido o si el canal interno de gRPC está saturado o cerrado.
async fn handle_telemetry(
    State(state): State<HttpsService>,
    body: Bytes,
) -> Result<Json<Value>, AppError> {

    match rmp_serde::from_slice::<serde_json::Value>(&body) {
        Ok(json_val) => tracing::error!("payload recibido desde el Hub: {}", json_val),
        Err(e) => tracing::error!(">>> el payload está roto (basura o truncado): {:?}", e),
    }

    // Intentamos parsear como Alerta de Aire
    match rmp_serde::from_slice::<AlertAir>(&body) {
        Ok(alert) => {
            let grpc_alert = grpc::AlertAir {
                metadata: Some(grpc::Metadata {
                    sender_user_id: alert.metadata.sender_user_id,
                    destination_id: alert.metadata.destination_id,
                    timestamp: alert.metadata.timestamp as i64,
                }),
                network: alert.network,
                initial_air_quality: alert.initial_air_quality,
                actual_air_quality: alert.actual_air_quality,
            };
            let router_msg = RouterMessage::ToData {
                message: ToDataSaver {
                    payload: Some(to_data_saver::Payload::AlertAir(grpc_alert)),
                },
            };
            
            if state.central_tx.send(router_msg).await.is_err() {
                return Err(AppError::AlertError("Error interno: canal cerrado".to_string()));
            }
            return Ok(Json(serde_json::json!({"status": "recibido y enrutado"})));
        }
        Err(_) => {}
    }

    match rmp_serde::from_slice::<AlertTh>(&body) {
        Ok(alert) => {
            let grpc_alert = grpc::AlertTh {
                metadata: Some(grpc::Metadata {
                    sender_user_id: alert.metadata.sender_user_id,
                    destination_id: alert.metadata.destination_id,
                    timestamp: alert.metadata.timestamp as i64,
                }),
                network: alert.network,
                initial_temp: alert.initial_temp,
                actual_temp: alert.actual_temp,
            };
            let router_msg = RouterMessage::ToData {
                message: ToDataSaver {
                    payload: Some(to_data_saver::Payload::AlertTh(grpc_alert)),
                },
            };
            
            if state.central_tx.send(router_msg).await.is_err() {
                return Err(AppError::AlertError("Error interno: canal cerrado".to_string()));
            }
            return Ok(Json(serde_json::json!({"status": "recibido y enrutado"})));
        }
        Err(_) => {}
    }

    // 3. Si ambos fallan, el payload es inválido o no corresponde a una alerta
    tracing::error!("El payload recibido no pudo ser deserializado como AlertAir ni AlertTh");
    Err(AppError::AlertError("Error: payload no coincide con ninguna alerta conocida".to_string()))
}

/// Endpoint de diagnóstico de salud del servicio (`GET /status`).
///
/// Proporciona un liveness probe ideal para balanceadores de carga o Kubernetes.
/// Expone estadísticas en tiempo real como las conexiones activas y la versión del compilado.
async fn handle_status(State(state): State<HttpsService>) -> Json<Value> {
    Json(json!({
        "status": "healthy",
        "active_connections": state.registry.count(),
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// Endpoint administrativo de listado de conexiones (`GET /connections`).
///
/// Toma una fotografía ("snapshot") del estado interno de memoria y devuelve
/// la lista detallada de todos los dispositivos mTLS conectados actualmente.
async fn handle_connections_list(State(state): State<HttpsService>) -> Json<Value> {
    let conns = state.registry.snapshot();
    Json(json!({
        "total": conns.len(),
        "connections": conns,
    }))
}
