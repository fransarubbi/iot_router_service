//! Gestión centralizada de errores HTTP para el Router Service.
//!
//! Este módulo define el tipo de error principal (`AppError`) utilizado en toda
//! la capa de transporte HTTPS (Axum). Provee una forma unificada de convertir
//! fallos internos del dominio o del sistema en respuestas HTTP estructuradas
//! en formato JSON, garantizando que el cliente web reciba siempre una estructura
//! de error predecible.


use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use serde_json::json;
use thiserror::Error;


/// Representa todos los posibles errores que pueden ocurrir en la capa HTTP.
///
/// Utiliza `thiserror` para generar automáticamente las implementaciones del
/// trait `std::fmt::Display` basándose en los atributos `#[error(...)]`.
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum AppError {
    /// Ocurre cuando un cliente intenta acceder sin credenciales o con credenciales insuficientes.
    #[error("No autorizado: {0}")]
    Unauthorized(String),

    /// Ocurre durante el handshake mTLS si el certificado presentado fue rechazado o es inválido.
    #[error("Certificado de cliente inválido: {0}")]
    InvalidClientCert(String),

    /// Ocurre cuando el cliente solicita una ruta o recurso que no existe en el servidor.
    #[error("Recurso no encontrado: {0}")]
    NotFound(String),

    /// Representa fallos críticos del servidor (ej. caídas de base de datos, fallos de I/O).
    /// El atributo `#[from]` permite usar el operador `?` para convertir automáticamente
    /// cualquier `anyhow::Error` en esta variante.
    #[error("Error interno: {0}")]
    Internal(#[from] anyhow::Error),

    /// Ocurre cuando el cliente ha excedido el límite de peticiones por segundo/minuto.
    #[error("Rate limit excedido")]
    RateLimited,

    /// Ocurre cuando falla el procesamiento, deserialización o enrutamiento
    /// de un mensaje de telemetría (Alerta) hacia el despachador gRPC.
    #[error("{0}")]
    AlertError(String),
}


/// Permite que `AppError` sea retornado directamente desde los handlers de Axum.
///
/// Esta implementación intercepta el error, evalúa de qué variante se trata,
/// y construye una respuesta HTTP asignando el `StatusCode` adecuado y formateando
/// el cuerpo de la respuesta como un objeto JSON unificado:
///
/// ```json
/// {
///   "error": "Descripción del error",
///   "status": 400
/// }
/// ```
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            AppError::InvalidClientCert(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            AppError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            AppError::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                "Rate limit excedido".to_string(),
            ),
            AppError::AlertError(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
        };

        // Construye el payload JSON estándar para todos los errores
        let body = Json(json!({
            "error": message,
            "status": status.as_u16(),
        }));

        (status, body).into_response()
    }
}