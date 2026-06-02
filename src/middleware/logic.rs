//! Capa de Middlewares HTTP: Trazabilidad, identificación y métricas.
//!
//! Este módulo contiene las funciones interceptoras (middlewares) de Axum que se
//! ejecutan en cada petición HTTP antes y/o después de llegar a los handlers de negocio.
//!
//! Sus responsabilidades principales incluyen:
//! 1. **Trazabilidad cruzada**: Asignación de identificadores únicos a cada transacción.
//! 2. **Observabilidad**: Recolección automatizada de métricas de rendimiento y uso
//!    para su posterior exposición en Prometheus.


use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use std::time::Instant;
use uuid::Uuid;


/// Nombre del encabezado HTTP estandarizado para la trazabilidad de peticiones.
///
/// Al instanciarlo estáticamente usando `from_static`, se optimiza el uso de
/// memoria evitando alojamientos innecesarios (`allocations`) en cada petición.
pub static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");


/// Middleware que inyecta un identificador único global (UUID) a cada transacción.
///
/// Este middleware actúa en dos fases:
/// 1. **Fase de entrada (Pre-procesamiento):** Genera un UUID v4 y lo inserta en los
///    encabezados (headers) de la petición entrante. Esto permite que otros middlewares,
///    sistemas de logging o handlers puedan extraer el ID para rastrear el flujo interno.
/// 2. **Fase de salida (Post-procesamiento):** Una vez que el servidor genera la respuesta,
///    inyecta exactamente el mismo UUID en los encabezados de respuesta hacia el cliente.
///    De este modo, si ocurre un error, el cliente puede reportar este ID al soporte técnico.
///
/// # Argumentos
/// * `req` - La petición HTTP entrante.
/// * `next` - El control de flujo para llamar a la siguiente capa del router.
pub async fn request_id(mut req: Request, next: Next) -> Response {
    let id = Uuid::new_v4().to_string();
    req.headers_mut().insert(
        X_REQUEST_ID.clone(),
        HeaderValue::from_str(&id).unwrap(),
    );

    let mut res = next.run(req).await;
    res.headers_mut().insert(
        X_REQUEST_ID.clone(),
        HeaderValue::from_str(&id).unwrap(),
    );
    res
}


/// Middleware de observabilidad que registra las métricas de tráfico y latencia.
///
/// Envuelve la ejecución de cada petición para recolectar datos operacionales críticos.
/// Al finalizar la solicitud, registra las siguientes métricas en el registro global:
///
/// * **`http_requests_total`** (Contador): Incrementa en 1, etiquetado por método, ruta y código HTTP.
/// * **`http_request_duration_seconds`** (Histograma): Registra el tiempo exacto que tomó procesar la petición.
///
/// # Argumentos
/// * `req` - La petición HTTP entrante.
/// * `next` - El control de flujo para ejecutar el handler correspondiente.
pub async fn record_metrics(req: Request, next: Next) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let start = Instant::now();

    let res = next.run(req).await;

    let status = res.status().as_u16().to_string();
    let duration = start.elapsed().as_secs_f64();

    metrics::counter!(
        "http_requests_total",
        "method" => method.clone(),
        "path" => path.clone(),
        "status" => status
    )
        .increment(1);

    metrics::histogram!(
        "http_request_duration_seconds",
        "method" => method,
        "path" => path,
    )
        .record(duration);

    res
}