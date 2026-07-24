use serde::{Deserialize, Serialize};

/// Metadatos estándar para todos los mensajes del sistema.
///
/// Proporciona contexto de trazabilidad, origen y destino para cada paquete de datos.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(rename = "s")]
    pub sender_user_id: String,
    #[serde(rename = "d")]
    pub destination_id: String,
    #[serde(rename = "t")]
    pub timestamp: u64,
}

/// Alerta de calidad de aire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertAir {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "ia")]
    pub initial_air_quality: f32,
    #[serde(rename = "aa")]
    pub actual_air_quality: f32,
}

/// Alerta de Temperatura y Humedad.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertTh {
    #[serde(rename = "m")]
    pub metadata: Metadata,
    #[serde(rename = "n")]
    pub network: String,
    #[serde(rename = "i")]
    pub initial_temp: f32,
    #[serde(rename = "a")]
    pub actual_temp: f32,
}

