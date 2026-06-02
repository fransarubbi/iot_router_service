use serde::{Serialize, Deserialize};


/// Metadatos estándar para todos los mensajes del sistema.
///
/// Proporciona contexto de trazabilidad, origen y destino para cada paquete de datos.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub sender_user_id: String,
    pub destination_id: String,
    pub timestamp: i64,
}


/// Alerta de calidad de aire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertAir {
    pub metadata: Metadata,
    pub network: String,
    pub co2_initial_ppm: f32,
    pub co2_actual_ppm: f32,
}


/// Alerta de Temperatura y Humedad.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertTh {
    pub metadata: Metadata,
    pub network: String,
    pub initial_temp: f32,
    pub actual_temp: f32,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    AlertAir(AlertAir),
    AlertTh(AlertTh),
}