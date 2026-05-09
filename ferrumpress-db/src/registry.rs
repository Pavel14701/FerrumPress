use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use ferrumpress_core::model::{Model, ModelService};

pub struct ModelRegistry {
    models: Mutex<HashMap<String, Arc<dyn Model>>>,
}
impl ModelRegistry {
    pub fn new() -> Self { Self { models: Mutex::new(HashMap::new()) } }
    pub fn register(&self, model: Arc<dyn Model>) -> Result<(), String> {
        let mut map = self.models.lock().unwrap();
        let name = model.table_name().to_string();
        if map.contains_key(&name) { return Err(format!("Model '{}' already registered", name)); }
        map.insert(name, model);
        Ok(())
    }
    pub fn get(&self, table_name: &str) -> Option<Arc<dyn Model>> {
        self.models.lock().unwrap().get(table_name).cloned()
    }
    pub fn remove(&self, table_name: &str) -> Option<Arc<dyn Model>> {
        self.models.lock().unwrap().remove(table_name)
    }
    pub fn clear(&self) { self.models.lock().unwrap().clear(); }
}

pub struct ServiceRegistry {
    services: Mutex<HashMap<String, Arc<dyn ModelService>>>,
}
impl ServiceRegistry {
    pub fn new() -> Self { Self { services: Mutex::new(HashMap::new()) } }
    pub fn register(&self, name: &str, service: Arc<dyn ModelService>) -> Result<(), String> {
        let mut map = self.services.lock().unwrap();
        if map.contains_key(name) { return Err(format!("Service for '{}' already registered", name)); }
        map.insert(name.to_string(), service);
        Ok(())
    }
    pub fn get(&self, name: &str) -> Option<Arc<dyn ModelService>> {
        self.services.lock().unwrap().get(name).cloned()
    }
    pub fn remove(&self, name: &str) -> Option<Arc<dyn ModelService>> {
        self.services.lock().unwrap().remove(name)
    }
}

lazy_static! {
    pub static ref MODEL_REGISTRY: ModelRegistry = ModelRegistry::new();
    pub static ref SERVICE_REGISTRY: ServiceRegistry = ServiceRegistry::new();
}