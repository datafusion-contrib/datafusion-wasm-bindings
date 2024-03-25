use std::sync::{Arc, Mutex};

use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::ObjectStore;
use opendal::services::{Http, S3};
use opendal::Operator;
use url::Url;

use crate::unsafe_opendal_store::OpendalStore;

#[derive(Debug, Default)]
pub struct S3Config {
    pub root: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Default)]
struct RegistryState {
    s3_config: S3Config,
}

#[derive(Debug, Default, Clone)]
pub struct OpendalRegistry {
    state: Arc<Mutex<RegistryState>>,
}

impl OpendalRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_s3_config(&self, s3_config: S3Config) {
        let mut state = self.state.lock().unwrap();
        state.s3_config = s3_config;
    }

    pub fn build_from_url(&self, url: &Url) -> Option<Operator> {
        match url.scheme().to_ascii_lowercase().as_str() {
            "s3" => {
                let state = self.state.lock().unwrap();

                let mut builder = S3::default();

                // Setup builders
                builder.root(&state.s3_config.root);
                builder.bucket(&state.s3_config.bucket);
                builder.region(&state.s3_config.region);
                builder.endpoint("https://s3.amazonaws.com");
                builder.access_key_id(&state.s3_config.access_key_id);
                builder.secret_access_key(&state.s3_config.secret_access_key);

                Some(Operator::new(builder).ok()?.finish())
            }
            "http" | "https" => {
                let mut http = Http::default();
                http.endpoint(&format!(
                    "{}://{}:{}",
                    url.scheme(),
                    url.host_str().unwrap_or_default(),
                    url.port_or_known_default().unwrap_or(0)
                ));
                Some(Operator::new(http).unwrap().finish())
            }
            _ => None,
        }
    }
}

impl ObjectStoreRegistry for OpendalRegistry {
    fn register_store(
        &self,
        url: &Url,
        _store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let operator = self.build_from_url(url)?;
        Some(Arc::new(OpendalStore::new(operator)))
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let operator = self.build_from_url(url).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Failed to build operator from URL".to_string(),
            )
        })?;
        Ok(Arc::new(OpendalStore::new(operator)))
    }
}
