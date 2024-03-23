use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::ObjectStore;
use opendal::services::Http;
use opendal::Operator;
use url::Url;

// use object_store_opendal::OpendalStore;
use crate::unsafe_opendal_store::OpendalStore;

#[derive(Debug, Default)]
pub struct OpendalRegistry {}

impl ObjectStoreRegistry for OpendalRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: std::sync::Arc<dyn ObjectStore>,
    ) -> Option<std::sync::Arc<dyn ObjectStore>> {
        let mut http = Http::default();
        http.endpoint(&format!(
            "{}://{}",
            url.scheme(),
            url.host_str().unwrap_or_default()
        ));
        Some(Arc::new(OpendalStore::new(
            Operator::new(http).unwrap().finish(),
        )))
        // let http = HttpBuilder::new()
        //     .with_url(url.origin().ascii_serialization())
        //     .build()
        //     .unwrap();
        // Some(Arc::new(http))
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<std::sync::Arc<dyn ObjectStore>> {
        let mut http = Http::default();
        http.endpoint(&format!(
            "{}://{}",
            url.scheme(),
            url.host_str().unwrap_or_default()
        ));

        Ok(Arc::new(OpendalStore::new(
            Operator::new(http).unwrap().finish(),
        )))
        // let http = HttpBuilder::new()
        //     .with_url(url.origin().ascii_serialization())
        //     .build()
        //     .unwrap();
        // Ok(Arc::new(http))
    }
}

fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}
