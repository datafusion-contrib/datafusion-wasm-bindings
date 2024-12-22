// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A fork of object_store_opendal::OpendalStore that uses unsafe Rust
//! to erase \![`Send`] and \![`Sync`] for OpenDAL's future.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Future, FutureExt, Stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};
use opendal::{Entry, FuturesBytesStream, Metadata, Metakey, Operator};
use pin_project::pin_project;

#[derive(Debug)]
pub struct OpendalStore {
    inner: Operator,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    pub fn new(op: Operator) -> Self {
        Self { inner: op }
    }
}

impl std::fmt::Display for OpendalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OpenDAL({:?})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for OpendalStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        ForceSend::new(
            self.inner
                .write(location.as_ref(), payload.as_ref()[0].clone()),
        )
        .await
        .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "put_opts is not implemented so far",
            )),
        })
    }

    /// Perform a multipart upload with options
    ///
    /// Client should prefer [`ObjectStore::put`] for small payloads, as streaming uploads
    /// typically require multiple separate requests. See [`MultipartUpload`] for more information
    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "put_multipart_opts is not implemented so far",
            )),
        })
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "get_opts is not implemented so far",
            )),
        })
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let meta = ForceSend::new(self.inner.stat(location.as_ref()))
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified().unwrap_or_default(),
            size: meta.content_length() as usize,
            e_tag: meta.etag().map(|x| x.to_string()),
            version: meta.version().map(|x| x.to_string()),
        };
        let r = ForceSend::new(self.inner.reader(location.as_ref()))
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(ForceSend::new(OpendalReader {
                inner: ForceSend::new(r.into_bytes_stream(0..meta.size as u64))
                    .await
                    .unwrap(),
            }))),
            range: (0..meta.size),
            meta,
            attributes: Attributes::default(),
        })
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let meta = ForceSend::new(self.inner.stat(location.as_ref()))
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified().unwrap_or_default(),
            size: meta.content_length() as usize,
            e_tag: meta.etag().map(|x| x.to_string()),
            version: meta.version().map(|x| x.to_string()),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        ForceSend::new(self.inner.delete(location.as_ref()))
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        // object_store `Path` always removes trailing slash
        // need to add it back
        let path = prefix.map_or("".into(), |x| format!("{}/", x));

        let fut = async move {
            let stream = self
                .inner
                .lister_with(&path)
                .metakey(Metakey::ContentLength | Metakey::LastModified)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            let stream = stream.then(|res| async {
                let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                let meta = entry.metadata();

                Ok(format_object_meta(entry.path(), meta))
            });
            Ok::<_, object_store::Error>(stream)
        };

        ForceSend::new(fut.into_stream().try_flatten()).boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| format!("{}/", x));
        let offset = offset.clone();

        let fut = async move {
            let fut = if self.inner.info().full_capability().list_with_start_after {
                ForceSend::new(
                    self.inner
                        .lister_with(&path)
                        .start_after(offset.as_ref())
                        .metakey(Metakey::ContentLength | Metakey::LastModified)
                        .recursive(true)
                        .await
                        .map_err(|err| format_object_store_error(err, &path))?
                        .then(try_format_object_meta),
                )
                .boxed()
            } else {
                ForceSend::new(
                    self.inner
                        .lister_with(&path)
                        .metakey(Metakey::ContentLength | Metakey::LastModified)
                        .recursive(true)
                        .await
                        .map_err(|err| format_object_store_error(err, &path))?
                        .try_filter(move |entry| {
                            futures::future::ready(entry.path() > offset.as_ref())
                        })
                        .then(try_format_object_meta),
                )
                .boxed()
            };

            Ok::<_, object_store::Error>(fut)
        };

        ForceSend::new(fut.into_stream().try_flatten()).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let path = prefix.map_or("".into(), |x| format!("{}/", x));
        let stream = ForceSend::new(async {
            self.inner
                .lister_with(&path)
                .metakey(Metakey::Mode | Metakey::ContentLength | Metakey::LastModified)
                .await
        })
        .await
        .map_err(|err| format_object_store_error(err, &path))?;
        let mut stream = ForceSend::new(stream);

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                common_prefixes.push(entry.path().into());
            } else {
                objects.push(format_object_meta(entry.path(), meta));
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy is not implemented so far",
            )),
        })
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "rename is not implemented so far",
            )),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy_if_not_exists is not implemented so far",
            )),
        })
    }
}

fn format_object_store_error(err: opendal::Error, path: &str) -> object_store::Error {
    use opendal::ErrorKind;
    match err.kind() {
        ErrorKind::NotFound => object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        ErrorKind::Unsupported => object_store::Error::NotSupported {
            source: Box::new(err),
        },
        ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        kind => object_store::Error::Generic {
            store: kind.into_static(),
            source: Box::new(err),
        },
    }
}

fn format_object_meta(path: &str, meta: &Metadata) -> ObjectMeta {
    ObjectMeta {
        location: path.into(),
        last_modified: meta.last_modified().unwrap_or_default(),
        size: meta.content_length() as usize,
        e_tag: meta.etag().map(|x| x.to_string()),
        version: meta.version().map(|x| x.to_string()),
    }
}

async fn try_format_object_meta(res: Result<Entry, opendal::Error>) -> Result<ObjectMeta> {
    let entry = res.map_err(|err| format_object_store_error(err, ""))?;
    let meta = entry.metadata();

    Ok(format_object_meta(entry.path(), meta))
}

struct OpendalReader {
    inner: FuturesBytesStream,
}

impl Stream for OpendalReader {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = Pin::new(&mut self.get_mut().inner);
        inner
            .poll_next(cx)
            .map_err(|err| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            })
    }
}

#[pin_project]
struct ForceSend<T> {
    #[pin]
    item: T,
}

unsafe impl<T> Send for ForceSend<T> {}
unsafe impl<T> Sync for ForceSend<T> {}

impl<T> Future for ForceSend<T>
where
    T: Future,
{
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.item.poll(cx)
    }
}

impl<T> Stream for ForceSend<T>
where
    T: Stream,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.item.poll_next(cx)
    }
}

impl<T> ForceSend<T> {
    pub fn new(item: T) -> Self {
        Self { item }
    }
}
