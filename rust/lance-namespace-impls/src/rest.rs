// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! REST implementation of Lance Namespace

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use lance_namespace::apis::configuration::{Configuration, RequestMiddleware};
use lance_namespace::apis::{namespace_api, table_api, tag_api, transaction_api};
use lance_namespace::models::{
    AlterTableAddColumnsRequest, AlterTableAddColumnsResponse, AlterTableAlterColumnsRequest,
    AlterTableAlterColumnsResponse, AlterTableDropColumnsRequest, AlterTableDropColumnsResponse,
    AlterTransactionRequest, AlterTransactionResponse, AnalyzeTableQueryPlanRequest,
    CountTableRowsRequest, CreateEmptyTableRequest, CreateEmptyTableResponse,
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableIndexRequest,
    CreateTableIndexResponse, CreateTableRequest, CreateTableResponse,
    CreateTableScalarIndexResponse, CreateTableTagRequest, CreateTableTagResponse,
    DeleteFromTableRequest, DeleteFromTableResponse, DeleteTableTagRequest, DeleteTableTagResponse,
    DeregisterTableRequest, DeregisterTableResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DescribeTableIndexStatsRequest, DescribeTableIndexStatsResponse,
    DescribeTableRequest, DescribeTableResponse, DescribeTransactionRequest,
    DescribeTransactionResponse, DropNamespaceRequest, DropNamespaceResponse,
    DropTableIndexRequest, DropTableIndexResponse, DropTableRequest, DropTableResponse,
    ExplainTableQueryPlanRequest, GetTableStatsRequest, GetTableStatsResponse,
    GetTableTagVersionRequest, GetTableTagVersionResponse, InsertIntoTableRequest,
    InsertIntoTableResponse, ListNamespacesRequest, ListNamespacesResponse,
    ListTableIndicesRequest, ListTableIndicesResponse, ListTableTagsRequest, ListTableTagsResponse,
    ListTableVersionsRequest, ListTableVersionsResponse, ListTablesRequest, ListTablesResponse,
    MergeInsertIntoTableRequest, MergeInsertIntoTableResponse, NamespaceExistsRequest,
    QueryTableRequest, RegisterTableRequest, RegisterTableResponse, RenameTableRequest,
    RenameTableResponse, RestoreTableRequest, RestoreTableResponse, TableExistsRequest,
    UpdateTableRequest, UpdateTableResponse, UpdateTableSchemaMetadataRequest,
    UpdateTableSchemaMetadataResponse, UpdateTableTagRequest, UpdateTableTagResponse,
};

use lance_core::{box_error, Error, Result};

use lance_namespace::LanceNamespace;

/// Provides headers for REST API requests.
///
/// Implementations can cache tokens and refresh them as needed.
/// The `get_headers` method is called before each API request.
///
/// # Examples
///
/// ```
/// # use lance_namespace_impls::rest::{HeaderProvider, StaticHeaderProvider};
/// # use std::collections::HashMap;
/// // Static headers
/// let mut headers = HashMap::new();
/// headers.insert("Authorization".to_string(), "Bearer token".to_string());
/// let provider = StaticHeaderProvider::new(headers);
/// ```
#[async_trait]
pub trait HeaderProvider: Send + Sync + std::fmt::Debug {
    /// Get headers to send with requests.
    ///
    /// Called before API requests - implementation should handle caching/refresh.
    async fn get_headers(&self) -> Result<HashMap<String, String>>;

    /// Return self as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Static header provider that returns the same headers for every request.
///
/// Use this for headers that don't change during the lifetime of the namespace,
/// such as API keys or user agent strings.
#[derive(Debug, Clone)]
pub struct StaticHeaderProvider {
    headers: HashMap<String, String>,
}

impl StaticHeaderProvider {
    /// Create a new static header provider with the given headers.
    pub fn new(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }

    /// Create a new static header provider from an iterator of key-value pairs.
    pub fn from_iter<I, K, V>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            headers: iter
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

#[async_trait]
impl HeaderProvider for StaticHeaderProvider {
    async fn get_headers(&self) -> Result<HashMap<String, String>> {
        Ok(self.headers.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Middleware that adds headers from a HeaderProvider to each request.
#[derive(Debug)]
struct HeaderMiddleware {
    provider: Arc<dyn HeaderProvider>,
}

#[async_trait]
impl RequestMiddleware for HeaderMiddleware {
    async fn process(&self, mut request: reqwest::Request) -> reqwest::Request {
        if let Ok(headers) = self.provider.get_headers().await {
            let header_map = request.headers_mut();
            for (name, value) in headers {
                if let (Ok(n), Ok(v)) = (
                    reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                    reqwest::header::HeaderValue::from_str(&value),
                ) {
                    header_map.insert(n, v);
                }
            }
        }
        request
    }
}

/// Builder for creating a RestNamespace.
///
/// This builder provides a fluent API for configuring and establishing
/// connections to REST-based Lance namespaces.
///
/// # Examples
///
/// ```no_run
/// # use lance_namespace_impls::RestNamespaceBuilder;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a REST namespace
/// let namespace = RestNamespaceBuilder::new("http://localhost:8080")
///     .delimiter(".")
///     .header("Authorization", "Bearer token")
///     .build();
/// # Ok(())
/// # }
/// ```
pub struct RestNamespaceBuilder {
    uri: String,
    delimiter: String,
    header_provider: Option<Arc<dyn HeaderProvider>>,
    cert_file: Option<String>,
    key_file: Option<String>,
    ssl_ca_cert: Option<String>,
    assert_hostname: bool,
}

impl std::fmt::Debug for RestNamespaceBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestNamespaceBuilder")
            .field("uri", &self.uri)
            .field("delimiter", &self.delimiter)
            .field(
                "header_provider",
                &self.header_provider.as_ref().map(|_| "..."),
            )
            .field("cert_file", &self.cert_file)
            .field("key_file", &self.key_file)
            .field("ssl_ca_cert", &self.ssl_ca_cert)
            .field("assert_hostname", &self.assert_hostname)
            .finish()
    }
}

impl Clone for RestNamespaceBuilder {
    fn clone(&self) -> Self {
        Self {
            uri: self.uri.clone(),
            delimiter: self.delimiter.clone(),
            header_provider: self.header_provider.clone(),
            cert_file: self.cert_file.clone(),
            key_file: self.key_file.clone(),
            ssl_ca_cert: self.ssl_ca_cert.clone(),
            assert_hostname: self.assert_hostname,
        }
    }
}

impl RestNamespaceBuilder {
    /// Default delimiter for object identifiers
    const DEFAULT_DELIMITER: &'static str = "$";

    /// Create a new RestNamespaceBuilder with the specified URI.
    ///
    /// # Arguments
    ///
    /// * `uri` - Base URI for the REST API
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            delimiter: Self::DEFAULT_DELIMITER.to_string(),
            header_provider: None,
            cert_file: None,
            key_file: None,
            ssl_ca_cert: None,
            assert_hostname: true,
        }
    }

    /// Create a RestNamespaceBuilder from properties HashMap.
    ///
    /// This method parses a properties map into builder configuration.
    /// It expects:
    /// - `uri`: The base URI for the REST API (required)
    /// - `delimiter`: Delimiter for object identifiers (optional, defaults to ".")
    /// - `header.*`: Additional headers (optional, prefix will be stripped)
    /// - `tls.cert_file`: Path to client certificate file (optional)
    /// - `tls.key_file`: Path to client private key file (optional)
    /// - `tls.ssl_ca_cert`: Path to CA certificate file (optional)
    /// - `tls.assert_hostname`: Whether to verify hostname (optional, defaults to true)
    ///
    /// # Arguments
    ///
    /// * `properties` - Configuration properties
    ///
    /// # Returns
    ///
    /// Returns a `RestNamespaceBuilder` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the `uri` property is missing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lance_namespace_impls::RestNamespaceBuilder;
    /// # use std::collections::HashMap;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut properties = HashMap::new();
    /// properties.insert("uri".to_string(), "http://localhost:8080".to_string());
    /// properties.insert("delimiter".to_string(), "/".to_string());
    /// properties.insert("header.Authorization".to_string(), "Bearer token".to_string());
    ///
    /// let namespace = RestNamespaceBuilder::from_properties(properties)?
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_properties(properties: HashMap<String, String>) -> Result<Self> {
        // Extract URI (required)
        let uri = properties
            .get("uri")
            .cloned()
            .ok_or_else(|| Error::Namespace {
                source: "Missing required property 'uri' for REST namespace".into(),
                location: snafu::location!(),
            })?;

        // Extract delimiter (optional)
        let delimiter = properties
            .get("delimiter")
            .cloned()
            .unwrap_or_else(|| Self::DEFAULT_DELIMITER.to_string());

        // Extract headers (properties prefixed with "header.")
        let mut headers = HashMap::new();
        for (key, value) in &properties {
            if let Some(header_name) = key.strip_prefix("header.") {
                headers.insert(header_name.to_string(), value.clone());
            }
        }

        // Create header provider if any headers were specified
        let header_provider: Option<Arc<dyn HeaderProvider>> = if headers.is_empty() {
            None
        } else {
            Some(Arc::new(StaticHeaderProvider::new(headers)))
        };

        // Extract TLS options
        let cert_file = properties.get("tls.cert_file").cloned();
        let key_file = properties.get("tls.key_file").cloned();
        let ssl_ca_cert = properties.get("tls.ssl_ca_cert").cloned();
        let assert_hostname = properties
            .get("tls.assert_hostname")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        Ok(Self {
            uri,
            delimiter,
            header_provider,
            cert_file,
            key_file,
            ssl_ca_cert,
            assert_hostname,
        })
    }

    /// Set the delimiter for object identifiers.
    ///
    /// # Arguments
    ///
    /// * `delimiter` - Delimiter string (e.g., ".", "/")
    pub fn delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.delimiter = delimiter.into();
        self
    }

    /// Set a dynamic header provider for HTTP requests.
    ///
    /// The header provider will be called before each API request to get
    /// the headers to send. Use this for dynamic headers like auth tokens
    /// that need to be refreshed periodically.
    ///
    /// # Arguments
    ///
    /// * `provider` - The header provider implementation
    pub fn header_provider(mut self, provider: Arc<dyn HeaderProvider>) -> Self {
        self.header_provider = Some(provider);
        self
    }

    /// Add a custom header to the HTTP requests.
    ///
    /// This is a convenience method that creates or updates a [`StaticHeaderProvider`].
    /// For dynamic headers (e.g., auth tokens that need refresh), use [`header_provider`]
    /// instead.
    ///
    /// # Arguments
    ///
    /// * `name` - Header name
    /// * `value` - Header value
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        let value = value.into();

        // Get existing headers or create empty map
        let mut headers = self.extract_static_headers();
        headers.insert(name, value);
        self.header_provider = Some(Arc::new(StaticHeaderProvider::new(headers)));
        self
    }

    /// Add multiple custom headers to the HTTP requests.
    ///
    /// This is a convenience method that creates or updates a [`StaticHeaderProvider`].
    /// For dynamic headers (e.g., auth tokens that need refresh), use [`header_provider`]
    /// instead.
    ///
    /// # Arguments
    ///
    /// * `headers` - HashMap of headers to add
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        let mut existing = self.extract_static_headers();
        existing.extend(headers);
        self.header_provider = Some(Arc::new(StaticHeaderProvider::new(existing)));
        self
    }

    /// Extract headers from an existing StaticHeaderProvider, or return empty map.
    fn extract_static_headers(&self) -> HashMap<String, String> {
        self.header_provider
            .as_ref()
            .and_then(|p| {
                // Try to downcast to StaticHeaderProvider using as_any()
                p.as_any()
                    .downcast_ref::<StaticHeaderProvider>()
                    .map(|sp| sp.headers.clone())
            })
            .unwrap_or_default()
    }

    /// Set the client certificate file for mTLS.
    ///
    /// # Arguments
    ///
    /// * `cert_file` - Path to the certificate file (PEM format)
    pub fn cert_file(mut self, cert_file: impl Into<String>) -> Self {
        self.cert_file = Some(cert_file.into());
        self
    }

    /// Set the client private key file for mTLS.
    ///
    /// # Arguments
    ///
    /// * `key_file` - Path to the private key file (PEM format)
    pub fn key_file(mut self, key_file: impl Into<String>) -> Self {
        self.key_file = Some(key_file.into());
        self
    }

    /// Set the CA certificate file for server verification.
    ///
    /// # Arguments
    ///
    /// * `ssl_ca_cert` - Path to the CA certificate file (PEM format)
    pub fn ssl_ca_cert(mut self, ssl_ca_cert: impl Into<String>) -> Self {
        self.ssl_ca_cert = Some(ssl_ca_cert.into());
        self
    }

    /// Set whether to verify the hostname in the server's certificate.
    ///
    /// # Arguments
    ///
    /// * `assert_hostname` - Whether to verify hostname
    pub fn assert_hostname(mut self, assert_hostname: bool) -> Self {
        self.assert_hostname = assert_hostname;
        self
    }

    /// Build the RestNamespace.
    ///
    /// # Returns
    ///
    /// Returns a `RestNamespace` instance.
    pub fn build(self) -> RestNamespace {
        let mut client_builder = reqwest::Client::builder();

        // Configure mTLS if certificate and key files are provided
        if let (Some(cert_file), Some(key_file)) = (&self.cert_file, &self.key_file) {
            if let (Ok(cert), Ok(key)) = (std::fs::read(cert_file), std::fs::read(key_file)) {
                if let Ok(identity) = reqwest::Identity::from_pem(&[&cert[..], &key[..]].concat()) {
                    client_builder = client_builder.identity(identity);
                }
            }
        }

        // Load CA certificate for server verification
        if let Some(ca_cert_file) = &self.ssl_ca_cert {
            if let Ok(ca_cert) = std::fs::read(ca_cert_file) {
                if let Ok(ca_cert) = reqwest::Certificate::from_pem(&ca_cert) {
                    client_builder = client_builder.add_root_certificate(ca_cert);
                }
            }
        }

        // Configure hostname verification
        client_builder = client_builder.danger_accept_invalid_hostnames(!self.assert_hostname);

        let client = client_builder
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        // Create configuration with middleware if header provider is set
        let mut config = Configuration::new();
        config.client = client;
        config.base_path = self.uri.clone();

        if let Some(provider) = self.header_provider {
            config.request_middleware = Some(Arc::new(HeaderMiddleware { provider }));
        }

        RestNamespace {
            config,
            delimiter: self.delimiter,
        }
    }
}

/// Convert an object identifier (list of strings) to a delimited string
fn object_id_str(id: &Option<Vec<String>>, delimiter: &str) -> Result<String> {
    match id {
        Some(id_parts) if !id_parts.is_empty() => Ok(id_parts.join(delimiter)),
        Some(_) => Ok(delimiter.to_string()),
        None => Err(Error::Namespace {
            source: "Object ID is required".into(),
            location: snafu::location!(),
        }),
    }
}

/// Convert API error to lance core error
fn convert_api_error<T: std::fmt::Debug>(err: lance_namespace::apis::Error<T>) -> Error {
    use lance_namespace::apis::Error as ApiError;
    match err {
        ApiError::Reqwest(e) => Error::IO {
            source: box_error(e),
            location: snafu::location!(),
        },
        ApiError::Serde(e) => Error::Namespace {
            source: format!("Serialization error: {}", e).into(),
            location: snafu::location!(),
        },
        ApiError::Io(e) => Error::IO {
            source: box_error(e),
            location: snafu::location!(),
        },
        ApiError::ResponseError(e) => Error::Namespace {
            source: format!("Response error: {:?}", e).into(),
            location: snafu::location!(),
        },
    }
}

/// REST implementation of Lance Namespace
///
/// # Examples
///
/// ```no_run
/// # use lance_namespace_impls::RestNamespaceBuilder;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Use the builder to create a namespace
/// let namespace = RestNamespaceBuilder::new("http://localhost:8080")
///     .build();
/// # Ok(())
/// # }
/// ```
pub struct RestNamespace {
    config: Configuration,
    delimiter: String,
}

impl Clone for RestNamespace {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            delimiter: self.delimiter.clone(),
        }
    }
}

impl std::fmt::Debug for RestNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.namespace_id())
    }
}

impl std::fmt::Display for RestNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.namespace_id())
    }
}

impl RestNamespace {
    /// Create a new REST namespace with custom configuration (for testing)
    #[cfg(test)]
    pub fn with_configuration(delimiter: String, config: Configuration) -> Self {
        Self { config, delimiter }
    }

    /// Get the base endpoint URL for this namespace
    pub fn endpoint(&self) -> &str {
        &self.config.base_path
    }
}

#[async_trait]
impl LanceNamespace for RestNamespace {
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        namespace_api::list_namespaces(
            &self.config,
            &id,
            Some(&self.delimiter),
            request.page_token.as_deref(),
            request.limit,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        namespace_api::describe_namespace(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        namespace_api::create_namespace(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        namespace_api::drop_namespace(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        namespace_api::namespace_exists(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::list_tables(
            &self.config,
            &id,
            Some(&self.delimiter),
            request.page_token.as_deref(),
            request.limit,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn describe_table(&self, request: DescribeTableRequest) -> Result<DescribeTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::describe_table(
            &self.config,
            &id,
            request.clone(),
            Some(&self.delimiter),
            request.with_table_uri,
            request.load_detailed_metadata,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<RegisterTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::register_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::table_exists(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<DropTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::drop_table(&self.config, &id, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn deregister_table(
        &self,
        request: DeregisterTableRequest,
    ) -> Result<DeregisterTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::deregister_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn count_table_rows(&self, request: CountTableRowsRequest) -> Result<i64> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::count_table_rows(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        request_data: Bytes,
    ) -> Result<CreateTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::create_table(
            &self.config,
            &id,
            request_data.to_vec(),
            Some(&self.delimiter),
            request.mode.as_deref(),
        )
        .await
        .map_err(convert_api_error)
    }

    async fn create_empty_table(
        &self,
        request: CreateEmptyTableRequest,
    ) -> Result<CreateEmptyTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::create_empty_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn insert_into_table(
        &self,
        request: InsertIntoTableRequest,
        request_data: Bytes,
    ) -> Result<InsertIntoTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::insert_into_table(
            &self.config,
            &id,
            request_data.to_vec(),
            Some(&self.delimiter),
            request.mode.as_deref(),
        )
        .await
        .map_err(convert_api_error)
    }

    async fn merge_insert_into_table(
        &self,
        request: MergeInsertIntoTableRequest,
        request_data: Bytes,
    ) -> Result<MergeInsertIntoTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        let on = request.on.as_deref().ok_or_else(|| Error::Namespace {
            source: "'on' field is required for merge insert".into(),
            location: snafu::location!(),
        })?;

        table_api::merge_insert_into_table(
            &self.config,
            &id,
            on,
            request_data.to_vec(),
            Some(&self.delimiter),
            request.when_matched_update_all,
            request.when_matched_update_all_filt.as_deref(),
            request.when_not_matched_insert_all,
            request.when_not_matched_by_source_delete,
            request.when_not_matched_by_source_delete_filt.as_deref(),
            request.timeout.as_deref(),
            request.use_index,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn update_table(&self, request: UpdateTableRequest) -> Result<UpdateTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::update_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn delete_from_table(
        &self,
        request: DeleteFromTableRequest,
    ) -> Result<DeleteFromTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::delete_from_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn query_table(&self, request: QueryTableRequest) -> Result<Bytes> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        let response = table_api::query_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)?;

        // Convert response to bytes
        let bytes = response.bytes().await.map_err(|e| Error::IO {
            source: box_error(e),
            location: snafu::location!(),
        })?;

        Ok(bytes)
    }

    async fn create_table_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> Result<CreateTableIndexResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::create_table_index(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn list_table_indices(
        &self,
        request: ListTableIndicesRequest,
    ) -> Result<ListTableIndicesResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::list_table_indices(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn describe_table_index_stats(
        &self,
        request: DescribeTableIndexStatsRequest,
    ) -> Result<DescribeTableIndexStatsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        // Note: The index_name parameter seems to be missing from the request structure
        // This might need to be adjusted based on the actual API
        let index_name = ""; // This should come from somewhere in the request

        table_api::describe_table_index_stats(
            &self.config,
            &id,
            index_name,
            request,
            Some(&self.delimiter),
        )
        .await
        .map_err(convert_api_error)
    }

    async fn describe_transaction(
        &self,
        request: DescribeTransactionRequest,
    ) -> Result<DescribeTransactionResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        transaction_api::describe_transaction(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn alter_transaction(
        &self,
        request: AlterTransactionRequest,
    ) -> Result<AlterTransactionResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        transaction_api::alter_transaction(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn create_table_scalar_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> Result<CreateTableScalarIndexResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::create_table_scalar_index(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn drop_table_index(
        &self,
        request: DropTableIndexRequest,
    ) -> Result<DropTableIndexResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        let index_name = request.index_name.as_deref().unwrap_or("");

        table_api::drop_table_index(&self.config, &id, index_name, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn list_all_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        table_api::list_all_tables(
            &self.config,
            Some(&self.delimiter),
            request.page_token.as_deref(),
            request.limit,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn restore_table(&self, request: RestoreTableRequest) -> Result<RestoreTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::restore_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<RenameTableResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::rename_table(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> Result<ListTableVersionsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::list_table_versions(
            &self.config,
            &id,
            Some(&self.delimiter),
            request.page_token.as_deref(),
            request.limit,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn update_table_schema_metadata(
        &self,
        request: UpdateTableSchemaMetadataRequest,
    ) -> Result<UpdateTableSchemaMetadataResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        let metadata = request.metadata.unwrap_or_default();

        let result = table_api::update_table_schema_metadata(
            &self.config,
            &id,
            metadata,
            Some(&self.delimiter),
        )
        .await
        .map_err(convert_api_error)?;

        Ok(UpdateTableSchemaMetadataResponse {
            metadata: Some(result),
            ..Default::default()
        })
    }

    async fn get_table_stats(
        &self,
        request: GetTableStatsRequest,
    ) -> Result<GetTableStatsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::get_table_stats(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn explain_table_query_plan(
        &self,
        request: ExplainTableQueryPlanRequest,
    ) -> Result<String> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::explain_table_query_plan(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn analyze_table_query_plan(
        &self,
        request: AnalyzeTableQueryPlanRequest,
    ) -> Result<String> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::analyze_table_query_plan(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn alter_table_add_columns(
        &self,
        request: AlterTableAddColumnsRequest,
    ) -> Result<AlterTableAddColumnsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::alter_table_add_columns(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn alter_table_alter_columns(
        &self,
        request: AlterTableAlterColumnsRequest,
    ) -> Result<AlterTableAlterColumnsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::alter_table_alter_columns(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn alter_table_drop_columns(
        &self,
        request: AlterTableDropColumnsRequest,
    ) -> Result<AlterTableDropColumnsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        table_api::alter_table_drop_columns(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn list_table_tags(
        &self,
        request: ListTableTagsRequest,
    ) -> Result<ListTableTagsResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        tag_api::list_table_tags(
            &self.config,
            &id,
            Some(&self.delimiter),
            request.page_token.as_deref(),
            request.limit,
        )
        .await
        .map_err(convert_api_error)
    }

    async fn get_table_tag_version(
        &self,
        request: GetTableTagVersionRequest,
    ) -> Result<GetTableTagVersionResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        tag_api::get_table_tag_version(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn create_table_tag(
        &self,
        request: CreateTableTagRequest,
    ) -> Result<CreateTableTagResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        tag_api::create_table_tag(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn delete_table_tag(
        &self,
        request: DeleteTableTagRequest,
    ) -> Result<DeleteTableTagResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        tag_api::delete_table_tag(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    async fn update_table_tag(
        &self,
        request: UpdateTableTagRequest,
    ) -> Result<UpdateTableTagResponse> {
        let id = object_id_str(&request.id, &self.delimiter)?;

        tag_api::update_table_tag(&self.config, &id, request, Some(&self.delimiter))
            .await
            .map_err(convert_api_error)
    }

    fn namespace_id(&self) -> String {
        format!(
            "RestNamespace {{ endpoint: {:?}, delimiter: {:?} }}",
            self.config.base_path, self.delimiter
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_rest_namespace_creation() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "http://example.com".to_string());
        properties.insert("delimiter".to_string(), "/".to_string());
        properties.insert(
            "header.Authorization".to_string(),
            "Bearer token".to_string(),
        );
        properties.insert("header.X-Custom".to_string(), "value".to_string());

        let _namespace = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder")
            .build();

        // Successfully created the namespace - test passes if no panic
    }

    #[tokio::test]
    async fn test_custom_headers_are_sent() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create mock that expects custom headers
        Mock::given(method("GET"))
            .and(path("/v1/namespace/test/list"))
            .and(wiremock::matchers::header(
                "Authorization",
                "Bearer test-token",
            ))
            .and(wiremock::matchers::header(
                "X-Custom-Header",
                "custom-value",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": []
            })))
            .mount(&mock_server)
            .await;

        // Create namespace with custom headers
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), mock_server.uri());
        properties.insert(
            "header.Authorization".to_string(),
            "Bearer test-token".to_string(),
        );
        properties.insert(
            "header.X-Custom-Header".to_string(),
            "custom-value".to_string(),
        );

        let namespace = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder")
            .build();

        let request = ListNamespacesRequest {
            id: Some(vec!["test".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };

        let result = namespace.list_namespaces(request).await;

        // Should succeed, meaning headers were sent correctly
        assert!(result.is_ok());
    }

    #[test]
    fn test_default_configuration() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "http://localhost:8080".to_string());
        let _namespace = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder")
            .build();

        // The default delimiter should be "$" - test passes if no panic
    }

    #[test]
    fn test_with_custom_uri() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "https://api.example.com/v1".to_string());

        let _namespace = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder")
            .build();
        // Test passes if no panic
    }

    #[test]
    fn test_tls_config_parsing() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "https://api.example.com".to_string());
        properties.insert("tls.cert_file".to_string(), "/path/to/cert.pem".to_string());
        properties.insert("tls.key_file".to_string(), "/path/to/key.pem".to_string());
        properties.insert("tls.ssl_ca_cert".to_string(), "/path/to/ca.pem".to_string());
        properties.insert("tls.assert_hostname".to_string(), "true".to_string());

        let builder = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder");
        assert_eq!(builder.cert_file, Some("/path/to/cert.pem".to_string()));
        assert_eq!(builder.key_file, Some("/path/to/key.pem".to_string()));
        assert_eq!(builder.ssl_ca_cert, Some("/path/to/ca.pem".to_string()));
        assert!(builder.assert_hostname);
    }

    #[test]
    fn test_tls_config_default_assert_hostname() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "https://api.example.com".to_string());
        properties.insert("tls.cert_file".to_string(), "/path/to/cert.pem".to_string());
        properties.insert("tls.key_file".to_string(), "/path/to/key.pem".to_string());

        let builder = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder");
        // Default should be true
        assert!(builder.assert_hostname);
    }

    #[test]
    fn test_tls_config_disable_hostname_verification() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "https://api.example.com".to_string());
        properties.insert("tls.cert_file".to_string(), "/path/to/cert.pem".to_string());
        properties.insert("tls.key_file".to_string(), "/path/to/key.pem".to_string());
        properties.insert("tls.assert_hostname".to_string(), "false".to_string());

        let builder = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder");
        assert!(!builder.assert_hostname);
    }

    #[test]
    fn test_namespace_creation_with_tls_config() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "https://api.example.com".to_string());
        properties.insert(
            "tls.cert_file".to_string(),
            "/nonexistent/cert.pem".to_string(),
        );
        properties.insert(
            "tls.key_file".to_string(),
            "/nonexistent/key.pem".to_string(),
        );
        properties.insert(
            "tls.ssl_ca_cert".to_string(),
            "/nonexistent/ca.pem".to_string(),
        );

        // Should not panic even with nonexistent files (they're just ignored)
        let _namespace = RestNamespaceBuilder::from_properties(properties)
            .expect("Failed to create namespace builder")
            .build();
    }

    #[tokio::test]
    async fn test_list_namespaces_success() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create mock response
        Mock::given(method("GET"))
            .and(path("/v1/namespace/test/list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": [
                    "namespace1",
                    "namespace2"
                ]
            })))
            .mount(&mock_server)
            .await;

        // Create namespace with mock server URL
        let mut config = Configuration::new();
        config.base_path = mock_server.uri();

        let namespace = RestNamespace::with_configuration("$".to_string(), config);

        let request = ListNamespacesRequest {
            id: Some(vec!["test".to_string()]),
            page_token: None,
            limit: Some(10),
            ..Default::default()
        };

        let result = namespace.list_namespaces(request).await;

        // Should succeed with mock server
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.namespaces.len(), 2);
        assert_eq!(response.namespaces[0], "namespace1");
        assert_eq!(response.namespaces[1], "namespace2");
    }

    #[tokio::test]
    async fn test_list_namespaces_error() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create mock error response
        Mock::given(method("GET"))
            .and(path("/v1/namespace/test/list"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": {
                    "message": "Namespace not found",
                    "type": "NamespaceNotFoundException"
                }
            })))
            .mount(&mock_server)
            .await;

        // Create namespace with mock server URL
        let mut config = Configuration::new();
        config.base_path = mock_server.uri();

        let namespace = RestNamespace::with_configuration("$".to_string(), config);

        let request = ListNamespacesRequest {
            id: Some(vec!["test".to_string()]),
            page_token: None,
            limit: Some(10),
            ..Default::default()
        };

        let result = namespace.list_namespaces(request).await;

        // Should return an error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_namespace_success() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create mock response
        let path_str = "/v1/namespace/test$newnamespace/create".replace("$", "%24");
        Mock::given(method("POST"))
            .and(path(path_str.as_str()))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "namespace": {
                    "identifier": ["test", "newnamespace"],
                    "properties": {}
                }
            })))
            .mount(&mock_server)
            .await;

        // Create namespace with mock server URL
        let mut config = Configuration::new();
        config.base_path = mock_server.uri();

        let namespace = RestNamespace::with_configuration("$".to_string(), config);

        let request = CreateNamespaceRequest {
            id: Some(vec!["test".to_string(), "newnamespace".to_string()]),
            properties: None,
            mode: None,
            ..Default::default()
        };

        let result = namespace.create_namespace(request).await;

        // Should succeed with mock server
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_create_table_success() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create mock response
        let path_str = "/v1/table/test$namespace$table/create".replace("$", "%24");
        Mock::given(method("POST"))
            .and(path(path_str.as_str()))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "table": {
                    "identifier": ["test", "namespace", "table"],
                    "location": "/path/to/table",
                    "version": 1
                }
            })))
            .mount(&mock_server)
            .await;

        // Create namespace with mock server URL
        let mut config = Configuration::new();
        config.base_path = mock_server.uri();

        let namespace = RestNamespace::with_configuration("$".to_string(), config);

        let request = CreateTableRequest {
            id: Some(vec![
                "test".to_string(),
                "namespace".to_string(),
                "table".to_string(),
            ]),
            mode: Some("Create".to_string()),
            ..Default::default()
        };

        let data = Bytes::from("arrow data here");
        let result = namespace.create_table(request, data).await;

        // Should succeed with mock server
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_insert_into_table_success() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Create mock response
        let path_str = "/v1/table/test$namespace$table/insert".replace("$", "%24");
        Mock::given(method("POST"))
            .and(path(path_str.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "transaction_id": "txn-123"
            })))
            .mount(&mock_server)
            .await;

        // Create namespace with mock server URL
        let mut config = Configuration::new();
        config.base_path = mock_server.uri();

        let namespace = RestNamespace::with_configuration("$".to_string(), config);

        let request = InsertIntoTableRequest {
            id: Some(vec![
                "test".to_string(),
                "namespace".to_string(),
                "table".to_string(),
            ]),
            mode: Some("Append".to_string()),
            ..Default::default()
        };

        let data = Bytes::from("arrow data here");
        let result = namespace.insert_into_table(request, data).await;

        // Should succeed with mock server
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.transaction_id, Some("txn-123".to_string()));
    }

    // ==================== HeaderProvider Tests ====================

    #[test]
    fn test_static_header_provider_new() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer token".to_string());
        headers.insert("X-Custom".to_string(), "value".to_string());

        let provider = StaticHeaderProvider::new(headers.clone());
        assert_eq!(provider.headers, headers);
    }

    #[test]
    fn test_static_header_provider_from_iter() {
        let provider = StaticHeaderProvider::from_iter([
            ("Authorization", "Bearer token"),
            ("X-Custom", "value"),
        ]);

        assert_eq!(provider.headers.len(), 2);
        assert_eq!(
            provider.headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
        assert_eq!(provider.headers.get("X-Custom"), Some(&"value".to_string()));
    }

    #[tokio::test]
    async fn test_static_header_provider_get_headers() {
        let provider = StaticHeaderProvider::from_iter([("Authorization", "Bearer token")]);

        let headers = provider.get_headers().await.unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
    }

    #[test]
    fn test_builder_header_provider_method() {
        #[derive(Debug)]
        struct TestProvider;

        #[async_trait]
        impl HeaderProvider for TestProvider {
            async fn get_headers(&self) -> Result<HashMap<String, String>> {
                let mut headers = HashMap::new();
                headers.insert("X-Test".to_string(), "test-value".to_string());
                Ok(headers)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        let provider = Arc::new(TestProvider);
        let builder = RestNamespaceBuilder::new("http://example.com").header_provider(provider);

        assert!(builder.header_provider.is_some());
    }

    #[test]
    fn test_builder_header_method_creates_static_provider() {
        let builder = RestNamespaceBuilder::new("http://example.com")
            .header("Authorization", "Bearer token")
            .header("X-Custom", "value");

        assert!(builder.header_provider.is_some());

        // Verify headers were accumulated
        let headers = builder.extract_static_headers();
        assert_eq!(headers.len(), 2);
    }

    #[test]
    fn test_builder_headers_method_creates_static_provider() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer token".to_string());
        headers.insert("X-Custom".to_string(), "value".to_string());

        let builder = RestNamespaceBuilder::new("http://example.com").headers(headers);

        assert!(builder.header_provider.is_some());
    }

    #[test]
    fn test_from_properties_creates_static_provider() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "http://example.com".to_string());
        properties.insert(
            "header.Authorization".to_string(),
            "Bearer token".to_string(),
        );
        properties.insert("header.X-Custom".to_string(), "value".to_string());

        let builder =
            RestNamespaceBuilder::from_properties(properties).expect("Failed to create builder");

        assert!(builder.header_provider.is_some());

        let headers = builder.extract_static_headers();
        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
    }

    #[test]
    fn test_from_properties_no_headers() {
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), "http://example.com".to_string());

        let builder =
            RestNamespaceBuilder::from_properties(properties).expect("Failed to create builder");

        // No headers specified, so no provider
        assert!(builder.header_provider.is_none());
    }

    #[tokio::test]
    async fn test_dynamic_header_provider() {
        use std::sync::atomic::{AtomicU32, Ordering};

        // A provider that returns different headers each time
        #[derive(Debug)]
        struct CountingProvider {
            counter: AtomicU32,
        }

        #[async_trait]
        impl HeaderProvider for CountingProvider {
            async fn get_headers(&self) -> Result<HashMap<String, String>> {
                let count = self.counter.fetch_add(1, Ordering::SeqCst);
                let mut headers = HashMap::new();
                headers.insert("X-Request-Count".to_string(), count.to_string());
                Ok(headers)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        let provider = Arc::new(CountingProvider {
            counter: AtomicU32::new(0),
        });

        // Verify the provider returns different values
        let headers1 = provider.get_headers().await.unwrap();
        let headers2 = provider.get_headers().await.unwrap();

        assert_eq!(headers1.get("X-Request-Count"), Some(&"0".to_string()));
        assert_eq!(headers2.get("X-Request-Count"), Some(&"1".to_string()));
    }

    #[tokio::test]
    async fn test_dynamic_header_provider_with_mock_server() {
        use std::sync::atomic::{AtomicU32, Ordering};

        // Start a mock server
        let mock_server = MockServer::start().await;

        // Provider that increments a counter in the header
        #[derive(Debug)]
        struct CountingProvider {
            counter: AtomicU32,
        }

        #[async_trait]
        impl HeaderProvider for CountingProvider {
            async fn get_headers(&self) -> Result<HashMap<String, String>> {
                let count = self.counter.fetch_add(1, Ordering::SeqCst);
                let mut headers = HashMap::new();
                headers.insert("X-Request-Count".to_string(), count.to_string());
                Ok(headers)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        // Create mock that expects the header
        Mock::given(method("GET"))
            .and(path("/v1/namespace/test/list"))
            .and(wiremock::matchers::header("X-Request-Count", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": ["ns1"]
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/namespace/test/list"))
            .and(wiremock::matchers::header("X-Request-Count", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": ["ns2"]
            })))
            .mount(&mock_server)
            .await;

        let provider = Arc::new(CountingProvider {
            counter: AtomicU32::new(0),
        });

        let namespace = RestNamespaceBuilder::new(mock_server.uri())
            .header_provider(provider)
            .build();

        let request1 = ListNamespacesRequest {
            id: Some(vec!["test".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };

        let request2 = ListNamespacesRequest {
            id: Some(vec!["test".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };

        // First request should have X-Request-Count: 0
        let result1 = namespace.list_namespaces(request1).await;
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap().namespaces, vec!["ns1"]);

        // Second request should have X-Request-Count: 1
        let result2 = namespace.list_namespaces(request2).await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap().namespaces, vec!["ns2"]);
    }
}
