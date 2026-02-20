use forge_kit::metadata::{MetadataManager, MetadataSource};
use forge_kit::parser::{self, ParseError, Span, ValidationConfig};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types;
use tower_lsp::{Client, LanguageServer};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetadataUrlConfig {
    extension: String,
    functions: Option<String>,
    enums: Option<String>,
    events: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ForgeConfig {
    metadata_urls: Option<Vec<MetadataUrlConfig>>,
}

pub struct ForgeLanguageServer {
    client: Client,
    metadata: Arc<MetadataManager>,
    documents: RwLock<HashMap<lsp_types::Url, String>>,
    pending_metadata: Mutex<Option<Vec<MetadataUrlConfig>>>,
}

impl ForgeLanguageServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            metadata: Arc::new(MetadataManager::new()),
            documents: RwLock::new(HashMap::new()),
            pending_metadata: Mutex::new(None),
        }
    }

    async fn publish_diagnostics_for(&self, uri: lsp_types::Url, text: String) {
        let diagnostics = compute_diagnostics(&uri, &text, self.metadata.clone());

        self.client
            .publish_diagnostics(uri, diagnostics, None)
            .await;
    }

    async fn scan_workspace(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        // Search for all .js, .ts, .forge files
        // Excluding node_modules, dist, out
        tokio::spawn(async move {
            let walker = ignore::WalkBuilder::new(".")
                .hidden(false)
                .git_ignore(true)
                .build();

            for result in walker {
                if let Ok(entry) = result {
                    if entry.file_type().unwrap().is_file() {
                        let path = entry.path();
                        let extension = path.extension().and_then(|s| s.to_str());
                        if matches!(extension, Some("js" | "ts" | "forge")) {
                            let path_str = path.to_string_lossy();
                            if !path_str.contains("node_modules")
                                && !path_str.contains("dist")
                                && !path_str.contains("out")
                            {
                                if let Ok(abs_path) = std::fs::canonicalize(path) {
                                    if let Ok(uri) = lsp_types::Url::from_file_path(abs_path) {
                                        let _ = tx.send(uri).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        while let Some(uri) = rx.recv().await {
            if let Ok(text) = std::fs::read_to_string(uri.to_file_path().unwrap()) {
                self.publish_diagnostics_for(uri, text).await;
            }
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for ForgeLanguageServer {
    async fn initialize(
        &self,
        params: lsp_types::InitializeParams,
    ) -> Result<lsp_types::InitializeResult> {
        if let Some(options) = params.initialization_options {
            if let Ok(config) = serde_json::from_value::<ForgeConfig>(options) {
                if let Some(urls) = config.metadata_urls {
                    let mut pending = self.pending_metadata.lock().await;
                    *pending = Some(urls);
                }
            }
        }

        Ok(lsp_types::InitializeResult {
            capabilities: lsp_types::ServerCapabilities {
                text_document_sync: Some(lsp_types::TextDocumentSyncCapability::Kind(
                    lsp_types::TextDocumentSyncKind::FULL,
                )),
                ..lsp_types::ServerCapabilities::default()
            },
            ..lsp_types::InitializeResult::default()
        })
    }

    async fn initialized(&self, _: lsp_types::InitializedParams) {
        self.client
            .log_message(lsp_types::MessageType::INFO, "ForgeLSP initialized")
            .await;

        // Process pending metadata URLs
        let urls = {
            let mut pending = self.pending_metadata.lock().await;
            pending.take()
        };

        if let Some(urls) = urls {
            for url_cfg in urls {
                let mut source = MetadataSource::new(url_cfg.extension);
                if let Some(u) = url_cfg.functions {
                    source = source.with_functions(u);
                }
                if let Some(u) = url_cfg.enums {
                    source = source.with_enums(u);
                }
                if let Some(u) = url_cfg.events {
                    source = source.with_events(u);
                }
                self.metadata.add_source(source);
            }

            // Trigger fetch
            match self.metadata.fetch_all().await {
                Ok(stats) => {
                    self.client
                        .log_message(lsp_types::MessageType::INFO, format!("Metadata: {}", stats))
                        .await;
                }
                Err(e) => {
                    self.client
                        .log_message(
                            lsp_types::MessageType::ERROR,
                            format!("Failed to fetch metadata: {}", e),
                        )
                        .await;
                }
            }
        }

        // Workspace scanning
        self.scan_workspace().await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: lsp_types::DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let text = params.text_document.text;

        {
            let mut docs = self.documents.write().await;
            docs.insert(uri.clone(), text.clone());
        }

        self.publish_diagnostics_for(uri, text).await;
    }

    async fn did_change(&self, params: lsp_types::DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;

        let text = match params.content_changes.into_iter().last().map(|c| c.text) {
            Some(t) => t,
            None => return,
        };

        {
            let mut docs = self.documents.write().await;
            docs.insert(uri.clone(), text.clone());
        }

        self.publish_diagnostics_for(uri, text).await;
    }

    async fn did_close(&self, params: lsp_types::DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;

        {
            let mut docs = self.documents.write().await;
            docs.remove(&uri);
        }

        self.client.publish_diagnostics(uri, Vec::new(), None).await;
    }
}

fn compute_diagnostics(
    uri: &lsp_types::Url,
    text: &str,
    metadata: Arc<MetadataManager>,
) -> Vec<lsp_types::Diagnostic> {
    let config = ValidationConfig::strict();

    let is_js_ts = uri.path().ends_with(".js") || uri.path().ends_with(".ts");

    let (_, errors) = if is_js_ts {
        parser::parse_with_validation(text, config, metadata)
    } else {
        parser::parse_forge_script_with_validation(text, config, metadata)
    };

    errors
        .into_iter()
        .map(|e| parse_error_to_diagnostic(text, e))
        .collect()
}

fn parse_error_to_diagnostic(text: &str, error: ParseError) -> lsp_types::Diagnostic {
    lsp_types::Diagnostic {
        range: span_to_range(text, error.span),
        severity: Some(lsp_types::DiagnosticSeverity::ERROR),
        message: error.message,
        ..lsp_types::Diagnostic::default()
    }
}

fn span_to_range(text: &str, span: Span) -> lsp_types::Range {
    let start = byte_offset_to_position(text, span.start);
    let end = byte_offset_to_position(text, span.end);
    lsp_types::Range { start, end }
}

fn byte_offset_to_position(text: &str, byte_offset: usize) -> lsp_types::Position {
    let clamped = std::cmp::min(byte_offset, text.len());
    let prefix = &text[..clamped];

    let mut line: u32 = 0;
    let mut last_line_start: usize = 0;

    for (idx, ch) in prefix.char_indices() {
        if ch == '\n' {
            line += 1;
            last_line_start = idx + 1;
        }
    }

    let col = prefix[last_line_start..].chars().count() as u32;

    lsp_types::Position {
        line,
        character: col,
    }
}
