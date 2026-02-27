use forge_kit::metadata::MetadataManager;
use forge_kit::metadata::MetadataSource;
use forge_kit::parser::{self, AstNode, ParseError, Span, ValidationConfig};
use forge_kit::types::Function;
use notify::{Event as NotifyEvent, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types;
use tower_lsp::{Client, LanguageServer};

// ============================================================================
// Config types received from the VS Code extension
// ============================================================================

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
    custom_functions_path: Option<String>,
    custom_functions_json: Option<String>,
    cache_path: Option<String>,
}

// ============================================================================
// Parse Cache
// ============================================================================

struct CachedParse {
    ast: AstNode,
}

// ============================================================================
// Language server
// ============================================================================

pub struct ForgeLanguageServer {
    client: Client,
    metadata: Arc<MetadataManager>,
    documents: RwLock<HashMap<lsp_types::Url, String>>,
    parse_cache: RwLock<HashMap<lsp_types::Url, CachedParse>>,
    pending_config: Mutex<Option<ForgeConfig>>,
    _fs_watcher: Mutex<Option<RecommendedWatcher>>,
}

impl ForgeLanguageServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            metadata: Arc::new(MetadataManager::new()),
            documents: RwLock::new(HashMap::new()),
            parse_cache: RwLock::new(HashMap::new()),
            pending_config: Mutex::new(None),
            _fs_watcher: Mutex::new(None),
        }
    }

    // ========================================================================
    // Document refresh
    // ========================================================================

    async fn refresh(&self, uri: lsp_types::Url, text: String) {
        let is_js_ts = uri.path().ends_with(".js") || uri.path().ends_with(".ts");
        let config = ValidationConfig::strict();

        let (ast, errors) = if is_js_ts {
            parser::parse_with_validation(&text, config, self.metadata.clone())
        } else {
            parser::parse_forge_script_with_validation(&text, config, self.metadata.clone())
        };

        {
            let mut cache = self.parse_cache.write().await;
            cache.insert(uri.clone(), CachedParse { ast });
        }

        let diagnostics: Vec<lsp_types::Diagnostic> = errors
            .into_iter()
            .map(|e| parse_error_to_diagnostic(&text, e))
            .collect();
        self.client
            .publish_diagnostics(uri, diagnostics, None)
            .await;
    }

    /// Re-parse every currently-open document (called after metadata changes).
    async fn refresh_all_documents(&self) {
        let docs: Vec<(lsp_types::Url, String)> = {
            let map = self.documents.read().await;
            map.iter().map(|(u, t)| (u.clone(), t.clone())).collect()
        };
        for (uri, text) in docs {
            self.refresh(uri, text).await;
        }
    }

    // ========================================================================
    // Workspace scan
    // ========================================================================

    async fn scan_workspace(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let walker = ignore::WalkBuilder::new(".")
                .hidden(false)
                .git_ignore(true)
                .build();

            for result in walker {
                if let Ok(entry) = result {
                    if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
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
            if let Ok(path) = uri.to_file_path() {
                if let Ok(text) = std::fs::read_to_string(path) {
                    {
                        let mut docs = self.documents.write().await;
                        docs.insert(uri.clone(), text.clone());
                    }
                    self.refresh(uri, text).await;
                }
            }
        }
    }

    // ========================================================================
    // Custom function loading
    // ========================================================================

    async fn load_custom_functions(&self, config: &ForgeConfig) {
        if let Some(json_path) = &config.custom_functions_json {
            let path = PathBuf::from(json_path);
            match self.metadata.add_custom_functions_from_json_file(&path) {
                Ok(count) => {
                    self.client
                        .log_message(
                            lsp_types::MessageType::INFO,
                            format!(
                                "Loaded {} custom function(s) from JSON: {}",
                                count,
                                path.display()
                            ),
                        )
                        .await;
                }
                Err(e) => {
                    self.client
                        .log_message(
                            lsp_types::MessageType::WARNING,
                            format!(
                                "Failed to load custom-functions JSON at {}: {}",
                                path.display(),
                                e
                            ),
                        )
                        .await;
                }
            }
        }

        if let Some(folder_path) = &config.custom_functions_path {
            let folder = PathBuf::from(folder_path);
            if folder.exists() && folder.is_dir() {
                match self.metadata.generate_custom_functions_json(&folder) {
                    Ok(json) => match self.metadata.add_custom_functions_from_json(&json) {
                        Ok(count) => {
                            self.client
                                .log_message(
                                    lsp_types::MessageType::INFO,
                                    format!(
                                        "Loaded {} custom function(s) from folder: {}",
                                        count,
                                        folder.display()
                                    ),
                                )
                                .await;
                        }
                        Err(e) => {
                            self.client
                                .log_message(
                                    lsp_types::MessageType::WARNING,
                                    format!("Failed to register custom functions: {}", e),
                                )
                                .await;
                        }
                    },
                    Err(e) => {
                        self.client
                            .log_message(
                                lsp_types::MessageType::WARNING,
                                format!(
                                    "Failed to parse custom functions from {}: {}",
                                    folder.display(),
                                    e
                                ),
                            )
                            .await;
                    }
                }
            } else {
                self.client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!(
                            "custom_functions_path is not a directory: {}",
                            folder.display()
                        ),
                    )
                    .await;
            }
        }
    }

    // ========================================================================
    // Metadata disk cache
    // ========================================================================

    fn resolve_cache_path(config: &ForgeConfig) -> PathBuf {
        if let Some(p) = &config.cache_path {
            return PathBuf::from(p);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(cache_dir) = dirs::cache_dir() {
            return cache_dir.join("forgelsp").join("metadata.json");
        }
        std::env::temp_dir().join("forgelsp-metadata.json")
    }

    async fn try_load_cache(&self, cache_path: &PathBuf) -> bool {
        if !cache_path.exists() {
            return false;
        }
        match self.metadata.load_cache_from_file(cache_path) {
            Ok(()) => {
                self.client
                    .log_message(
                        lsp_types::MessageType::INFO,
                        format!(
                            "ForgeLSP: loaded metadata cache ({} functions) from {}",
                            self.metadata.function_count(),
                            cache_path.display()
                        ),
                    )
                    .await;
                true
            }
            Err(e) => {
                self.client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!(
                            "ForgeLSP: could not load metadata cache ({}): {}",
                            cache_path.display(),
                            e
                        ),
                    )
                    .await;
                false
            }
        }
    }

    async fn save_cache(&self, cache_path: &PathBuf) {
        if let Some(parent) = cache_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                self.client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!("ForgeLSP: cannot create cache directory: {}", e),
                    )
                    .await;
                return;
            }
        }
        match self.metadata.save_cache_to_file(cache_path) {
            Ok(()) => {
                self.client
                    .log_message(
                        lsp_types::MessageType::INFO,
                        format!(
                            "ForgeLSP: metadata cache saved ({} functions) to {}",
                            self.metadata.function_count(),
                            cache_path.display()
                        ),
                    )
                    .await;
            }
            Err(e) => {
                self.client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!("ForgeLSP: failed to save metadata cache: {}", e),
                    )
                    .await;
            }
        }
    }

    // ========================================================================
    // custom_functions_path file-system watcher
    // ========================================================================

    async fn start_custom_functions_watcher(&self, folder: PathBuf) {
        let metadata = Arc::clone(&self.metadata);
        let client = self.client.clone();
        let folder_clone = folder.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<notify::Result<NotifyEvent>>(32);

        let watcher_result = RecommendedWatcher::new(
            move |res| {
                let _ = tx.blocking_send(res);
            },
            notify::Config::default(),
        );

        let mut watcher = match watcher_result {
            Ok(w) => w,
            Err(e) => {
                client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!("ForgeLSP: cannot create file watcher: {}", e),
                    )
                    .await;
                return;
            }
        };

        if let Err(e) = watcher.watch(&folder, RecursiveMode::Recursive) {
            client
                .log_message(
                    lsp_types::MessageType::WARNING,
                    format!("ForgeLSP: cannot watch {}: {}", folder.display(), e),
                )
                .await;
            return;
        }

        *self._fs_watcher.lock().await = Some(watcher);

        client
            .log_message(
                lsp_types::MessageType::INFO,
                format!(
                    "ForgeLSP: watching custom-functions folder: {}",
                    folder.display()
                ),
            )
            .await;

        tokio::spawn(async move {
            while let Some(event_result) = rx.recv().await {
                let event = match event_result {
                    Ok(e) => e,
                    Err(e) => {
                        client
                            .log_message(
                                lsp_types::MessageType::WARNING,
                                format!("ForgeLSP: watcher error: {}", e),
                            )
                            .await;
                        continue;
                    }
                };

                let is_relevant = matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                ) && event.paths.iter().any(|p| {
                    p.extension()
                        .and_then(|e| e.to_str())
                        .map(|e| e == "js" || e == "ts")
                        .unwrap_or(false)
                });

                if !is_relevant {
                    continue;
                }

                match metadata.generate_custom_functions_json(&folder_clone) {
                    Ok(json) => {
                        match metadata.add_custom_functions_from_json(&json) {
                            Ok(count) => {
                                client
                                    .log_message(
                                        lsp_types::MessageType::INFO,
                                        format!("ForgeLSP: reloaded {} custom function(s)", count),
                                    )
                                    .await;
                            }
                            Err(e) => {
                                client
                                .log_message(
                                    lsp_types::MessageType::WARNING,
                                    format!("ForgeLSP: failed to register updated custom functions: {}", e),
                                )
                                .await;
                            }
                        }
                    }
                    Err(e) => {
                        client
                            .log_message(
                                lsp_types::MessageType::WARNING,
                                format!("ForgeLSP: failed to parse custom functions: {}", e),
                            )
                            .await;
                    }
                }
            }
        });
    }
}

// ============================================================================
// LanguageServer impl
// ============================================================================

#[tower_lsp::async_trait]
impl LanguageServer for ForgeLanguageServer {
    async fn initialize(
        &self,
        params: lsp_types::InitializeParams,
    ) -> Result<lsp_types::InitializeResult> {
        if let Some(options) = params.initialization_options {
            if let Ok(config) = serde_json::from_value::<ForgeConfig>(options) {
                *self.pending_config.lock().await = Some(config);
            }
        }

        let result = lsp_types::InitializeResult {
            capabilities: lsp_types::ServerCapabilities {
                text_document_sync: Some(lsp_types::TextDocumentSyncCapability::Kind(
                    lsp_types::TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(lsp_types::CompletionOptions {
                    trigger_characters: Some(vec![
                        "$".to_string(),
                        "[".to_string(),
                        ";".to_string(),
                    ]),
                    resolve_provider: Some(false),
                    ..lsp_types::CompletionOptions::default()
                }),
                hover_provider: Some(lsp_types::HoverProviderCapability::Simple(true)),
                signature_help_provider: Some(lsp_types::SignatureHelpOptions {
                    trigger_characters: Some(vec!["[".to_string(), ";".to_string()]),
                    retrigger_characters: Some(vec![";".to_string()]),
                    ..lsp_types::SignatureHelpOptions::default()
                }),
                semantic_tokens_provider: Some(
                    lsp_types::SemanticTokensServerCapabilities::SemanticTokensOptions(
                        lsp_types::SemanticTokensOptions {
                            legend: lsp_types::SemanticTokensLegend {
                                token_types: vec![lsp_types::SemanticTokenType::FUNCTION],
                                token_modifiers: vec![],
                            },
                            full: Some(lsp_types::SemanticTokensFullOptions::Bool(true)),
                            ..lsp_types::SemanticTokensOptions::default()
                        },
                    ),
                ),
                definition_provider: Some(lsp_types::OneOf::Left(true)),
                ..lsp_types::ServerCapabilities::default()
            },
            ..lsp_types::InitializeResult::default()
        };

        Ok(result)
    }

    async fn initialized(&self, _: lsp_types::InitializedParams) {
        let config = self.pending_config.lock().await.take();
        self.client
            .log_message(
                lsp_types::MessageType::INFO,
                format!("ForgeLSP initialized"),
            )
            .await;

        let config = match config {
            Some(c) => c,
            None => {
                self.scan_workspace().await;
                return;
            }
        };

        let cache_path = Self::resolve_cache_path(&config);
        let cache_loaded = self.try_load_cache(&cache_path).await;

        if cache_loaded {
            self.scan_workspace().await;
        }

        self.load_custom_functions(&config).await;

        if let Some(folder_path) = &config.custom_functions_path {
            let folder = PathBuf::from(folder_path);
            if folder.exists() && folder.is_dir() {
                self.start_custom_functions_watcher(folder).await;
            }
        }

        if let Some(urls) = &config.metadata_urls {
            for url_cfg in urls {
                let mut source = MetadataSource::new(url_cfg.extension.clone());
                if let Some(u) = &url_cfg.functions {
                    source = source.with_functions(u.clone());
                }
                if let Some(u) = &url_cfg.enums {
                    source = source.with_enums(u.clone());
                }
                if let Some(u) = &url_cfg.events {
                    source = source.with_events(u.clone());
                }
                self.metadata.add_source(source);
            }

            match self.metadata.fetch_all().await {
                Ok(stats) => {
                    self.client
                        .log_message(
                            lsp_types::MessageType::INFO,
                            format!("ForgeLSP: fresh metadata fetched — {}", stats),
                        )
                        .await;

                    self.save_cache(&cache_path).await;
                    self.refresh_all_documents().await;
                }
                Err(e) => {
                    let msg = if cache_loaded {
                        format!(
                            "ForgeLSP: metadata fetch failed ({}); using cached data.",
                            e
                        )
                    } else {
                        format!("ForgeLSP: metadata fetch failed and no cache found: {}", e)
                    };
                    self.client
                        .log_message(lsp_types::MessageType::WARNING, msg)
                        .await;
                }
            }
        }

        if !cache_loaded {
            self.scan_workspace().await;
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    // ========================================================================
    // Text-document lifecycle
    // ========================================================================

    async fn did_open(&self, params: lsp_types::DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let text = params.text_document.text;
        {
            let mut docs = self.documents.write().await;
            docs.insert(uri.clone(), text.clone());
        }
        self.refresh(uri, text).await;
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
        self.refresh(uri, text).await;
    }

    async fn did_close(&self, params: lsp_types::DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;
        {
            let mut docs = self.documents.write().await;
            docs.remove(&uri);
        }
        {
            let mut cache = self.parse_cache.write().await;
            cache.remove(&uri);
        }
        self.client.publish_diagnostics(uri, Vec::new(), None).await;
    }

    // ========================================================================
    // Completion
    // ========================================================================

    async fn completion(
        &self,
        params: lsp_types::CompletionParams,
    ) -> Result<Option<lsp_types::CompletionResponse>> {
        let uri = &params.text_document_position.text_document.uri;
        let pos = params.text_document_position.position;

        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => return Ok(None),
            }
        };

        let cursor_offset = position_to_byte_offset(&text, pos);

        if let Some(ctx) = find_arg_context(&text, cursor_offset) {
            let func_name = format!("${}", ctx.func_name);
            if let Some(func) = self.metadata.get(&func_name) {
                if let Some(enum_items) = get_enum_for_arg(&func, ctx.arg_index, &self.metadata) {
                    // We have a definitive enum list — return it exclusively.
                    let items: Vec<lsp_types::CompletionItem> = enum_items
                        .into_iter()
                        .map(|val| lsp_types::CompletionItem {
                            label: val.clone(),
                            kind: Some(lsp_types::CompletionItemKind::ENUM_MEMBER),
                            detail: func.args.as_ref().and_then(|args| {
                                args.get(ctx.arg_index).map(|a| {
                                    format!("{}: {}", a.name, format_arg_type(&a.arg_type))
                                })
                            }),
                            documentation: func.args.as_ref().and_then(|args| {
                                args.get(ctx.arg_index).and_then(|a| {
                                    if a.description.is_empty() {
                                        None
                                    } else {
                                        Some(lsp_types::Documentation::MarkupContent(
                                            lsp_types::MarkupContent {
                                                kind: lsp_types::MarkupKind::Markdown,
                                                value: a.description.clone(),
                                            },
                                        ))
                                    }
                                })
                            }),
                            insert_text: Some(val),
                            ..lsp_types::CompletionItem::default()
                        })
                        .collect();
                    return Ok(Some(lsp_types::CompletionResponse::Array(items)));
                }
                // No enum for this arg — fall through to prefix completions so
                // the user can still type a nested $function call as the value.
            }
            // Unknown function or no args — fall through as well.
        }

        let prefix = extract_dollar_prefix(&text, cursor_offset);
        let search_prefix = format!("${}", prefix);

        let functions = self.metadata.get_completions(&search_prefix);
        if functions.is_empty() && !prefix.is_empty() {
            return Ok(None);
        }

        let functions = if functions.is_empty() {
            self.metadata.all_functions()
        } else {
            functions
        };

        let items: Vec<lsp_types::CompletionItem> = functions
            .into_iter()
            .map(|func| build_completion_item(&func, &text, cursor_offset))
            .collect();

        Ok(Some(lsp_types::CompletionResponse::Array(items)))
    }

    // ========================================================================
    // Hover
    // ========================================================================

    async fn hover(&self, params: lsp_types::HoverParams) -> Result<Option<lsp_types::Hover>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => return Ok(None),
            }
        };

        let cursor_offset = position_to_byte_offset(&text, pos);

        let h_info = {
            let cache = self.parse_cache.read().await;
            match cache.get(uri) {
                Some(cached) => find_hover_info(&cached.ast, cursor_offset, 0),
                None => None,
            }
        };

        let func_name = match h_info {
            Some(i) => i.func_name,
            None => match find_function_name_at_offset(&text, cursor_offset) {
                Some(name) => name,
                None => return Ok(None),
            },
        };

        let func = match self.metadata.get(&func_name) {
            Some(f) => f,
            None => return Ok(None),
        };

        let hover_md = build_hover_markdown(&func);

        Ok(Some(lsp_types::Hover {
            contents: lsp_types::HoverContents::Markup(lsp_types::MarkupContent {
                kind: lsp_types::MarkupKind::Markdown,
                value: hover_md,
            }),
            range: None,
        }))
    }

    // ========================================================================
    // Signature Help
    // ========================================================================

    async fn signature_help(
        &self,
        params: lsp_types::SignatureHelpParams,
    ) -> Result<Option<lsp_types::SignatureHelp>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => return Ok(None),
            }
        };

        let cursor_offset = position_to_byte_offset(&text, pos);

        let call_info = {
            let cache = self.parse_cache.read().await;
            match cache.get(uri) {
                Some(cached) => find_call_for_sig_help(&cached.ast, cursor_offset),
                None => find_call_from_text(&text, cursor_offset),
            }
        };

        let call_info = match call_info {
            Some(c) => c,
            None => return Ok(None),
        };

        let func = match self.metadata.get(&call_info.func_name) {
            Some(f) => f,
            None => return Ok(None),
        };

        let sig = build_signature_info(&func);
        let active_param = call_info.arg_index as u32;

        Ok(Some(lsp_types::SignatureHelp {
            signatures: vec![sig],
            active_signature: Some(0),
            active_parameter: Some(active_param),
        }))
    }

    async fn semantic_tokens_full(
        &self,
        params: lsp_types::SemanticTokensParams,
    ) -> Result<Option<lsp_types::SemanticTokensResult>> {
        let uri = &params.text_document.uri;

        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => {
                    self.client
                        .log_message(
                            lsp_types::MessageType::WARNING,
                            format!("semantic_tokens: document not found: {}", uri),
                        )
                        .await;
                    return Ok(None);
                }
            }
        };

        let cache = self.parse_cache.read().await;
        let ast = match cache.get(uri) {
            Some(cached) => &cached.ast,
            None => {
                self.client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        "semantic_tokens: no AST cache",
                    )
                    .await;
                return Ok(None);
            }
        };

        let mut tokens = Vec::new();
        collect_semantic_tokens(ast, &text, &mut tokens);

        tokens.sort_by(|a, b| {
            if a.line != b.line {
                a.line.cmp(&b.line)
            } else {
                a.start.cmp(&b.start)
            }
        });

        let mut data = Vec::new();
        let mut last_line = 0u32;
        let mut last_char = 0u32;

        for token in tokens {
            let line_delta = token.line - last_line;
            let char_delta = if line_delta == 0 {
                token.start - last_char
            } else {
                token.start
            };

            data.push(lsp_types::SemanticToken {
                delta_line: line_delta,
                delta_start: char_delta,
                length: token.length,
                token_type: token.token_type,
                token_modifiers_bitset: token.modifier_mask,
            });

            last_line = token.line;
            last_char = token.start;
        }

        Ok(Some(lsp_types::SemanticTokensResult::Tokens(
            lsp_types::SemanticTokens {
                result_id: None,
                data,
            },
        )))
    }

    async fn goto_definition(
        &self,
        params: lsp_types::GotoDefinitionParams,
    ) -> Result<Option<lsp_types::GotoDefinitionResponse>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => return Ok(None),
            }
        };

        let cursor_offset = position_to_byte_offset(&text, pos);

        let h_info = {
            let cache = self.parse_cache.read().await;
            match cache.get(uri) {
                Some(cached) => find_hover_info(&cached.ast, cursor_offset, 0),
                None => None,
            }
        };

        let func_name = match h_info {
            Some(i) => i.func_name,
            None => match find_function_name_at_offset(&text, cursor_offset) {
                Some(name) => name,
                None => return Ok(None),
            },
        };

        let func = match self.metadata.get(&func_name) {
            Some(f) => f,
            None => return Ok(None),
        };

        if let Some(path) = &func.local_path {
            if let Ok(uri) = lsp_types::Url::from_file_path(path) {
                let line = func.line.unwrap_or(0);
                return Ok(Some(lsp_types::GotoDefinitionResponse::Scalar(
                    lsp_types::Location {
                        uri,
                        range: lsp_types::Range {
                            start: lsp_types::Position {
                                line: line as u32,
                                character: 0,
                            },
                            end: lsp_types::Position {
                                line: line as u32,
                                character: 0,
                            },
                        },
                    },
                )));
            }
        }

        Ok(None)
    }
}

// ============================================================================
// Diagnostics helpers
// ============================================================================

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

// ============================================================================
// Position / offset helpers
// ============================================================================

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

fn position_to_byte_offset(text: &str, pos: lsp_types::Position) -> usize {
    let mut current_line = 0u32;
    let mut line_start = 0usize;

    for (idx, ch) in text.char_indices() {
        if current_line == pos.line {
            let mut col = 0u32;
            let mut byte = line_start;
            for c in text[line_start..].chars() {
                if col >= pos.character {
                    break;
                }
                col += c.len_utf16() as u32;
                byte += c.len_utf8();
            }
            return byte;
        }
        if ch == '\n' {
            current_line += 1;
            line_start = idx + 1;
        }
    }

    text.len()
}

// ============================================================================
// Completion helpers
// ============================================================================

fn extract_dollar_prefix(text: &str, cursor: usize) -> String {
    let bytes = text.as_bytes();
    let cursor = cursor.min(bytes.len());

    let mut start = cursor;
    while start > 0 {
        let b = bytes[start - 1];
        if b.is_ascii_alphanumeric() || b == b'_' {
            start -= 1;
        } else {
            break;
        }
    }
    let id_start = start;

    let mut search = id_start;
    while search > 0 {
        let b = bytes[search - 1];
        if b == b'$' {
            return text[id_start..cursor].to_string();
        } else if b.is_ascii_alphabetic() || b == b'@' || b == b'!' || b == b'#' || b == b'?' {
            search -= 1;
        } else {
            break;
        }
    }

    String::new()
}

struct ArgContext {
    func_name: String,
    arg_index: usize,
}

fn find_arg_context(text: &str, cursor: usize) -> Option<ArgContext> {
    let bytes = text.as_bytes();
    let cursor = cursor.min(bytes.len());

    let mut depth = 0i32;
    let mut bracket_pos: Option<usize> = None;
    let mut i = cursor;

    while i > 0 {
        i -= 1;
        let b = bytes[i];

        if i > 0 && bytes[i - 1] == b'\\' {
            i -= 1;
            continue;
        }

        match b {
            b']' => depth += 1,
            b'[' => {
                if depth == 0 {
                    bracket_pos = Some(i);
                    break;
                }
                depth -= 1;
            }
            b'\n' if cursor.saturating_sub(i) > 500 => break,
            _ => {}
        }
    }

    let bracket_pos = bracket_pos?;

    let name_end = bracket_pos;
    let mut name_start = name_end;
    while name_start > 0
        && (bytes[name_start - 1].is_ascii_alphanumeric() || bytes[name_start - 1] == b'_')
    {
        name_start -= 1;
    }

    if name_start == name_end {
        return None;
    }
    if name_start == 0 || bytes[name_start - 1] != b'$' {
        return None;
    }

    let func_name = text[name_start..name_end].to_string();
    let content = &text[bracket_pos + 1..cursor.min(text.len())];
    let arg_index = count_arg_index(content);

    Some(ArgContext {
        func_name,
        arg_index,
    })
}

fn count_arg_index(content: &str) -> usize {
    let mut depth = 0i32;
    let mut count = 0usize;
    let bytes = content.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if i > 0 && bytes[i - 1] == b'\\' {
            i += 1;
            continue;
        }
        match bytes[i] {
            b'[' => depth += 1,
            b']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            b';' if depth == 0 => count += 1,
            _ => {}
        }
        i += 1;
    }
    count
}

fn get_enum_for_arg(
    func: &Function,
    arg_index: usize,
    metadata: &MetadataManager,
) -> Option<Vec<String>> {
    let args = func.args.as_ref()?;

    let has_rest = args.iter().any(|a| a.rest);
    let arg = if arg_index < args.len() {
        &args[arg_index]
    } else if has_rest {
        args.last()?
    } else {
        return None;
    };

    if let Some(inline) = &arg.arg_enum {
        if !inline.is_empty() {
            return Some(inline.clone());
        }
    }

    if let Some(enum_name) = &arg.enum_name {
        return metadata.get_enum(enum_name);
    }

    None
}

fn format_arg_type(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|i| i.as_str())
            .collect::<Vec<_>>()
            .join(" | "),
        _ => String::new(),
    }
}

fn build_completion_item(func: &Function, text: &str, cursor: usize) -> lsp_types::CompletionItem {
    let raw_name = func.name.clone();
    let label = raw_name.clone();
    let insert_name = raw_name.strip_prefix('$').unwrap_or(&raw_name).to_string();

    let bytes = text.as_bytes();
    let mut start = cursor;

    while start > 0 {
        let b = bytes[start - 1];
        if b.is_ascii_alphanumeric() || b == b'_' {
            start -= 1;
        } else {
            break;
        }
    }

    let replace_range = lsp_types::Range {
        start: byte_offset_to_position(text, start),
        end: byte_offset_to_position(text, cursor),
    };

    let detail = first_sentence(&func.description);
    let doc = build_hover_markdown_for_completion(func);

    lsp_types::CompletionItem {
        label: label.clone(),
        kind: Some(lsp_types::CompletionItemKind::FUNCTION),

        detail: if detail.is_empty() {
            None
        } else {
            Some(detail)
        },

        documentation: if doc.is_empty() {
            None
        } else {
            Some(lsp_types::Documentation::MarkupContent(
                lsp_types::MarkupContent {
                    kind: lsp_types::MarkupKind::Markdown,
                    value: doc,
                },
            ))
        },

        text_edit: Some(lsp_types::CompletionTextEdit::Edit(lsp_types::TextEdit {
            range: replace_range,
            new_text: insert_name,
        })),

        insert_text_format: Some(lsp_types::InsertTextFormat::PLAIN_TEXT),
        ..lsp_types::CompletionItem::default()
    }
}

fn first_sentence(s: &str) -> String {
    s.split(['.', '\n']).next().unwrap_or("").trim().to_string()
}

// ============================================================================
// AST traversal helpers
// ============================================================================

struct HoverInfo {
    func_name: String,
}

fn find_hover_info(node: &AstNode, offset: usize, current_depth: usize) -> Option<HoverInfo> {
    match node {
        AstNode::Program { body, .. } => {
            for child in body {
                if let Some(info) = find_hover_info(child, offset, current_depth) {
                    return Some(info);
                }
            }
            None
        }
        AstNode::FunctionCall {
            name,
            span,
            args,
            name_span,
            ..
        } => {
            if offset < span.start || offset >= span.end {
                return None;
            }

            if let Some(args) = args {
                for arg in args {
                    for part in &arg.parts {
                        if let Some(inner) = find_hover_info(part, offset, current_depth + 1) {
                            return Some(inner);
                        }
                    }
                }
            }

            let in_name = offset >= name_span.start && offset < name_span.end;
            if !in_name {
                return None;
            }

            let full_name = if name.starts_with('$') {
                name.clone()
            } else {
                format!("${}", name)
            };

            Some(HoverInfo {
                func_name: full_name,
            })
        }
        _ => None,
    }
}

struct CallInfo {
    func_name: String,
    arg_index: usize,
}

fn find_call_for_sig_help(node: &AstNode, offset: usize) -> Option<CallInfo> {
    match node {
        AstNode::Program { body, .. } => {
            for child in body {
                if let Some(info) = find_call_for_sig_help(child, offset) {
                    return Some(info);
                }
            }
            None
        }
        AstNode::FunctionCall {
            name,
            args_span,
            args,
            ..
        } => {
            // Recurse into children first so the innermost call wins.
            if let Some(args) = args {
                for arg in args {
                    for part in &arg.parts {
                        if let Some(inner) = find_call_for_sig_help(part, offset) {
                            return Some(inner);
                        }
                    }
                }
            }

            if let Some(aspan) = args_span {
                if offset >= aspan.start && offset < aspan.end {
                    let func_name = if name.starts_with('$') {
                        name.clone()
                    } else {
                        format!("${}", name)
                    };

                    let arg_index = if let Some(args) = args {
                        args.iter().take_while(|a| a.span.end < offset).count()
                    } else {
                        0
                    };

                    return Some(CallInfo {
                        func_name,
                        arg_index,
                    });
                }
            }
            None
        }
        _ => None,
    }
}

// ============================================================================
// Text-based fallback helpers
// ============================================================================

fn find_function_name_at_offset(text: &str, offset: usize) -> Option<String> {
    let bytes = text.as_bytes();
    let offset = offset.min(bytes.len());

    let mut end = offset;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }

    let mut start = offset;
    while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        start -= 1;
    }

    if start > 0 && bytes[start - 1] == b'$' {
        let name = &text[start..end];
        if !name.is_empty() {
            return Some(format!("${}", name));
        }
    }
    None
}

fn find_call_from_text(text: &str, cursor: usize) -> Option<CallInfo> {
    let ctx = find_arg_context(text, cursor)?;
    Some(CallInfo {
        func_name: format!("${}", ctx.func_name),
        arg_index: ctx.arg_index,
    })
}

// ============================================================================
// Hover / Signature markdown builders
// ============================================================================

pub fn build_hover_markdown(func: &Function) -> String {
    let mut md = String::new();

    let usage = build_usage_line_v2(func);
    md.push_str(&format!("```forge\n{}\n```", usage));

    if let Some(ext) = &func.extension {
        md.push_str(&format!("\n*{}*\n", ext));
    }

    md.push_str("\n---\n");

    if !func.description.is_empty() {
        md.push('\n');
        md.push_str(&func.description);
        md.push('\n');
    }

    if func.deprecated.unwrap_or(false) {
        md.push_str("\n> ⚠️ **Deprecated**\n");
    }

    if func.experimental.unwrap_or(false) {
        md.push_str("\n> 🧪 **Experimental**\n");
    }

    let mut links = Vec::new();

    if let Some(url) = &func.source_url {
        if let Some(github) = extract_github_url(url) {
            links.push(format!("[Github]({})", github));
        }
    }

    if let Some(ext) = &func.extension {
        links.push(format!(
            "[Documentation](https://docs.botforge.org/function/?p={})",
            ext
        ));
    }

    if !links.is_empty() {
        md.push_str("\n---\n");
        md.push_str(&links.join(" | "));
        md.push('\n');
    }

    if let Some(aliases) = &func.aliases {
        if !aliases.is_empty() {
            let alias_list: Vec<String> = aliases
                .iter()
                .map(|a| {
                    if a.starts_with('$') {
                        format!("`{}`", a)
                    } else {
                        format!("`${}`", a)
                    }
                })
                .collect();

            md.push_str(&format!("\n**Aliases:** {}\n", alias_list.join(", ")));
        }
    }

    md
}

fn extract_github_url(source_url: &str) -> Option<String> {
    if source_url.contains("raw.githubusercontent.com") {
        let parts: Vec<&str> = source_url.split('/').collect();
        if parts.len() >= 6 {
            let user = parts[3];
            let repo = parts[4];
            let branch = parts[5];
            return Some(format!(
                "https://github.com/{}/{}/tree/{}/",
                user, repo, branch
            ));
        }
    }
    None
}

pub fn build_hover_markdown_for_completion(func: &Function) -> String {
    build_hover_markdown(func)
}

fn build_usage_line_v2(func: &Function) -> String {
    let has_args = match &func.args {
        Some(v) => !v.is_empty(),
        None => false,
    };
    let show_brackets = func.brackets.unwrap_or(false) || has_args;

    if !show_brackets {
        return func.name.clone();
    }

    match &func.args {
        Some(vec) if !vec.is_empty() => {
            let arg_parts: Vec<String> = vec
                .iter()
                .map(|a| {
                    let mut name_part = a.name.clone();
                    if a.rest {
                        name_part = format!("...{}", name_part);
                    }
                    if !a.required.unwrap_or(false) && !a.rest {
                        name_part.push('?');
                    }

                    let ty = format_arg_type(&a.arg_type);
                    format!("{}: {}", name_part, ty)
                })
                .collect();
            format!("{}[{}]", func.name, arg_parts.join("; "))
        }
        _ => {
            if show_brackets {
                format!("{}[]", func.name)
            } else {
                func.name.clone()
            }
        }
    }
}

fn build_signature_info(func: &Function) -> lsp_types::SignatureInformation {
    let label = build_usage_line_v2(func);
    let mut parameters: Vec<lsp_types::ParameterInformation> = Vec::new();

    if let Some(args) = &func.args {
        let mut current_offset = func.name.len() + 1;

        for (idx, arg) in args.iter().enumerate() {
            let mut name_part = arg.name.clone();
            if arg.rest {
                name_part = format!("...{}", name_part);
            }
            if !arg.required.unwrap_or(false) && !arg.rest {
                name_part.push('?');
            }

            let ty = format_arg_type(&arg.arg_type);
            let rendered = format!("{}: {}", name_part, ty);

            let param_start = current_offset;
            let param_end = param_start + rendered.len();

            let doc = if arg.description.is_empty() {
                None
            } else {
                Some(lsp_types::Documentation::MarkupContent(
                    lsp_types::MarkupContent {
                        kind: lsp_types::MarkupKind::Markdown,
                        value: arg.description.clone(),
                    },
                ))
            };

            parameters.push(lsp_types::ParameterInformation {
                label: lsp_types::ParameterLabel::LabelOffsets([
                    param_start as u32,
                    param_end as u32,
                ]),
                documentation: doc,
            });

            current_offset = param_end;
            if idx + 1 < args.len() {
                current_offset += 2;
            }
        }
    }

    lsp_types::SignatureInformation {
        label: label.clone(),
        documentation: Some(lsp_types::Documentation::MarkupContent(
            lsp_types::MarkupContent {
                kind: lsp_types::MarkupKind::Markdown,
                value: "---".to_string(),
            },
        )),
        parameters: Some(parameters),
        active_parameter: None,
    }
}

// ============================================================================
// Semantic Tokens helpers
// ============================================================================

struct RawSemanticToken {
    line: u32,
    start: u32,
    length: u32,
    token_type: u32,
    modifier_mask: u32,
}

const TOKEN_TYPE_FUNCTION: u32 = 0;

fn collect_semantic_tokens(node: &AstNode, text: &str, tokens: &mut Vec<RawSemanticToken>) {
    match node {
        AstNode::Program { body, .. } => {
            for child in body {
                collect_semantic_tokens(child, text, tokens);
            }
        }
        AstNode::FunctionCall {
            name,
            name_span,
            args,
            ..
        } => {
            if name != "c" && name != "$c" {
                let pos = byte_offset_to_position(text, name_span.start);
                let length = (name_span.end - name_span.start) as u32;

                tokens.push(RawSemanticToken {
                    line: pos.line,
                    start: pos.character,
                    length,
                    token_type: TOKEN_TYPE_FUNCTION,
                    modifier_mask: 0,
                });
            }

            if let Some(args) = args {
                for arg in args {
                    for part in &arg.parts {
                        collect_semantic_tokens(part, text, tokens);
                    }
                }
            }
        }
        _ => {}
    }
}
