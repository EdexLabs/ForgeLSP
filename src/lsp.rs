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

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct MetadataUrlConfig {
    extension: String,
    functions: Option<String>,
    enums: Option<String>,
    events: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum CustomFunctionsPath {
    Single(String),
    Multiple(Vec<String>),
}

impl CustomFunctionsPath {
    fn to_vec(&self) -> Vec<String> {
        match self {
            Self::Single(s) => vec![s.clone()],
            Self::Multiple(v) => v.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ForgeConfig {
    metadata_urls: Option<Vec<MetadataUrlConfig>>,
    custom_functions_path: Option<CustomFunctionsPath>,
    custom_functions_json: Option<String>,
    cache_path: Option<String>,
    // ── Function cycling colors (existing) ─────────────────────────────────
    custom_colors: Option<Vec<String>>,
    constant_custom_colors: Option<bool>,
    // ── Per-token-type colors (new, all optional single hex strings) ────────
    custom_color_text: Option<String>,
    custom_color_time: Option<String>,
    custom_color_numbers: Option<String>,
    custom_color_dollar: Option<String>,
    custom_color_modifiers: Option<String>,
    custom_color_boolean: Option<String>,
    custom_color_separators: Option<String>,
    // ── Dual-decoration mode ───────────────────────────────────────────────
    /// When true, both semantic tokens (theme-driven) AND text decorations
    /// (custom colors) are active simultaneously.
    semantic_decorations: Option<bool>,
}

// ============================================================================
// Custom color notification types
// ============================================================================

/// A token that carries a single fixed color (no index into a palette).
#[derive(serde::Serialize, serde::Deserialize)]
struct SimpleColorToken {
    range: lsp_types::Range,
}

/// A function-name token whose color is chosen by index into the palette.
#[derive(serde::Serialize, serde::Deserialize)]
struct CustomColorToken {
    range: lsp_types::Range,
    color_index: usize,
}

/// Notification sent from the server to the client with all colored token
/// ranges so the extension can apply `TextEditorDecorationType`s.
#[derive(serde::Serialize, serde::Deserialize)]
struct CustomColorNotification {
    uri: lsp_types::Url,
    /// Function-name tokens — colored via `custom_colors` palette (rotating/hashed).
    tokens: Vec<CustomColorToken>,
    /// Raw text nodes — colored via `custom_color_text`.
    text_tokens: Vec<SimpleColorToken>,
    /// Time-literal tokens (e.g. `10m`, `30s`) — colored via `custom_color_time`.
    time_tokens: Vec<SimpleColorToken>,
    /// Numeric-literal tokens — colored via `custom_color_numbers`.
    number_tokens: Vec<SimpleColorToken>,
    /// `$` prefix tokens — colored via `custom_color_dollar`.
    dollar_tokens: Vec<SimpleColorToken>,
    /// Modifier character tokens (`!`, `#`, `?`, etc.) — colored via `custom_color_modifiers`.
    modifier_tokens: Vec<SimpleColorToken>,
    /// Boolean-literal tokens (`true`/`false`) — colored via `custom_color_boolean`.
    boolean_tokens: Vec<SimpleColorToken>,
    /// `;` separator tokens — colored via `custom_color_separators`.
    separator_tokens: Vec<SimpleColorToken>,
}

impl tower_lsp::lsp_types::notification::Notification for CustomColorNotification {
    type Params = CustomColorNotification;
    const METHOD: &'static str = "forge/customColors";
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
    config: RwLock<Option<ForgeConfig>>,
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
            config: RwLock::new(None),
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

        if let Some(cfg) = self.config.read().await.as_ref() {
            let has_any_custom = cfg
                .custom_colors
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false)
                || cfg.custom_color_text.is_some()
                || cfg.custom_color_time.is_some()
                || cfg.custom_color_numbers.is_some()
                || cfg.custom_color_dollar.is_some()
                || cfg.custom_color_modifiers.is_some()
                || cfg.custom_color_boolean.is_some()
                || cfg.custom_color_separators.is_some();

            if has_any_custom {
                let mut color_tokens = Vec::new();
                let mut text_tokens = Vec::new();
                let mut time_tokens = Vec::new();
                let mut number_tokens = Vec::new();
                let mut dollar_tokens = Vec::new();
                let mut modifier_tokens = Vec::new();
                let mut boolean_tokens = Vec::new();
                let mut separator_tokens = Vec::new();
                let mut dummy_semantic_tokens = Vec::new();

                let color_count = cfg.custom_colors.as_ref().map(|v| v.len()).unwrap_or(0);
                let constant_colors = cfg.constant_custom_colors.unwrap_or(false);
                let mut state = 0;

                collect_all_decorations_and_semantic_tokens(
                    &ast,
                    &text,
                    &mut color_tokens,
                    &mut text_tokens,
                    &mut time_tokens,
                    &mut number_tokens,
                    &mut dollar_tokens,
                    &mut modifier_tokens,
                    &mut boolean_tokens,
                    &mut separator_tokens,
                    &mut dummy_semantic_tokens,
                    color_count,
                    &mut state,
                    constant_colors,
                    !is_js_ts, // inside_code_block: true for .forge, false for JS/TS top-level
                );

                if cfg.custom_colors.is_none()
                    || cfg
                        .custom_colors
                        .as_ref()
                        .map(|v| v.is_empty())
                        .unwrap_or(true)
                {
                    color_tokens.clear();
                }
                if cfg.custom_color_text.is_none() {
                    text_tokens.clear();
                }
                if cfg.custom_color_time.is_none() {
                    time_tokens.clear();
                }
                if cfg.custom_color_numbers.is_none() {
                    number_tokens.clear();
                }
                if cfg.custom_color_dollar.is_none() {
                    dollar_tokens.clear();
                }
                if cfg.custom_color_modifiers.is_none() {
                    modifier_tokens.clear();
                }
                if cfg.custom_color_boolean.is_none() {
                    boolean_tokens.clear();
                }
                if cfg.custom_color_separators.is_none() {
                    separator_tokens.clear();
                }

                self.client
                    .send_notification::<CustomColorNotification>(CustomColorNotification {
                        uri: uri.clone(),
                        tokens: color_tokens,
                        text_tokens,
                        time_tokens,
                        number_tokens,
                        dollar_tokens,
                        modifier_tokens,
                        boolean_tokens,
                        separator_tokens,
                    })
                    .await;
            }
        }

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

    // No longer a method here, moved to a standalone function below

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

    async fn start_custom_functions_watcher(&self, folders: Vec<PathBuf>) {
        if folders.is_empty() {
            return;
        }

        let metadata = Arc::clone(&self.metadata);
        let client = self.client.clone();
        let config_json = self
            .config
            .read()
            .await
            .as_ref()
            .and_then(|c| c.custom_functions_json.clone());
        let folders_clone = folders.clone();

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

        for folder in &folders {
            if let Err(e) = watcher.watch(folder, RecursiveMode::Recursive) {
                client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!("ForgeLSP: cannot watch {}: {}", folder.display(), e),
                    )
                    .await;
                return;
            }
        }

        *self._fs_watcher.lock().await = Some(watcher);

        for folder in &folders {
            client
                .log_message(
                    lsp_types::MessageType::INFO,
                    format!(
                        "ForgeLSP: watching custom-functions folder: {}",
                        folder.display()
                    ),
                )
                .await;
        }

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

                perform_load_custom_functions(
                    metadata.clone(),
                    client.clone(),
                    config_json.clone(),
                    Some(
                        folders_clone
                            .iter()
                            .map(|p| p.to_string_lossy().to_string())
                            .collect(),
                    ),
                )
                .await;
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
                semantic_tokens_provider: {
                    // Disable semantic tokens when custom_colors is set, UNLESS
                    // semantic_decorations:true is configured (dual-mode).
                    let has_custom_colors;
                    let semantic_decorations;
                    {
                        let pending = self.pending_config.lock().await;
                        let cfg = pending.as_ref();
                        has_custom_colors = cfg
                            .and_then(|c| c.custom_colors.as_ref())
                            .map(|v| !v.is_empty())
                            .unwrap_or(false);
                        semantic_decorations =
                            cfg.and_then(|c| c.semantic_decorations).unwrap_or(false);
                    }
                    if has_custom_colors && !semantic_decorations {
                        None
                    } else {
                        Some(
                            lsp_types::SemanticTokensServerCapabilities::SemanticTokensOptions(
                                lsp_types::SemanticTokensOptions {
                                    legend: lsp_types::SemanticTokensLegend {
                                        token_types: vec![
                                            lsp_types::SemanticTokenType::FUNCTION,  // 0
                                            lsp_types::SemanticTokenType::COMMENT,   // 1
                                            lsp_types::SemanticTokenType::STRING,    // 2
                                            lsp_types::SemanticTokenType::NUMBER,    // 3
                                            lsp_types::SemanticTokenType::MACRO,     // 4 (time)
                                            lsp_types::SemanticTokenType::OPERATOR, // 5 (modifiers & separators)
                                            lsp_types::SemanticTokenType::DECORATOR, // 6 (dollar sign)
                                            lsp_types::SemanticTokenType::KEYWORD,   // 7 (booleans)
                                        ],
                                        token_modifiers: vec![],
                                    },
                                    full: Some(lsp_types::SemanticTokensFullOptions::Bool(true)),
                                    ..lsp_types::SemanticTokensOptions::default()
                                },
                            ),
                        )
                    }
                },
                definition_provider: Some(lsp_types::OneOf::Left(true)),
                folding_range_provider: Some(lsp_types::FoldingRangeProviderCapability::Simple(
                    true,
                )),
                execute_command_provider: Some(lsp_types::ExecuteCommandOptions {
                    commands: vec!["forge.getInlineCompletions".to_string()],
                    ..lsp_types::ExecuteCommandOptions::default()
                }),
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
            Some(c) => {
                let mut guard = self.config.write().await;
                *guard = Some(c.clone());
                c
            }
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

        perform_load_custom_functions(
            self.metadata.clone(),
            self.client.clone(),
            config.custom_functions_json.clone(),
            config.custom_functions_path.as_ref().map(|p| p.to_vec()),
        )
        .await;

        if let Some(cf_path) = &config.custom_functions_path {
            let mut folders = Vec::new();
            for folder_path in cf_path.to_vec() {
                let folder = PathBuf::from(folder_path);
                if folder.exists() && folder.is_dir() {
                    folders.push(folder);
                }
            }
            if !folders.is_empty() {
                self.start_custom_functions_watcher(folders).await;
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

        self.refresh_all_documents().await;
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

        let prefix = match extract_dollar_prefix(&text, cursor_offset) {
            Some(p) => p,
            None => return Ok(None),
        };

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

        let mut items: Vec<lsp_types::CompletionItem> = functions
            .into_iter()
            .map(|func| build_completion_item(&func, &text, cursor_offset))
            .collect();

        // Sort by label length ascending so shorter names surface first.
        items.sort_by(|a, b| {
            a.label
                .len()
                .cmp(&b.label.len())
                .then(a.label.cmp(&b.label))
        });

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

    async fn execute_command(
        &self,
        params: lsp_types::ExecuteCommandParams,
    ) -> Result<Option<serde_json::Value>> {
        if params.command == "forge.getInlineCompletions" {
            let args = params.arguments;
            if args.len() < 3 {
                return Ok(None);
            }

            // Expected arguments from VS Code: [ uri, line, character ]
            let uri_str = args.get(0).and_then(|v| v.as_str());
            let line = args.get(1).and_then(|v| v.as_u64());
            let character = args.get(2).and_then(|v| v.as_u64());

            let (uri_str, line, character) = match (uri_str, line, character) {
                (Some(u), Some(l), Some(c)) => (u, l as u32, c as u32),
                _ => return Ok(None),
            };

            let uri = match lsp_types::Url::parse(uri_str) {
                Ok(u) => u,
                Err(_) => return Ok(None),
            };

            let text = {
                let docs = self.documents.read().await;
                match docs.get(&uri) {
                    Some(t) => t.clone(),
                    None => return Ok(None),
                }
            };

            let pos = lsp_types::Position { line, character };
            let cursor_offset = position_to_byte_offset(&text, pos);

            let mut completions = Vec::new();

            // 1. Suggest [] for functions that accept brackets
            if let Some(prefix) = extract_dollar_prefix(&text, cursor_offset) {
                let func_name = format!("${}", prefix);
                if let Some(func) = self.metadata.get_exact(&func_name) {
                    if func.brackets == Some(true) {
                        let followed_by_bracket =
                            text[cursor_offset..].trim_start().starts_with('[');

                        if !followed_by_bracket {
                            completions.push("[]".to_string());
                        }
                    }
                }
            }

            // 2. Suggest ] for missing closing brackets
            if completions.is_empty() {
                if find_arg_context(&text, cursor_offset).is_some() {
                    if is_bracket_unclosed(&text, cursor_offset) {
                        completions.push("]".to_string());
                    }
                }
            }

            return Ok(Some(
                serde_json::to_value(completions).unwrap_or(serde_json::Value::Null),
            ));
        }

        Ok(None)
    }

    async fn semantic_tokens_full(
        &self,
        params: lsp_types::SemanticTokensParams,
    ) -> Result<Option<lsp_types::SemanticTokensResult>> {
        let uri = &params.text_document.uri;
        let is_js_ts = uri.path().ends_with(".js") || uri.path().ends_with(".ts");

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

        let mut dummy_colors = Vec::new();
        let mut dummy_text = Vec::new();
        let mut dummy_time = Vec::new();
        let mut dummy_number = Vec::new();
        let mut dummy_dollar = Vec::new();
        let mut dummy_modifier = Vec::new();
        let mut dummy_boolean = Vec::new();
        let mut dummy_separator = Vec::new();
        let mut tokens = Vec::new();

        collect_all_decorations_and_semantic_tokens(
            ast,
            &text,
            &mut dummy_colors,
            &mut dummy_text,
            &mut dummy_time,
            &mut dummy_number,
            &mut dummy_dollar,
            &mut dummy_modifier,
            &mut dummy_boolean,
            &mut dummy_separator,
            &mut tokens,
            0,
            &mut 0,
            false,
            !is_js_ts,
        );

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

    async fn folding_range(
        &self,
        params: lsp_types::FoldingRangeParams,
    ) -> Result<Option<Vec<lsp_types::FoldingRange>>> {
        let uri = &params.text_document.uri;

        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => return Ok(None),
            }
        };

        let cache = self.parse_cache.read().await;
        let ast = match cache.get(uri) {
            Some(cached) => &cached.ast,
            None => return Ok(None),
        };

        let mut ranges = Vec::new();
        collect_folding_ranges(ast, &text, &mut ranges);

        Ok(Some(ranges))
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

    // LSP positions use UTF-16 code units, not Unicode scalar values.
    // Characters outside the Basic Multilingual Plane (e.g. emoji like 🔴,
    // U+1F534) encode as 1 Rust `char` but 2 UTF-16 code units.  Using
    // `chars().count()` therefore under-counts the column for any line that
    // contains such characters, causing semantic-token highlights, diagnostics,
    // and completion ranges to be shifted left by one position per non-BMP char.
    let col: u32 = prefix[last_line_start..]
        .chars()
        .map(|c| c.len_utf16() as u32)
        .sum();
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

fn extract_dollar_prefix(text: &str, cursor: usize) -> Option<String> {
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
    let mut in_bracket = false;

    while search > 0 {
        let b = bytes[search - 1];

        if b == b'$' && !in_bracket {
            return Some(text[id_start..cursor].to_string());
        }

        if b == b']' {
            in_bracket = true;
        } else if b == b'[' {
            in_bracket = false;
        }

        // Allowed in prefix: modifiers (!, #, ?), tags (@, []), and whitespace.
        // Inside brackets, we are more permissive (for tag content).
        let is_valid = if in_bracket {
            b != b'\n' && b != b'\r'
        } else {
            matches!(
                b,
                b'!' | b'#' | b'?' | b'@' | b'[' | b']' | b' ' | b'\t' | b'.' | b'-' | b'_'
            )
        };

        if is_valid {
            search -= 1;
        } else {
            break;
        }
    }

    None
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

fn is_bracket_unclosed(text: &str, cursor: usize) -> bool {
    let bytes = text.as_bytes();
    let mut backward_depth = 0i32;
    let mut i = cursor;

    // 1. Find nearest unclosed opening bracket before cursor
    let mut opening_bracket_pos = None;
    while i > 0 {
        i -= 1;
        match bytes[i] {
            b'[' => {
                if backward_depth == 0 {
                    opening_bracket_pos = Some(i);
                    break;
                }
                backward_depth -= 1;
            }
            b']' => {
                backward_depth += 1;
            }
            b'\n' => break,
            _ => {}
        }
    }

    let start_pos = match opening_bracket_pos {
        Some(pos) => pos,
        None => return false,
    };

    // 2. From that opening bracket, scan forward to the end of the line
    // to see if it is EVER closed.
    let mut forward_depth = 0i32;
    let mut j = start_pos;
    while j < bytes.len() {
        match bytes[j] {
            b'[' => forward_depth += 1,
            b']' => {
                forward_depth -= 1;
                if forward_depth == 0 {
                    if j >= cursor {
                        return false;
                    }
                }
            }
            b'\n' => break,
            _ => {}
        }
        j += 1;
    }

    // If we finished the scan and forward_depth > 0, it means it's unclosed.
    forward_depth > 0
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

    if let Some(output) = &func.output {
        let out_str = format_arg_type(output);
        if !out_str.is_empty() {
            md.push_str(&format!("\n**Returns:** `{}`\n", out_str));
        }
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
        let name = ext.split('/').last().unwrap_or(ext);
        links.push(format!(
            "[Documentation](https://docs.botforge.org/function/{}?p={})",
            &func.name, name
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
    let arg_parts = if let Some(vec) = &func.args {
        if !vec.is_empty() {
            Some(
                vec.iter()
                    .map(|a| {
                        let mut name_part = a.name.clone();
                        if a.rest {
                            name_part = format!("...{}", name_part);
                        }
                        if a.rest || !a.required.unwrap_or(false) {
                            name_part.push('?');
                        }

                        let ty = format_arg_type(&a.arg_type);
                        format!("{}: {}", name_part, ty)
                    })
                    .collect::<Vec<String>>()
                    .join("; "),
            )
        } else {
            None
        }
    } else {
        None
    };

    match func.brackets {
        Some(true) => match arg_parts {
            Some(args) => format!("{}[{}]", func.name, args),
            None => format!("{}[]", func.name),
        },
        Some(false) => match arg_parts {
            Some(args) => format!("{}[{}]?", func.name, args),
            None => format!("{}[]?", func.name),
        },
        None => func.name.clone(),
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
            if arg.rest || !arg.required.unwrap_or(false) {
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

const TOKEN_TYPE_FUNCTION: u32 = 0; // FUNCTION  (index 0)
const TOKEN_TYPE_COMMENT: u32 = 1; // COMMENT   (index 1)
const TOKEN_TYPE_STRING: u32 = 2; // STRING    (index 2)
const TOKEN_TYPE_NUMBER: u32 = 3; // NUMBER    (index 3)
const TOKEN_TYPE_TIME: u32 = 4; // MACRO     (index 4)
const TOKEN_TYPE_OPERATOR: u32 = 5; // OPERATOR  (index 5) — modifiers
const TOKEN_TYPE_DOLLAR: u32 = 6; // DECORATOR (index 6) — dollar sign
const TOKEN_TYPE_SEPARATOR: u32 = 5; // OPERATOR  (index 5) — reuse for separators
const TOKEN_TYPE_BOOLEAN: u32 = 7; // KEYWORD   (index 7)

fn get_sub_tokens_in_text(content: &str, base_offset: usize) -> (Vec<Span>, Vec<Span>, Vec<Span>) {
    let mut times = Vec::new();
    let mut numbers = Vec::new();
    let mut booleans = Vec::new();

    let bytes = content.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Check Boolean "true"
        if i + 4 <= len && &bytes[i..i + 4] == b"true" {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after_ok =
                i + 4 >= len || !(bytes[i + 4].is_ascii_alphanumeric() || bytes[i + 4] == b'_');
            if before_ok && after_ok {
                booleans.push(Span::new(base_offset + i, base_offset + i + 4));
                i += 4;
                continue;
            }
        }

        // Check Boolean "false"
        if i + 5 <= len && &bytes[i..i + 5] == b"false" {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after_ok =
                i + 5 >= len || !(bytes[i + 5].is_ascii_alphanumeric() || bytes[i + 5] == b'_');
            if before_ok && after_ok {
                booleans.push(Span::new(base_offset + i, base_offset + i + 5));
                i += 5;
                continue;
            }
        }

        // Check for number/time starting with digit
        if bytes[i].is_ascii_digit() {
            let is_word_start =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            if is_word_start {
                let start = i;
                let mut temp_i = i;
                while temp_i < len && bytes[temp_i].is_ascii_digit() {
                    temp_i += 1;
                }

                // Check time suffix
                if temp_i < len && matches!(bytes[temp_i], b's' | b'm' | b'h' | b'd') {
                    let end = temp_i + 1;
                    let after_ok =
                        end >= len || !(bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_');
                    if after_ok {
                        times.push(Span::new(base_offset + start, base_offset + end));
                        i = end;
                        continue;
                    }
                }

                // Check decimal
                let mut decimal_i = temp_i;
                if decimal_i + 1 < len
                    && bytes[decimal_i] == b'.'
                    && bytes[decimal_i + 1].is_ascii_digit()
                {
                    decimal_i += 1; // consume `.`
                    while decimal_i < len && bytes[decimal_i].is_ascii_digit() {
                        decimal_i += 1;
                    }
                }

                let is_word_end = decimal_i >= len
                    || !(bytes[decimal_i].is_ascii_alphanumeric() || bytes[decimal_i] == b'_');
                if is_word_end {
                    numbers.push(Span::new(base_offset + start, base_offset + decimal_i));
                    i = decimal_i;
                    continue;
                }
            }
        }

        i += 1;
    }

    (times, numbers, booleans)
}

fn get_string_spans(total_span: Span, mut exclude_spans: Vec<Span>) -> Vec<Span> {
    exclude_spans.sort_by_key(|s| s.start);

    let mut string_spans = Vec::new();
    let mut current_start = total_span.start;

    for excl in exclude_spans {
        if excl.start > current_start {
            string_spans.push(Span::new(current_start, excl.start));
        }
        current_start = current_start.max(excl.end);
    }

    if total_span.end > current_start {
        string_spans.push(Span::new(current_start, total_span.end));
    }

    string_spans
}

fn collect_all_decorations_and_semantic_tokens(
    node: &AstNode,
    text: &str,
    tokens: &mut Vec<CustomColorToken>,
    text_tokens: &mut Vec<SimpleColorToken>,
    time_tokens: &mut Vec<SimpleColorToken>,
    number_tokens: &mut Vec<SimpleColorToken>,
    dollar_tokens: &mut Vec<SimpleColorToken>,
    modifier_tokens: &mut Vec<SimpleColorToken>,
    boolean_tokens: &mut Vec<SimpleColorToken>,
    separator_tokens: &mut Vec<SimpleColorToken>,
    semantic_tokens: &mut Vec<RawSemanticToken>,
    color_count: usize,
    state: &mut usize,
    constant_colors: bool,
    inside_code_block: bool,
) {
    match node {
        AstNode::Program { body, .. } => {
            for child in body {
                collect_all_decorations_and_semantic_tokens(
                    child,
                    text,
                    tokens,
                    text_tokens,
                    time_tokens,
                    number_tokens,
                    dollar_tokens,
                    modifier_tokens,
                    boolean_tokens,
                    separator_tokens,
                    semantic_tokens,
                    color_count,
                    state,
                    constant_colors,
                    inside_code_block,
                );
            }
        }
        AstNode::FunctionCall {
            name,
            name_span,
            modifier_span,
            args,
            ..
        } => {
            let is_comment = name.eq_ignore_ascii_case("c") || name.eq_ignore_ascii_case("$c");

            if is_comment {
                let pos = byte_offset_to_position(text, name_span.start);
                semantic_tokens.push(RawSemanticToken {
                    line: pos.line,
                    start: pos.character,
                    length: (name_span.end - name_span.start) as u32,
                    token_type: TOKEN_TYPE_COMMENT,
                    modifier_mask: 0,
                });
                if let Some(args) = args {
                    for arg in args {
                        for part in &arg.parts {
                            collect_all_decorations_and_semantic_tokens(
                                part,
                                text,
                                tokens,
                                text_tokens,
                                time_tokens,
                                number_tokens,
                                dollar_tokens,
                                modifier_tokens,
                                boolean_tokens,
                                separator_tokens,
                                semantic_tokens,
                                color_count,
                                state,
                                constant_colors,
                                inside_code_block,
                            );
                        }
                    }
                }
            } else {
                // 1. Dollar sign ($)
                let dollar_span = Span::new(name_span.start, name_span.start + 1);
                let dollar_range = span_to_range(text, dollar_span);
                dollar_tokens.push(SimpleColorToken {
                    range: dollar_range,
                });

                let pos_dollar = byte_offset_to_position(text, dollar_span.start);
                semantic_tokens.push(RawSemanticToken {
                    line: pos_dollar.line,
                    start: pos_dollar.character,
                    length: 1,
                    token_type: TOKEN_TYPE_DOLLAR,
                    modifier_mask: 0,
                });

                // 2. Modifiers
                let mut modifier_end = name_span.start + 1;
                if let Some(mspan) = modifier_span {
                    if !mspan.is_empty() {
                        modifier_tokens.push(SimpleColorToken {
                            range: span_to_range(text, *mspan),
                        });
                        modifier_end = mspan.end;

                        let pos_mod = byte_offset_to_position(text, mspan.start);
                        semantic_tokens.push(RawSemanticToken {
                            line: pos_mod.line,
                            start: pos_mod.character,
                            length: (mspan.end - mspan.start) as u32,
                            token_type: TOKEN_TYPE_OPERATOR,
                            modifier_mask: 0,
                        });
                    }
                }

                // 3. Function identifier name (excluding dollar and modifiers)
                let id_span = Span::new(modifier_end, name_span.end);
                if !id_span.is_empty() {
                    let range = span_to_range(text, id_span);
                    if color_count > 0 {
                        let color_index = if constant_colors {
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            name.hash(&mut hasher);
                            (hasher.finish() as usize) % color_count
                        } else {
                            let idx = *state % color_count;
                            *state += 1;
                            idx
                        };
                        tokens.push(CustomColorToken { range, color_index });
                    }

                    let pos_id = byte_offset_to_position(text, id_span.start);
                    semantic_tokens.push(RawSemanticToken {
                        line: pos_id.line,
                        start: pos_id.character,
                        length: (id_span.end - id_span.start) as u32,
                        token_type: TOKEN_TYPE_FUNCTION,
                        modifier_mask: 0,
                    });
                }

                // 4. Arguments and separators
                if let Some(arguments) = args {
                    if arguments.len() > 1 {
                        for k in 0..arguments.len() - 1 {
                            let sep_start = arguments[k].span.end;
                            let sep_span = Span::new(sep_start, sep_start + 1);
                            let range = span_to_range(text, sep_span);
                            separator_tokens.push(SimpleColorToken { range });

                            let pos_sep = byte_offset_to_position(text, sep_span.start);
                            semantic_tokens.push(RawSemanticToken {
                                line: pos_sep.line,
                                start: pos_sep.character,
                                length: 1,
                                token_type: TOKEN_TYPE_SEPARATOR,
                                modifier_mask: 0,
                            });
                        }
                    }

                    for arg in arguments {
                        for part in &arg.parts {
                            collect_all_decorations_and_semantic_tokens(
                                part,
                                text,
                                tokens,
                                text_tokens,
                                time_tokens,
                                number_tokens,
                                dollar_tokens,
                                modifier_tokens,
                                boolean_tokens,
                                separator_tokens,
                                semantic_tokens,
                                color_count,
                                state,
                                constant_colors,
                                true, // inside function args → always inside a code block
                            );
                        }
                    }
                }
            }
        }
        AstNode::Text { content, span } => {
            // Only emit text/number/time/boolean tokens when inside a code block.
            // For JS/TS files, top-level Text nodes are raw JS/TS source — not Forge content.
            if !inside_code_block {
                return;
            }

            let (times, numbers, booleans) = get_sub_tokens_in_text(content, span.start);

            for t_span in &times {
                time_tokens.push(SimpleColorToken {
                    range: span_to_range(text, *t_span),
                });
                let pos = byte_offset_to_position(text, t_span.start);
                semantic_tokens.push(RawSemanticToken {
                    line: pos.line,
                    start: pos.character,
                    length: (t_span.end - t_span.start) as u32,
                    token_type: TOKEN_TYPE_TIME,
                    modifier_mask: 0,
                });
            }

            for n_span in &numbers {
                number_tokens.push(SimpleColorToken {
                    range: span_to_range(text, *n_span),
                });
                let pos = byte_offset_to_position(text, n_span.start);
                semantic_tokens.push(RawSemanticToken {
                    line: pos.line,
                    start: pos.character,
                    length: (n_span.end - n_span.start) as u32,
                    token_type: TOKEN_TYPE_NUMBER,
                    modifier_mask: 0,
                });
            }

            for b_span in &booleans {
                boolean_tokens.push(SimpleColorToken {
                    range: span_to_range(text, *b_span),
                });
                let pos = byte_offset_to_position(text, b_span.start);
                semantic_tokens.push(RawSemanticToken {
                    line: pos.line,
                    start: pos.character,
                    length: (b_span.end - b_span.start) as u32,
                    token_type: TOKEN_TYPE_BOOLEAN,
                    modifier_mask: 0,
                });
            }

            let mut exclude_spans = Vec::new();
            exclude_spans.extend(times);
            exclude_spans.extend(numbers);
            exclude_spans.extend(booleans);

            let string_spans = get_string_spans(*span, exclude_spans);
            for s_span in string_spans {
                if s_span.is_empty() {
                    continue;
                }
                let substring = &text[s_span.start..s_span.end];
                if !substring.trim().is_empty() {
                    text_tokens.push(SimpleColorToken {
                        range: span_to_range(text, s_span),
                    });
                    let pos = byte_offset_to_position(text, s_span.start);
                    semantic_tokens.push(RawSemanticToken {
                        line: pos.line,
                        start: pos.character,
                        length: (s_span.end - s_span.start) as u32,
                        token_type: TOKEN_TYPE_STRING,
                        modifier_mask: 0,
                    });
                }
            }
        }
        AstNode::Escaped { name, span, .. } => {
            if name.eq_ignore_ascii_case("c") || name.eq_ignore_ascii_case("$c") {
                let pos = byte_offset_to_position(text, span.start);
                semantic_tokens.push(RawSemanticToken {
                    line: pos.line,
                    start: pos.character,
                    length: (span.end - span.start) as u32,
                    token_type: TOKEN_TYPE_COMMENT,
                    modifier_mask: 0,
                });
            }
        }
        _ => {}
    }
}

// ============================================================================
// Folding Range helpers
// ============================================================================

fn collect_folding_ranges(node: &AstNode, text: &str, ranges: &mut Vec<lsp_types::FoldingRange>) {
    match node {
        AstNode::Program { body, .. } => {
            for child in body {
                collect_folding_ranges(child, text, ranges);
            }
        }
        AstNode::FunctionCall { span, args, .. } => {
            let start_pos = byte_offset_to_position(text, span.start);
            let end_pos = byte_offset_to_position(text, span.end);

            if start_pos.line < end_pos.line {
                ranges.push(lsp_types::FoldingRange {
                    start_line: start_pos.line,
                    start_character: Some(start_pos.character),
                    end_line: end_pos.line - 1,
                    end_character: None,
                    kind: Some(lsp_types::FoldingRangeKind::Region),
                    ..lsp_types::FoldingRange::default()
                });
            }

            if let Some(args) = args {
                for arg in args {
                    for part in &arg.parts {
                        collect_folding_ranges(part, text, ranges);
                    }
                }
            }
        }
        _ => {}
    }
}
// ============================================================================
// Custom function loading (standalone functions to avoid &self lifetime issues)
// ============================================================================

async fn perform_load_custom_functions(
    metadata: Arc<MetadataManager>,
    client: Client,
    custom_functions_json: Option<String>,
    custom_functions_path: Option<Vec<String>>,
) {
    let mut all_functions: Vec<serde_json::Value> = Vec::new();

    if let Some(json_path) = &custom_functions_json {
        let path = PathBuf::from(json_path);
        match std::fs::read_to_string(&path) {
            Ok(json_str) => match serde_json::from_str::<Vec<serde_json::Value>>(&json_str) {
                Ok(mut functions) => {
                    all_functions.append(&mut functions);
                    let _ = client
                        .log_message(
                            lsp_types::MessageType::INFO,
                            format!(
                                "Loaded {} custom function(s) from JSON: {}",
                                functions.len(),
                                path.display()
                            ),
                        )
                        .await;
                }
                Err(e) => {
                    let _ = client
                        .log_message(
                            lsp_types::MessageType::WARNING,
                            format!(
                                "Failed to parse custom-functions JSON at {}: {}",
                                path.display(),
                                e
                            ),
                        )
                        .await;
                }
            },
            Err(e) => {
                let _ = client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!(
                            "Failed to read custom-functions JSON file at {}: {}",
                            path.display(),
                            e
                        ),
                    )
                    .await;
            }
        }
    }

    if let Some(paths) = &custom_functions_path {
        for folder_path in paths {
            let folder = PathBuf::from(&folder_path);
            if folder.exists() && folder.is_dir() {
                match metadata.generate_custom_functions_json(&folder) {
                    Ok(json_str) => match serde_json::from_str::<Vec<serde_json::Value>>(&json_str)
                    {
                        Ok(mut functions) => {
                            let count = functions.len();
                            all_functions.append(&mut functions);
                            let _ = client
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
                            let _ = client
                                .log_message(
                                    lsp_types::MessageType::WARNING,
                                    format!(
                                        "Failed to parse generated JSON from {}: {}",
                                        folder.display(),
                                        e
                                    ),
                                )
                                .await;
                        }
                    },
                    Err(e) => {
                        let _ = client
                            .log_message(
                                lsp_types::MessageType::WARNING,
                                format!(
                                    "Failed to generate custom functions from {}: {}",
                                    folder.display(),
                                    e
                                ),
                            )
                            .await;
                    }
                }
            } else {
                let _ = client
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

    if !all_functions.is_empty() {
        match serde_json::to_string(&all_functions) {
            Ok(final_json) => match metadata.add_custom_functions_from_json(&final_json) {
                Ok(count) => {
                    let _ = client
                        .log_message(
                            lsp_types::MessageType::INFO,
                            format!("Registered {} total custom function(s)", count),
                        )
                        .await;
                }
                Err(e) => {
                    let _ = client
                        .log_message(
                            lsp_types::MessageType::WARNING,
                            format!("Failed to register custom functions: {}", e),
                        )
                        .await;
                }
            },
            Err(e) => {
                let _ = client
                    .log_message(
                        lsp_types::MessageType::WARNING,
                        format!("Failed to serialize aggregated custom functions: {}", e),
                    )
                    .await;
            }
        }
    } else {
        metadata.remove_custom_functions();
    }
}
