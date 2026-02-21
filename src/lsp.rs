use forge_kit::metadata::MetadataManager;
use forge_kit::metadata::MetadataSource;
use forge_kit::parser::{self, AstNode, ParseError, Span, ValidationConfig};
use forge_kit::types::{Arg, Function};
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

/// Top-level initialization options sent by the extension via `initializationOptions`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ForgeConfig {
    metadata_urls: Option<Vec<MetadataUrlConfig>>,
    custom_functions_path: Option<String>,
    custom_functions_json: Option<String>,
}

// ============================================================================
// Parse Cache
// ============================================================================

/// Cached result of a single parse pass for one document.
///
/// We parse *once* per `did_open`/`did_change` and store the AST here
/// so that completion, signature-help, and hover all share the same parse
/// without re-running the parser a second (or third) time.
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
}

impl ForgeLanguageServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            metadata: Arc::new(MetadataManager::new()),
            documents: RwLock::new(HashMap::new()),
            parse_cache: RwLock::new(HashMap::new()),
            pending_config: Mutex::new(None),
        }
    }

    /// Re-parse the document and update the cache + publish diagnostics.
    async fn refresh(&self, uri: lsp_types::Url, text: String) {
        let is_js_ts = uri.path().ends_with(".js") || uri.path().ends_with(".ts");
        let config = ValidationConfig::strict();

        // Single parse — used for both the cache and diagnostics.
        let (ast, errors) = if is_js_ts {
            parser::parse_with_validation(&text, config, self.metadata.clone())
        } else {
            parser::parse_forge_script_with_validation(&text, config, self.metadata.clone())
        };

        // Cache the AST.
        {
            let mut cache = self.parse_cache.write().await;
            cache.insert(uri.clone(), CachedParse { ast });
        }

        // Publish diagnostics from the same parse.
        let diagnostics: Vec<lsp_types::Diagnostic> = errors
            .into_iter()
            .map(|e| parse_error_to_diagnostic(&text, e))
            .collect();
        self.client
            .publish_diagnostics(uri, diagnostics, None)
            .await;
    }

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
                    // Store text before refresh (which also needs the text in docs map)
                    {
                        let mut docs = self.documents.write().await;
                        docs.insert(uri.clone(), text.clone());
                    }
                    self.refresh(uri, text).await;
                }
            }
        }
    }

    /// Load custom functions from whichever sources `config` describes.
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

        Ok(lsp_types::InitializeResult {
            capabilities: lsp_types::ServerCapabilities {
                text_document_sync: Some(lsp_types::TextDocumentSyncCapability::Kind(
                    lsp_types::TextDocumentSyncKind::FULL,
                )),
                // Completion: trigger on '$' so the user immediately gets suggestions
                completion_provider: Some(lsp_types::CompletionOptions {
                    trigger_characters: Some(vec!["$".to_string()]),
                    resolve_provider: Some(false),
                    ..lsp_types::CompletionOptions::default()
                }),
                // Hover over function names
                hover_provider: Some(lsp_types::HoverProviderCapability::Simple(true)),
                // Signature help: trigger on '[' (open args) and ';' (next arg)
                signature_help_provider: Some(lsp_types::SignatureHelpOptions {
                    trigger_characters: Some(vec!["[".to_string(), ";".to_string()]),
                    retrigger_characters: Some(vec![";".to_string()]),
                    ..lsp_types::SignatureHelpOptions::default()
                }),
                ..lsp_types::ServerCapabilities::default()
            },
            ..lsp_types::InitializeResult::default()
        })
    }

    async fn initialized(&self, _: lsp_types::InitializedParams) {
        self.client
            .log_message(lsp_types::MessageType::INFO, "ForgeLSP initialized")
            .await;

        let config = self.pending_config.lock().await.take();

        if let Some(config) = config {
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
                                format!("Metadata: {}", stats),
                            )
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

            self.load_custom_functions(&config).await;
        }

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

        // Get document text.
        let text = {
            let docs = self.documents.read().await;
            match docs.get(uri) {
                Some(t) => t.clone(),
                None => return Ok(None),
            }
        };

        let cursor_offset = position_to_byte_offset(&text, pos);

        // ── Check if cursor is inside a function's argument brackets ──────────
        if let Some(ctx) = find_arg_context(&text, cursor_offset) {
            // Look up the function in metadata
            let func_name = format!("${}", ctx.func_name);
            if let Some(func) = self.metadata.get(&func_name) {
                // Try to find enum values for the current argument
                if let Some(enum_items) = get_enum_for_arg(&func, ctx.arg_index, &self.metadata) {
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
            }
            // Inside brackets but no enum — don't show function completions
            return Ok(None);
        }

        // ── Function name completion: find the `$prefix` being typed ──────────
        let prefix = extract_dollar_prefix(&text, cursor_offset);
        let search_prefix = format!("${}", prefix);

        let functions = self.metadata.get_completions(&search_prefix);
        if functions.is_empty() && !prefix.is_empty() {
            // No matches — nothing to show
            return Ok(None);
        }

        // If prefix is empty return all functions (triggered by '$')
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

        // Use cached AST to find the function call under the cursor
        let func_name = {
            let cache = self.parse_cache.read().await;
            match cache.get(uri) {
                Some(cached) => find_function_at(&cached.ast, cursor_offset),
                None => None,
            }
        };

        let func_name = match func_name {
            Some(n) => n,
            None => {
                // Fallback: scan text directly for hovered function name
                match find_function_name_at_offset(&text, cursor_offset) {
                    Some(n) => n,
                    None => return Ok(None),
                }
            }
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

        // Use cached AST to find the function call whose args-span contains cursor
        let call_info = {
            let cache = self.parse_cache.read().await;
            match cache.get(uri) {
                Some(cached) => find_call_for_sig_help(&cached.ast, cursor_offset),
                None => None,
            }
        };

        // Fallback: scan text directly
        let call_info = match call_info {
            Some(c) => c,
            None => match find_call_from_text(&text, cursor_offset) {
                Some(c) => c,
                None => return Ok(None),
            },
        };

        let func = match self.metadata.get(&call_info.func_name) {
            Some(f) => f,
            None => return Ok(None),
        };

        let sig = build_signature_info(&func, &self.metadata);
        let active_param = call_info.arg_index as u32;

        Ok(Some(lsp_types::SignatureHelp {
            signatures: vec![sig],
            active_signature: Some(0),
            active_parameter: Some(active_param),
        }))
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

/// Convert an LSP `Position` (line + UTF-16 column) to a byte offset into `text`.
fn position_to_byte_offset(text: &str, pos: lsp_types::Position) -> usize {
    let mut current_line = 0u32;
    let mut line_start = 0usize;

    for (idx, ch) in text.char_indices() {
        if current_line == pos.line {
            // Walk forward `pos.character` UTF-16 code units on this line
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

    // If we land after all lines, return end of text
    text.len()
}

// ============================================================================
// Completion helpers
// ============================================================================

/// Scan backwards from `cursor` to extract the identifier fragment
/// immediately after the last unescaped `$`.
/// Returns the fragment WITHOUT the `$`, e.g. "sen" for `$sen`.
/// Returns "" if cursor is right after `$`.
fn extract_dollar_prefix(text: &str, cursor: usize) -> String {
    let bytes = text.as_bytes();
    let cursor = cursor.min(bytes.len());

    // Walk back over alphanumeric / underscore to find identifier chars
    let end = cursor;
    let mut start = cursor;
    while start > 0 {
        let b = bytes[start - 1];
        if b.is_ascii_alphanumeric() || b == b'_' {
            start -= 1;
        } else {
            break;
        }
    }

    // There must be a `$` right before the identifier start
    if start > 0 && bytes[start - 1] == b'$' {
        text[start..end].to_string()
    } else {
        String::new()
    }
}

/// Context when the cursor is inside a function's argument list.
struct ArgContext {
    func_name: String,
    arg_index: usize,
}

/// Determine whether `cursor` is inside `funcName[...]` brackets.
///
/// We scan backwards to find the matching `[` with depth-awareness,
/// then identify the function name before it.
fn find_arg_context(text: &str, cursor: usize) -> Option<ArgContext> {
    let bytes = text.as_bytes();
    let cursor = cursor.min(bytes.len());

    // Walk backwards to find the opening `[` of this argument list
    let mut depth = 0i32;
    let mut bracket_pos: Option<usize> = None;
    let mut i = cursor;

    while i > 0 {
        i -= 1;
        let b = bytes[i];

        // Simple escape heuristic: skip if preceded by backslash
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
            // Stop if we hit a newline far away (heuristic to avoid scanning entire file)
            b'\n' if cursor.saturating_sub(i) > 500 => break,
            _ => {}
        }
    }

    let bracket_pos = bracket_pos?;

    // Extract function name before `[`
    // It must be preceded by alphanumeric/underscore chars and a `$`
    let name_end = bracket_pos;
    let mut name_start = name_end;
    while name_start > 0
        && (bytes[name_start - 1].is_ascii_alphanumeric() || bytes[name_start - 1] == b'_')
    {
        name_start -= 1;
    }

    if name_start == name_end {
        return None; // No identifier before `[`
    }
    if name_start == 0 || bytes[name_start - 1] != b'$' {
        return None; // Not preceded by `$`
    }

    let func_name = text[name_start..name_end].to_string();

    // Count arg separators `;` at depth 0 between `[+1` and `cursor`
    let content = &text[bracket_pos + 1..cursor.min(text.len())];
    let arg_index = count_arg_index(content);

    Some(ArgContext {
        func_name,
        arg_index,
    })
}

/// Count the `;`-separated argument index at depth 0 in `content`.
///
/// Example: "a;b;c" → cursor at end → index 2
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

/// Get the enum values for `func`'s argument at `arg_index`.
fn get_enum_for_arg(
    func: &Function,
    arg_index: usize,
    metadata: &MetadataManager,
) -> Option<Vec<String>> {
    let args = func.args.as_ref()?;

    // Handle rest args: last arg applies to all extra indices
    let has_rest = args.iter().any(|a| a.rest);
    let arg = if arg_index < args.len() {
        &args[arg_index]
    } else if has_rest {
        args.last()?
    } else {
        return None;
    };

    // Prefer inline enum list, then named enum from metadata
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

/// Format a type value (which can be a string or array in the JSON) for display.
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

/// Build a `CompletionItem` for a function.
fn build_completion_item(func: &Function, text: &str, cursor: usize) -> lsp_types::CompletionItem {
    // The label is the full function name (e.g. `$send`)
    let label = func.name.clone();

    // insert_text: include the $ so it's not removed
    let insert_text = func.name.clone();

    // Determine the range to replace: from the '$' to the cursor
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

    // The '$' is right before the identifier
    let range = if start > 0 && bytes[start - 1] == b'$' {
        let start_pos = byte_offset_to_position(text, start - 1);
        let end_pos = byte_offset_to_position(text, cursor);
        Some(lsp_types::Range {
            start: start_pos,
            end: end_pos,
        })
    } else {
        None
    };

    // Short detail line: first sentence of description
    let detail = first_sentence(&func.description);

    // Full markdown documentation
    let doc = build_hover_markdown_for_completion(func);

    lsp_types::CompletionItem {
        label,
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
        insert_text: Some(insert_text),
        text_edit: range.map(|r| {
            lsp_types::CompletionTextEdit::Edit(lsp_types::TextEdit {
                range: r,
                new_text: func.name.clone(),
            })
        }),
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

/// Walk the AST and find the name of the innermost `FunctionCall` node
/// whose `span` contains `offset`. Returns `"$name"` form.
fn find_function_at(node: &AstNode, offset: usize) -> Option<String> {
    match node {
        AstNode::Program { body, .. } => {
            for child in body {
                if let Some(name) = find_function_at(child, offset) {
                    return Some(name);
                }
            }
            None
        }
        AstNode::FunctionCall {
            name,
            span,
            args,
            name_span: _,
            ..
        } => {
            // Hover works on the name_span region for best UX
            let in_span = offset >= span.start && offset < span.end;
            if !in_span {
                return None;
            }

            // Check nested args first for inner functions
            if let Some(args) = args {
                for arg in args {
                    for part in &arg.parts {
                        if let Some(inner) = find_function_at(part, offset) {
                            return Some(inner);
                        }
                    }
                }
            }

            // Return this node's name
            let full_name = if name.starts_with('$') {
                name.clone()
            } else {
                format!("${}", name)
            };
            Some(full_name)
        }
        _ => None,
    }
}

/// Context for signature help: the function name and active argument index.
struct CallInfo {
    func_name: String,
    arg_index: usize,
}

/// Walk AST to find the `FunctionCall` whose `args_span` contains `offset`,
/// returning the function name and active argument index.
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
            // Check nested args first
            if let Some(args) = args {
                for arg in args {
                    for part in &arg.parts {
                        if let Some(inner) = find_call_for_sig_help(part, offset) {
                            return Some(inner);
                        }
                    }
                }
            }

            // Is cursor inside our args_span?
            if let Some(aspan) = args_span {
                if offset >= aspan.start && offset <= aspan.end {
                    let func_name = if name.starts_with('$') {
                        name.clone()
                    } else {
                        format!("${}", name)
                    };

                    // Count arg index from args list
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
// Text-based fallback helpers (when AST is not available)
// ============================================================================

/// Fallback: scan text to find a function name `$name` that the cursor is over.
fn find_function_name_at_offset(text: &str, offset: usize) -> Option<String> {
    let bytes = text.as_bytes();
    let offset = offset.min(bytes.len());

    // Find the start of the word under cursor
    let mut end = offset;
    // Extend end rightward
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }

    // Walk left to find the start
    let mut start = offset;
    while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        start -= 1;
    }

    // Check for `$` before start
    if start > 0 && bytes[start - 1] == b'$' {
        let name = &text[start..end];
        if !name.is_empty() {
            return Some(format!("${}", name));
        }
    }
    None
}

/// Fallback: scan text backwards from cursor to find an open function call.
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

/// Build rich hover markdown for a function.
pub fn build_hover_markdown(func: &Function) -> String {
    let mut md = String::new();

    // ── Header (Usage) ──────────────────────────────────────────────────────
    let usage = build_usage_line_v2(func);
    md.push_str(&format!("```forge\n{}\n```", usage));

    md.push_str("\n---\n");

    // ── Description ─────────────────────────────────────────────────────────
    if !func.description.is_empty() {
        md.push('\n');
        md.push_str(&func.description);
        md.push('\n');
    }

    // ── Category / Extension ────────────────────────────────────────────────
    md.push('\n');
    let mut badges: Vec<String> = Vec::new();
    if let Some(ext) = &func.extension {
        badges.push(format!("Extension: *{}*", ext));
    }
    if let Some(cat) = &func.category {
        badges.push(format!("Category: *{}*", cat));
    }
    if !badges.is_empty() {
        md.push_str(&badges.join(" · "));
        md.push('\n');
    }

    // ── Status flags ────────────────────────────────────────────────────────
    if func.deprecated.unwrap_or(false) {
        md.push_str("\n> ⚠️ **Deprecated**\n");
    }
    if func.experimental.unwrap_or(false) {
        md.push_str("\n> 🧪 **Experimental**\n");
    }

    // ── Links ───────────────────────────────────────────────────────────────
    md.push('\n');
    if let Some(url) = &func.source_url {
        if let Some(github) = extract_github_url(url) {
            md.push_str(&format!("[Github]({})\n\n", github));
        }
    }

    if let Some(ext) = &func.extension {
        md.push_str(&format!(
            "[Documentation](https://docs.botforge.org/function/?p={})\n",
            ext
        ));
    }

    // ── Aliases ─────────────────────────────────────────────────────────────
    if let Some(aliases) = &func.aliases {
        if !aliases.is_empty() {
            md.push('\n');
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
            md.push_str(&format!("**Aliases:** {}\n", alias_list.join(", ")));
        }
    }

    md
}

fn extract_github_url(source_url: &str) -> Option<String> {
    if source_url.contains("raw.githubusercontent.com") {
        let parts: Vec<&str> = source_url.split('/').collect();
        // https: / / raw.githubusercontent.com / user / repo / branch / ...
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

/// Build the same hover markdown for use in completion documentation.
/// Slightly more compact (no `---` separator).
fn build_hover_markdown_for_completion(func: &Function) -> String {
    let mut md = String::new();

    if !func.description.is_empty() {
        md.push_str(&func.description);
        md.push('\n');
    }

    if let Some(args) = &func.args {
        if !args.is_empty() {
            md.push('\n');
            md.push_str("**Arguments**\n\n");
            md.push_str("| Name | Type | Required |\n");
            md.push_str("|------|------|:--------:|\n");
            for arg in args {
                let name = if arg.rest {
                    format!("{}...", arg.name)
                } else {
                    arg.name.clone()
                };
                let ty = format_arg_type(&arg.arg_type);
                let req = if arg.required.unwrap_or(false) {
                    "✓"
                } else {
                    ""
                };
                md.push_str(&format!("| `{}` | {} | {} |\n", name, ty, req));
            }
        }
    }

    if let Some(output) = &func.output {
        if !output.is_empty() {
            md.push('\n');
            md.push_str(&format!("**Returns:** `{}`\n", output.join("` | `")));
        }
    }

    md
}

/// Build enum value hint string for an argument (e.g. "one of: `a`, `b`").
fn build_enum_hint(arg: &Arg, metadata: &MetadataManager) -> String {
    let values = if let Some(inline) = &arg.arg_enum {
        if !inline.is_empty() {
            Some(inline.clone())
        } else {
            None
        }
    } else if let Some(enum_name) = &arg.enum_name {
        metadata.get_enum(enum_name)
    } else {
        None
    };

    if let Some(vals) = values {
        if vals.is_empty() {
            return String::new();
        }
        // Show at most 8 values inline, then indicate there are more
        if vals.len() <= 8 {
            let quoted: Vec<String> = vals.iter().map(|v| format!("`{}`", v)).collect();
            format!("one of: {}", quoted.join(", "))
        } else {
            let first8: Vec<String> = vals.iter().take(8).map(|v| format!("`{}`", v)).collect();
            format!("one of: {}, … ({} total)", first8.join(", "), vals.len())
        }
    } else {
        String::new()
    }
}

/// Build the refined usage line.
/// Format: $function[Arg: Type; Arg: Type; ?OptionalArg: Type; ...RestArg: Type]
fn build_usage_line_v2(func: &Function) -> String {
    let has_brackets = func.brackets.unwrap_or(false);

    if !has_brackets {
        return func.name.clone();
    }

    match &func.args {
        Some(vec) if !vec.is_empty() => {
            let arg_parts: Vec<String> = vec
                .iter()
                .map(|a| {
                    let mut prefix = String::new();
                    if a.rest {
                        prefix.push_str("...");
                    } else if !a.required.unwrap_or(false) {
                        prefix.push('?');
                    }

                    let ty = format_arg_type(&a.arg_type);
                    format!("{}{}: {}", prefix, a.name, ty)
                })
                .collect();
            format!("{}[{}]", func.name, arg_parts.join("; "))
        }
        _ => {
            if has_brackets {
                format!("{}[]", func.name)
            } else {
                func.name.clone()
            }
        }
    }
}

/// Build a `SignatureInformation` for signature help.
fn build_signature_info(
    func: &Function,
    metadata: &MetadataManager,
) -> lsp_types::SignatureInformation {
    // Build the full label, e.g.  $send[channel: String; message: String; ?color: String]
    let label = build_usage_line_v2(func);

    // Build per-parameter info and figure out byte offsets into `label`
    let mut parameters: Vec<lsp_types::ParameterInformation> = Vec::new();

    if let Some(args) = &func.args {
        let mut current_offset = func.name.len() + 1; // Start after '$func['

        for (idx, arg) in args.iter().enumerate() {
            let mut prefix = String::new();
            if arg.rest {
                prefix.push_str("...");
            } else if !arg.required.unwrap_or(false) {
                prefix.push('?');
            }

            let ty = format_arg_type(&arg.arg_type);
            let rendered = format!("{}{}: {}", prefix, arg.name, ty);

            // Find this rendered arg in label starting from current_offset
            let param_start = current_offset;
            let param_end = param_start + rendered.len();

            let mut doc_parts = Vec::new();
            if !arg.description.is_empty() {
                doc_parts.push(arg.description.clone());
            }
            let enum_hint = build_enum_hint(arg, metadata);
            if !enum_hint.is_empty() {
                doc_parts.push(enum_hint);
            }

            let doc = if doc_parts.is_empty() {
                None
            } else {
                Some(lsp_types::Documentation::MarkupContent(
                    lsp_types::MarkupContent {
                        kind: lsp_types::MarkupKind::Markdown,
                        value: doc_parts.join("\n\n"),
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

            // Advance: rendered arg + "; " separator (except last)
            current_offset = param_end;
            if idx + 1 < args.len() {
                current_offset += 2; // "; "
            }
        }
    }

    lsp_types::SignatureInformation {
        label: label.clone(),
        documentation: Some(lsp_types::Documentation::MarkupContent(
            lsp_types::MarkupContent {
                kind: lsp_types::MarkupKind::Markdown,
                value: format!("{}\n\n---", label),
            },
        )),
        parameters: Some(parameters),
        active_parameter: None,
    }
}
