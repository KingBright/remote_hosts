//! Bounded reads and optimistic edits confined by directory capabilities.
use crate::{hash, random};
use anyhow::{Context, Result, bail, ensure};
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

const MAX_FILE: u64 = 8 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub device_id: String,
    pub root: PathBuf,
}
pub fn relative(path: &str) -> Result<&Path> {
    let p = Path::new(path);
    ensure!(
        !path.is_empty()
            && path.len() <= 4096
            && !path.chars().any(char::is_control)
            && p.components().all(|c| matches!(c, Component::Normal(_))),
        "path must be a non-empty relative path without . or .."
    );
    Ok(p)
}
pub(crate) fn directory(ws: &Workspace) -> Result<Dir> {
    Ok(Dir::open_ambient_dir(
        &ws.root,
        cap_std::ambient_authority(),
    )?)
}
pub(crate) fn read_text(dir: &Dir, path: &str) -> Result<String> {
    let mut file = dir.open(relative(path)?)?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() <= MAX_FILE,
        "not a regular text file or file exceeds 8 MiB"
    );
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_FILE + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_FILE && !bytes.contains(&0),
        "binary or oversized file"
    );
    String::from_utf8(bytes).context("file is not UTF-8")
}
pub fn bounded(s: &str, max: usize) -> (&str, bool) {
    let mut end = max.min(s.len());
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], end < s.len())
}
pub fn number(v: &Value, key: &str, default: usize, min: usize, max: usize) -> Result<usize> {
    let n = match v.get(key) {
        None => default,
        Some(n) => usize::try_from(n.as_u64().context("expected positive integer")?)?,
    };
    ensure!((min..=max).contains(&n), "{key} outside allowed bounds");
    Ok(n)
}
pub fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string {key}"))
}

fn paths(ws: &Workspace, v: &Value) -> Result<Vec<String>> {
    let glob = v.get("glob").and_then(Value::as_str).unwrap_or("**/*");
    ensure!(glob.len() <= 1024, "glob too long");
    let matcher = globset::Glob::new(glob)?.compile_matcher();
    relative(glob)?;
    // A path filter must also bound traversal, not merely filter a full-repository scan.
    let components: Vec<_> = glob.split('/').collect();
    let mut anchor = ws.root.clone();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        if component.contains(['*', '?', '[', '{']) {
            break;
        }
        anchor.push(component);
    }
    if !anchor.exists() {
        return Ok(vec![]);
    }
    ensure!(
        anchor.canonicalize()?.starts_with(&ws.root),
        "glob anchor escapes workspace"
    );
    let ignored = v
        .get("include_ignored")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut builder = ignore::WalkBuilder::new(&anchor);
    builder
        .hidden(false)
        .follow_links(false)
        .require_git(false)
        .git_ignore(!ignored)
        .git_global(!ignored)
        .git_exclude(!ignored)
        .ignore(!ignored)
        .filter_entry(|e| e.file_name() != ".git");
    let mut out = vec![];
    let start = std::time::Instant::now();
    for entry in builder.build() {
        ensure!(
            start.elapsed().as_secs() < 15,
            "file enumeration exceeded 15 seconds; narrow the glob"
        );
        let entry = entry?;
        if entry.file_type().is_some_and(|t| t.is_file()) {
            let p = entry
                .path()
                .strip_prefix(&ws.root)?
                .to_string_lossy()
                .replace('\\', "/");
            if p.split('/').any(|s| s == ".git") {
                continue;
            }
            if matcher.is_match(&p) {
                out.push(p);
            }
            ensure!(out.len() <= 100000, "too many files; narrow the glob");
        }
    }
    out.sort();
    Ok(out)
}
fn page_offset(v: &Value, fingerprint: &str) -> Result<usize> {
    let Some(c) = v.get("cursor").and_then(Value::as_str) else {
        return Ok(0);
    };
    let (version, offset) = c.split_once(':').context("invalid cursor")?;
    ensure!(
        version == fingerprint,
        "stale cursor; repeat query without cursor"
    );
    Ok(offset.parse()?)
}
fn fingerprint(v: &Value, paths: &[String]) -> String {
    let mut query = v.clone();
    if let Some(o) = query.as_object_mut() {
        o.remove("cursor");
    }
    hash(format!("{query}:{}", paths.join("\n")))
}

pub fn list(ws: &Workspace, v: &Value) -> Result<Value> {
    let paths = paths(ws, v)?;
    let fp = fingerprint(v, &paths);
    let offset = page_offset(v, &fp)?;
    let limit = number(v, "limit", 100, 1, 500)?;
    let mut end = offset.saturating_add(limit).min(paths.len());
    ensure!(offset <= end, "invalid cursor offset");
    let mut budget = 0;
    for (i, p) in paths[offset..end].iter().enumerate() {
        budget += p.len() + 8;
        if budget > 48000 {
            end = offset + i;
            break;
        }
    }
    Ok(
        json!({"files":paths[offset..end],"next_cursor":(end<paths.len()).then(||format!("{fp}:{end}")),"total":paths.len()}),
    )
}
pub fn read(ws: &Workspace, v: &Value) -> Result<Value> {
    crate::reads::read(ws, v)
}
struct SearchSnippet {
    text: String,
    first: usize,
    last: usize,
    byte_offset: usize,
    truncated: bool,
    match_truncated: bool,
}

// Reserve the actual match before adding context. A long preceding line must not
// consume the response and turn a reported match into an unrelated text fragment.
fn search_snippet(
    lines: &[&str],
    index: usize,
    hit: std::ops::Range<usize>,
    context: usize,
    budget: usize,
) -> SearchSnippet {
    let line = lines[index];
    let wanted_first = index.saturating_sub(context);
    let wanted_last = (index + context + 1).min(lines.len());
    if line.len() > budget {
        let mut offset = hit
            .start
            .saturating_sub(budget.saturating_sub(hit.len()) / 2);
        while !line.is_char_boundary(offset) {
            offset += 1;
        }
        let (text, _) = bounded(&line[offset..], budget);
        return SearchSnippet {
            text: text.into(),
            first: index,
            last: index + 1,
            byte_offset: offset,
            truncated: true,
            match_truncated: hit.end > offset + text.len(),
        };
    }
    let mut first = index;
    let mut last = index + 1;
    let mut size = line.len();
    while first > wanted_first && size + lines[first - 1].len() < budget {
        first -= 1;
        size += lines[first].len() + 1;
    }
    while last < wanted_last && size + lines[last].len() < budget {
        size += lines[last].len() + 1;
        last += 1;
    }
    SearchSnippet {
        text: lines[first..last].join("\n"),
        first,
        last,
        byte_offset: 0,
        truncated: first != wanted_first || last != wanted_last,
        match_truncated: false,
    }
}

pub fn search(ws: &Workspace, v: &Value) -> Result<Value> {
    let query = text(v, "query")?;
    ensure!(
        !query.is_empty() && query.len() <= 4096,
        "query must be 1..4096 bytes"
    );
    let regex =
        regex::RegexBuilder::new(&if v.get("regex").and_then(Value::as_bool) == Some(true) {
            query.into()
        } else {
            regex::escape(query)
        })
        .size_limit(2 * 1024 * 1024)
        .build()?;
    let paths = paths(ws, v)?;
    let fp = fingerprint(v, &paths);
    let offset = page_offset(v, &fp)?;
    let limit = number(v, "limit", 40, 1, 200)?;
    let context = number(v, "context_lines", 2, 0, 5)?;
    let mut remaining = number(v, "max_bytes", 16000, 1024, 65536)?;
    let dir = directory(ws)?;
    let mut results = vec![];
    let mut count = 0;
    let mut skipped = 0;
    let start = std::time::Instant::now();
    for path in &paths {
        ensure!(
            start.elapsed().as_secs() < 15,
            "search exceeded 15 seconds; narrow the glob"
        );
        let content = match read_text(&dir, path) {
            Ok(s) => s,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let lines: Vec<_> = content.lines().collect();
        let version = hash(&content);
        for (i, line) in lines.iter().enumerate() {
            let Some(hit) = regex.find(line) else {
                continue;
            };
            count += 1;
            if count <= offset {
                continue;
            }
            if results.len() >= limit || remaining < 1024 {
                return Ok(
                    json!({"matches":results,"next_cursor":format!("{fp}:{}",count-1),"skipped_files":skipped,"consistency":"live files; repeat query after edits"}),
                );
            }
            let snippet = search_snippet(&lines, i, hit.range(), context, remaining.min(8192));
            remaining = remaining.saturating_sub(snippet.text.len() + path.len() + 192);
            let mut result = json!({"path":path,"line":i+1,"start_line":snippet.first+1,"end_line":snippet.last,"text":snippet.text,"truncated":snippet.truncated,"version":version});
            if snippet.byte_offset > 0 || snippet.match_truncated {
                result["line_byte_offset"] = json!(snippet.byte_offset);
                result["match_truncated"] = json!(snippet.match_truncated);
            }
            results.push(result);
        }
    }
    Ok(json!({"matches":results,"next_cursor":null,"skipped_files":skipped}))
}
pub fn symbols(ws: &Workspace, v: &Value) -> Result<Value> {
    let path = text(v, "path")?;
    let content = read_text(&directory(ws)?, path)?;
    let start = number(v, "start_line", 1, 1, 10000000)?;
    let limit = number(v, "limit", 100, 1, 300)?;
    let language = match Path::new(path).extension().and_then(|x| x.to_str()) {
        Some("rs") => Some(tree_sitter_rust::LANGUAGE.into()),
        Some("py") => Some(tree_sitter_python::LANGUAGE.into()),
        Some("js" | "jsx" | "mjs" | "cjs") => Some(tree_sitter_javascript::LANGUAGE.into()),
        Some("ts") => Some(tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()),
        Some("tsx") => Some(tree_sitter_typescript::LANGUAGE_TSX.into()),
        _ => None,
    };
    if let Some(language) = language {
        let mut parser = tree_sitter::Parser::new();
        parser.set_language(&language)?;
        let tree = parser.parse(&content, None).context("parser cancelled")?;
        let mut nodes = vec![tree.root_node()];
        let mut symbols = vec![];
        while let Some(node) = nodes.pop() {
            if node.start_position().row + 1 >= start
                && matches!(
                    node.kind(),
                    "function_item"
                        | "struct_item"
                        | "enum_item"
                        | "trait_item"
                        | "impl_item"
                        | "mod_item"
                        | "type_item"
                        | "const_item"
                        | "function_definition"
                        | "class_definition"
                        | "function_declaration"
                        | "class_declaration"
                        | "method_definition"
                        | "interface_declaration"
                        | "type_alias_declaration"
                        | "lexical_declaration"
                )
            {
                symbols.push(json!({"kind":node.kind(),"name":node.child_by_field_name("name").or_else(||node.child_by_field_name("type")).and_then(|n|n.utf8_text(content.as_bytes()).ok()).map(|n|bounded(n,200).0),"start_line":node.start_position().row+1,"end_line":node.end_position().row+1,"declaration":bounded(node.utf8_text(content.as_bytes())?.lines().next().unwrap_or(""),300).0}));
                if symbols.len() > limit {
                    break;
                }
            }
            for i in (0..node.named_child_count()).rev() {
                if let Some(child) = node.named_child(i) {
                    nodes.push(child);
                }
            }
        }
        let next = symbols
            .get(limit)
            .and_then(|s| s.get("start_line"))
            .cloned();
        symbols.truncate(limit);
        return Ok(
            json!({"path":path,"version":hash(&content),"kind":"syntax_tree","has_parse_errors":tree.root_node().has_error(),"symbols":symbols,"next_line":next}),
        );
    }
    let re = regex::Regex::new(
        r"^\s*(?:(?:pub(?:\([^)]*\))?|export|default|async|public|private|protected|static|final|abstract)\s+)*(?:fn|struct|enum|trait|impl|mod|class|def|interface|type|function|func|const)\b",
    )?;
    let matches: Vec<_> = content
        .lines()
        .enumerate()
        .skip(start - 1)
        .filter(|(_, l)| re.is_match(l))
        .take(limit + 1)
        .collect();
    Ok(
        json!({"path":path,"version":hash(&content),"kind":"lexical_outline","symbols":matches.iter().take(limit).map(|(i,l)|json!({"line":i+1,"declaration":bounded(l,300).0})).collect::<Vec<_>>(),"next_line":matches.get(limit).map(|(i,_)|i+1)}),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Replacement {
    old_text: String,
    new_text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileEdit {
    path: String,
    expected_version: String,
    action: Option<String>,
    edits: Option<Vec<Replacement>>,
    patch: Option<String>,
    content: Option<String>,
}
struct Prepared {
    path: String,
    old: Option<String>,
    new: Option<String>,
}
fn transformed(old: &str, edit: &FileEdit) -> Result<String> {
    ensure!(
        edit.content.is_none(),
        "content only supported when creating a new file"
    );
    if let Some(patch) = &edit.patch {
        ensure!(edit.edits.is_none(), "choose edits or patch, not both");
        return Ok(diffy::apply(old, &diffy::Patch::from_str(patch)?)?);
    }
    let edits = edit.edits.as_ref().context("missing edits or patch")?;
    ensure!(
        !edits.is_empty() && edits.len() <= 100,
        "expected 1..100 edits"
    );
    let mut replacements = vec![];
    for e in edits {
        ensure!(!e.old_text.is_empty(), "old_text cannot be empty");
        let positions: Vec<_> = old.match_indices(&e.old_text).map(|(i, _)| i).collect();
        ensure!(
            positions.len() == 1,
            "match_conflict: {} matches in {}",
            positions.len(),
            edit.path
        );
        replacements.push((
            positions[0],
            positions[0] + e.old_text.len(),
            e.new_text.as_str(),
        ));
    }
    replacements.sort_by_key(|x| x.0);
    ensure!(
        replacements.windows(2).all(|w| w[0].1 <= w[1].0),
        "overlapping edits"
    );
    let mut out = old.to_owned();
    for (a, b, new) in replacements.into_iter().rev() {
        out.replace_range(a..b, new)
    }
    Ok(out)
}
fn atomic_write(dir: &Dir, path: &str, content: &str) -> Result<()> {
    let path = relative(path)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    dir.create_dir_all(parent)?;
    let sub = dir.open_dir(parent)?;
    let name = path.file_name().context("missing filename")?;
    let temp = format!(".remote-hosts-{}.tmp", random());
    let mut opts = cap_std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    let mut f = sub.open_with(&temp, &opts)?;
    let result = (|| -> Result<()> {
        if let Ok(meta) = sub.metadata(name) {
            f.set_permissions(meta.permissions())?;
        }
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
        sub.rename(&temp, &sub, name)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = sub.remove_file(&temp);
    }
    result
}
fn change_summary(journal: &Value) -> Result<Value> {
    ensure!(
        journal["schema_version"] == 3,
        "unsupported change journal schema"
    );
    let files = journal["files"]
        .as_array()
        .context("invalid change journal files")?;
    ensure!(
        !files.is_empty() && files.len() <= 20,
        "invalid change journal file count"
    );
    let mut views = Vec::with_capacity(files.len());
    for file in files {
        let path = file["path"].as_str().context("invalid change path")?;
        relative(path)?;
        let before = file["before_version"]
            .as_str()
            .context("missing before version")?;
        let after = file["after_version"]
            .as_str()
            .context("missing after version")?;
        ensure!(
            (before == "absent" || crate::transfers::valid_hash(before))
                && (after == "absent" || crate::transfers::valid_hash(after)),
            "invalid change version"
        );
        if let Some(content) = file.get("after").and_then(Value::as_str) {
            ensure!(
                hash(content) == after,
                "change journal after content mismatch"
            );
        } else {
            ensure!(after == "absent", "missing change journal after content");
        }
        views.push(
            json!({"path":path,"before_version":before,"after_version":after,
            "status":file["status"]}),
        );
    }
    Ok(
        json!({"change_set_id":journal["change_set_id"],"state":journal["status"],"files":views,
        "recovery_protocol":1,"atomicity":"per_file"}),
    )
}

pub fn resume(ws: &Workspace, journal: &Path) -> Result<Value> {
    let bytes = std::fs::read(journal).context("change_set_unavailable: journal missing")?;
    ensure!(
        bytes.len() <= 64 * 1024 * 1024,
        "change_set journal exceeds recovery budget"
    );
    let mut state: Value =
        serde_json::from_slice(&bytes).context("change_set journal malformed")?;
    ensure!(
        state["schema_version"] == 3,
        "unsupported change journal schema"
    );
    let saved: Workspace = serde_json::from_value(state["workspace"].clone())?;
    ensure!(
        saved.id == ws.id && saved.device_id == ws.device_id && saved.root == ws.root,
        "change_set workspace identity conflict"
    );
    let total = state["files"]
        .as_array()
        .context("invalid change journal files")?
        .len();
    ensure!(
        (1..=20).contains(&total),
        "invalid change journal file count"
    );
    let dir = directory(ws)?;
    let mut conflicts = Vec::new();
    let mut newly_applied = 0usize;
    let mut already_applied = 0usize;
    for index in 0..total {
        let entry = state["files"][index].clone();
        let path = entry["path"]
            .as_str()
            .context("invalid change path")?
            .to_owned();
        relative(&path)?;
        let before = entry["before_version"]
            .as_str()
            .context("missing before version")?
            .to_owned();
        let after = entry["after_version"]
            .as_str()
            .context("missing after version")?
            .to_owned();
        let next = entry
            .get("after")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(content) = &next {
            ensure!(
                hash(content) == after,
                "change journal after content mismatch"
            );
        } else {
            ensure!(after == "absent", "missing change journal after content");
        }
        let current = if dir.try_exists(&path)? {
            Some(read_text(&dir, &path)?)
        } else {
            None
        };
        let version = current
            .as_ref()
            .map(hash)
            .unwrap_or_else(|| "absent".into());
        if version == after {
            state["files"][index]["status"] = json!("applied");
            already_applied += 1;
        } else if version != before {
            state["files"][index]["status"] = json!("conflict");
            conflicts.push(json!({"path":path,"current_version":version,"expected_before":before,"expected_after":after}));
        } else {
            let outcome = if let Some(content) = next {
                atomic_write(&dir, &path, &content)
            } else {
                dir.remove_file(&path).map_err(Into::into)
            };
            match outcome {
                Ok(()) => {
                    let verify = if dir.try_exists(&path)? {
                        hash(read_text(&dir, &path)?)
                    } else {
                        "absent".into()
                    };
                    ensure!(verify == after, "change_resume verification failed");
                    state["files"][index]["status"] = json!("applied");
                    newly_applied += 1;
                }
                Err(_) => {
                    state["files"][index]["status"] = json!("io_failed");
                    conflicts.push(json!({"path":path,"current_version":version,"expected_before":before,"expected_after":after,"reason":"io_failed"}));
                }
            }
        }
        state["status"] = json!(if conflicts.is_empty() {
            "applying"
        } else {
            "partial"
        });
        crate::write_private(journal, &serde_json::to_vec(&state)?)?;
    }
    state["status"] = json!(if conflicts.is_empty() {
        "completed"
    } else {
        "partial"
    });
    crate::write_private(journal, &serde_json::to_vec(&state)?)?;
    let summary = change_summary(&state)?;
    if conflicts.is_empty() {
        Ok(
            json!({"state":"completed","change_set":summary,"newly_applied":newly_applied,
            "already_applied":already_applied,"automatic_replay_safe":false}),
        )
    } else {
        Ok(
            json!({"error":"partial_change_set","error_code":"version_or_io_conflict","state":"partial",
            "outcome":"partial","change_set":summary,"conflicts":conflicts,"newly_applied":newly_applied,
            "already_applied":already_applied,"recovery_action":"inspect_and_merge_conflicts_then_call_change_resume_with_a_new_idempotency_key_only_when_recorded_versions_match",
            "automatic_replay_safe":false}),
        )
    }
}

pub fn apply(ws: &Workspace, v: &Value, journal: &Path) -> Result<Value> {
    let edits: Vec<FileEdit> =
        serde_json::from_value(v.get("files").cloned().context("missing files")?)?;
    ensure!(
        !edits.is_empty() && edits.len() <= 20,
        "expected 1..20 files"
    );
    let dir = directory(ws)?;
    let mut paths = BTreeSet::new();
    let mut prepared = vec![];
    for edit in edits {
        relative(&edit.path)?;
        ensure!(paths.insert(edit.path.clone()), "duplicate file");
        // Refuse symlink replacement; capability opens also prevent escaping the workspace.
        if let Ok(meta) = dir.symlink_metadata(&edit.path) {
            ensure!(!meta.is_symlink(), "cannot edit a symlink");
        }
        let old = match read_text(&dir, &edit.path) {
            Ok(s) => Some(s),
            Err(e) => {
                if dir.try_exists(&edit.path)? {
                    return Err(e);
                }
                None
            }
        };
        let version = old.as_ref().map(hash).unwrap_or_else(|| "absent".into());
        ensure!(
            edit.expected_version == version,
            "version_conflict: {} current_version={version}",
            edit.path
        );
        let new = match edit.action.as_deref().unwrap_or("edit") {
            "create" => {
                ensure!(
                    old.is_none() && edit.edits.is_none() && edit.patch.is_none(),
                    "create requires absent file and content only"
                );
                Some(edit.content.context("missing content")?)
            }
            "delete" => {
                ensure!(
                    old.is_some()
                        && edit.edits.is_none()
                        && edit.patch.is_none()
                        && edit.content.is_none(),
                    "delete requires existing file and no content/edits"
                );
                None
            }
            "edit" => Some(transformed(
                old.as_deref().context("file does not exist")?,
                &edit,
            )?),
            _ => bail!("unsupported edit action"),
        };
        ensure!(
            new.as_ref().is_none_or(|s| s.len() as u64 <= MAX_FILE),
            "edited file too large"
        );
        ensure!(
            new.as_ref().is_none_or(|s| !s.as_bytes().contains(&0)),
            "binary content is not supported by text-edit tools"
        );
        prepared.push(Prepared {
            path: edit.path,
            old,
            new,
        });
    }
    let change_set_id = journal
        .file_stem()
        .context("change journal needs id")?
        .to_string_lossy()
        .to_string();
    uuid::Uuid::parse_str(&change_set_id).context("invalid change_set id")?;
    let mut journal_data = json!({"schema_version":3,"change_set_id":change_set_id,"workspace":ws,
        "fingerprint":hash(serde_json::to_vec(v)?),"files":prepared.iter().map(|p|json!({"path":p.path,"after":p.new,
        "before_version":p.old.as_ref().map(hash).unwrap_or_else(||"absent".into()),
        "after_version":p.new.as_ref().map(hash).unwrap_or_else(||"absent".into()),"status":"pending"})).collect::<Vec<_>>(),"status":"prepared"});
    let journal_bytes = serde_json::to_vec(&journal_data)?;
    ensure!(
        journal_bytes.len() <= 64 * 1024 * 1024,
        "change_set journal exceeds 64 MiB budget"
    );
    crate::write_private(journal, &journal_bytes)?;
    let mut changed = vec![];
    let mut remaining = 16000;
    for (index, p) in prepared.iter().enumerate() {
        journal_data["files"][index]["status"] = json!("publishing");
        crate::write_private(journal, &serde_json::to_vec(&journal_data)?)?;
        let result = (|| -> Result<()> {
            let current = if dir.try_exists(&p.path)? {
                Some(read_text(&dir, &p.path)?)
            } else {
                None
            };
            ensure!(
                current == p.old,
                "version_conflict_during_apply: {}",
                p.path
            );
            if let Some(new) = &p.new {
                atomic_write(&dir, &p.path, new)?
            } else {
                dir.remove_file(&p.path)?;
            }
            Ok(())
        })();
        if let Err(e) = result {
            journal_data["status"] = json!("partial");
            journal_data["files"][index]["status"] = json!("conflicted_or_io_failed");
            crate::write_private(journal, &serde_json::to_vec(&journal_data)?)?;
            let summary = change_summary(&journal_data)?;
            return Ok(
                json!({"error":"partial_edit","error_code":"version_or_io_conflict","message":e.to_string(),"changed":changed,
                    "outcome":"partial","journal_id":journal.file_stem().map(|s|s.to_string_lossy()),"change_set":summary,
                    "failed_index":index,"pending":prepared[index..].iter().map(|p|&p.path).collect::<Vec<_>>(),
                    "recovery_action":"use change_resume with the original change_set_id and a new recovery-attempt key after inspecting conflicts",
                    "automatic_replay_safe":false}),
            );
        }
        journal_data["files"][index]["status"] = json!("applied");
        journal_data["status"] = json!(if index + 1 == prepared.len() {
            "completed"
        } else {
            "applying"
        });
        crate::write_private(journal, &serde_json::to_vec(&journal_data)?)?;
        let diff = similar::TextDiff::from_lines(
            p.old.as_deref().unwrap_or(""),
            p.new.as_deref().unwrap_or(""),
        );
        let rendered = diff
            .unified_diff()
            .context_radius(3)
            .header(&p.path, &p.path)
            .to_string();
        let (preview, truncated) = bounded(&rendered, remaining);
        remaining = remaining.saturating_sub(preview.len());
        changed.push(json!({"path":p.path,"version":p.new.as_ref().map(hash).unwrap_or_else(||"absent".into()),"diff":preview,"truncated":truncated}));
    }
    let summary = change_summary(&journal_data)?;
    Ok(
        json!({"changed":changed,"atomicity":"per_file","journal_id":journal.file_stem().map(|s|s.to_string_lossy()),
            "change_set":summary,"state":"completed"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Workspace) {
        let d = tempfile::tempdir().unwrap();
        let w = Workspace {
            id: "w".into(),
            device_id: "d".into(),
            root: d.path().into(),
        };
        (d, w)
    }
    #[test]
    fn precise_edits_and_conflicts() {
        let (d, w) = fixture();
        std::fs::write(d.path().join("a"), "alpha\r\nbeta\r\n").unwrap();
        let v = json!({"files":[{"path":"a","expected_version":hash("alpha\r\nbeta\r\n"),"edits":[{"old_text":"beta","new_text":"gamma"}]}]});
        let journal = d.path().join(format!("{}.json", uuid::Uuid::new_v4()));
        apply(&w, &v, &journal).unwrap();
        assert_eq!(
            std::fs::read_to_string(d.path().join("a")).unwrap(),
            "alpha\r\ngamma\r\n"
        );
        assert!(apply(&w, &v, &d.path().join("journal2")).is_err());
    }
    #[test]
    fn preflight_is_all_files() {
        let (d, w) = fixture();
        std::fs::write(d.path().join("a"), "one").unwrap();
        let v = json!({"files":[{"path":"a","expected_version":hash("one"),"edits":[{"old_text":"one","new_text":"two"}]},{"path":"missing","expected_version":"bad","edits":[]}]});
        assert!(apply(&w, &v, &d.path().join("j")).is_err());
        assert_eq!(std::fs::read_to_string(d.path().join("a")).unwrap(), "one");
    }
    #[test]
    fn budget_and_traversal() {
        let (d, w) = fixture();
        std::fs::write(d.path().join("a"), "a\nb\nc\n").unwrap();
        let v = read(
            &w,
            &json!({"requests":[{"path":"a","start_line":2,"end_line":2}]}),
        )
        .unwrap();
        assert_eq!(v["ranges"][0]["text"], "b\n");
        assert!(relative("../secret").is_err());
        assert!(relative("/tmp/a").is_err());
    }
    #[test]
    fn resume_applies_only_safe_versions_and_preserves_user_edits() {
        let (d, w) = fixture();
        std::fs::write(d.path().join("a"), "after-a").unwrap();
        std::fs::write(d.path().join("b"), "before-b").unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let journal = d.path().join(format!("{id}.json"));
        let state = json!({"schema_version":3,"change_set_id":id,"workspace":w,
        "fingerprint":hash("fixture"),"status":"partial","files":[
            {"path":"a","after":"after-a","before_version":hash("before-a"),"after_version":hash("after-a"),"status":"publishing"},
            {"path":"b","after":"after-b","before_version":hash("before-b"),"after_version":hash("after-b"),"status":"pending"}
        ]});
        crate::write_private(&journal, &serde_json::to_vec(&state).unwrap()).unwrap();
        let done = resume(&w, &journal).unwrap();
        assert_eq!(done["state"], "completed");
        assert_eq!(done["already_applied"], 1);
        assert_eq!(done["newly_applied"], 1);
        assert_eq!(
            std::fs::read_to_string(d.path().join("b")).unwrap(),
            "after-b"
        );
        std::fs::write(d.path().join("a"), "user-edit").unwrap();
        let partial = resume(&w, &journal).unwrap();
        assert_eq!(partial["state"], "partial");
        assert_eq!(partial["conflicts"][0]["path"], "a");
        assert_eq!(
            std::fs::read_to_string(d.path().join("a")).unwrap(),
            "user-edit"
        );
    }
    #[cfg(unix)]
    #[test]
    fn symlink_escape() {
        let (d, w) = fixture();
        std::os::unix::fs::symlink("/etc/passwd", d.path().join("secret")).unwrap();
        assert!(
            read(
                &w,
                &json!({"requests":[{"path":"secret","start_line":1,"end_line":3}]})
            )
            .is_err()
        );
    }
}
