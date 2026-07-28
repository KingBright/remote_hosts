//! HTTP API surface for operators and future UI.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use remote_hosts_core::{
    AccessCandidate, AccessResolutionError, AccessResolver, CommandProfileCatalog,
    ConnectorStateTracker, HostStateAggregator, HostStateInput, ProtectionDecision,
    PtySessionHeartbeatCommand, PtySessionInputCommand, PtySessionOpenCommand,
    PtySessionSupervisor, PtySessionSupervisorError, WorkspaceCreateCommand,
    WorkspaceOperationError, WorkspaceOperationSupervisor, WorkspaceRunCommand,
    WorkspaceSupervisor, WorkspaceSupervisorError,
};
use remote_hosts_db::{DbError, Repositories};
use remote_hosts_domain::{
    AccessPath, AccessPathHealth, AgentWorkspace, ConnectionSession, Connector, ConnectorId,
    CredentialBinding, CredentialBindingId, CredentialBindingView, CredentialKind,
    CredentialMetadata, EntityState, Host, HostFact, HostId, OperationId, OperationOutputArtifact,
    OperationOutputArtifactId, OperationOutputChunk, OperationRun, PtyInputEvent, PtyOutputChunk,
    PtySession, PtySessionId, SequencedStateEvent, SessionId, StateEvent, StateSnapshot,
    StoredCredential, TopologyEdge, TopologyEdgeId, TopologyNode, TopologyNodeId, TopologyNodeKind,
    TopologyNodeStatus, TopologyRelation, TopologySyncRun, TopologySyncRunId, WorkspaceId,
    WorkspaceState, now_utc,
};
use remote_hosts_vault::{CredentialSecret, CredentialVault};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tower_http::{request_id::MakeRequestUuid, trace::TraceLayer};
use uuid::Uuid;

/// Shared API state.
#[derive(Clone)]
pub struct ApiState {
    repositories: Arc<Repositories>,
    vault_master_password: Option<Arc<SecretString>>,
}

impl ApiState {
    /// Creates API state from repositories.
    pub fn new(repositories: Repositories) -> Self {
        Self {
            repositories: Arc::new(repositories),
            vault_master_password: None,
        }
    }

    /// Creates API state with an unlocked local credential vault for management writes.
    pub fn with_vault_master_password(
        repositories: Repositories,
        vault_master_password: SecretString,
    ) -> Self {
        Self {
            repositories: Arc::new(repositories),
            vault_master_password: Some(Arc::new(vault_master_password)),
        }
    }
}

/// Builds a health-only router.
pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/health", get(healthz))
        .layer(TraceLayer::new_for_http())
}

/// Builds the full HTTP router.
pub fn router_with_state(state: ApiState) -> Router {
    Router::new()
        .route("/", get(admin_dashboard))
        .route("/admin", get(admin_dashboard))
        .route("/healthz", get(healthz_state))
        .route("/v1/health", get(healthz_state))
        .route("/v1/hosts", get(list_hosts))
        .route("/v1/topology", get(get_topology))
        .route("/v1/topology/sync", post(sync_topology))
        .route(
            "/v1/topology/credential-bindings",
            get(list_credential_bindings),
        )
        .route(
            "/v1/topology/nodes/{node_id}/credentials",
            post(store_topology_credential),
        )
        .route("/v1/credentials", get(list_credentials))
        .route("/v1/admin/overview", get(admin_overview))
        .route("/v1/hosts/{host_id}", get(get_host))
        .route("/v1/hosts/{host_id}/access-paths", get(list_access_paths))
        .route("/v1/hosts/{host_id}/resolve-access", get(resolve_access))
        .route("/v1/hosts/{host_id}/state", get(get_host_state))
        .route("/v1/command-profiles", get(list_command_profiles))
        .route(
            "/v1/connectors/{connector_id}/heartbeat",
            post(record_connector_heartbeat),
        )
        .route(
            "/v1/connectors/{connector_id}/events",
            get(list_connector_events),
        )
        .route("/v1/runtime-events/wait", post(wait_runtime_events))
        .route(
            "/v1/hosts/{host_id}/workspaces",
            get(list_workspaces).post(create_workspace),
        )
        .route("/v1/workspaces/{workspace_id}", get(get_workspace))
        .route(
            "/v1/workspaces/{workspace_id}/operations",
            get(list_workspace_operations).post(run_workspace_operation),
        )
        .route(
            "/v1/workspaces/{workspace_id}/output",
            get(read_workspace_output),
        )
        .route(
            "/v1/workspaces/{workspace_id}/output-artifacts",
            get(list_workspace_output_artifacts),
        )
        .route(
            "/v1/output-artifacts/{artifact_id}",
            get(get_output_artifact),
        )
        .route(
            "/v1/workspaces/{workspace_id}/wait",
            post(wait_workspace_state),
        )
        .route("/v1/workspaces/{workspace_id}/close", post(close_workspace))
        .route(
            "/v1/workspaces/{workspace_id}/state",
            post(update_workspace_state),
        )
        .route(
            "/v1/workspaces/{workspace_id}/pty-sessions",
            get(list_workspace_pty_sessions).post(open_workspace_pty_session),
        )
        .route(
            "/v1/pty-sessions/{pty_session_id}/heartbeat",
            post(heartbeat_pty_session),
        )
        .route(
            "/v1/pty-sessions/{pty_session_id}/output",
            get(read_pty_output),
        )
        .route(
            "/v1/pty-sessions/{pty_session_id}/input",
            post(queue_pty_input),
        )
        .route(
            "/v1/pty-sessions/{pty_session_id}/input-events",
            get(list_pty_input_events),
        )
        .route(
            "/v1/pty-sessions/{pty_session_id}/close",
            post(close_pty_session),
        )
        .route(
            "/v1/pty-sessions/reap-expired",
            post(reap_expired_pty_sessions),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Serves the HTTP API.
///
/// # Errors
///
/// Returns an error if binding the listener or serving HTTP fails.
pub async fn serve(addr: SocketAddr, state: ApiState) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router_with_state(state)).await
}

async fn healthz() -> Json<HealthResponse> {
    Json(health_response())
}

async fn healthz_state(State(_state): State<ApiState>) -> Json<HealthResponse> {
    Json(health_response())
}

fn health_response() -> HealthResponse {
    HealthResponse {
        status: "ok",
        service: "remote-hosts-api",
        checked_at: OffsetDateTime::now_utc().to_string(),
    }
}

fn no_store_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    headers
}

async fn admin_dashboard() -> impl IntoResponse {
    let mut headers = no_store_headers();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; \
             style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; \
             object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    (headers, Html(include_str!("admin.html")))
}

async fn get_topology(
    State(state): State<ApiState>,
    Query(query): Query<TopologyQuery>,
) -> Result<Json<TopologyGraphResponse>, ApiError> {
    let (nodes, edges) = state
        .repositories
        .topology
        .list_graph(query.include_inactive.unwrap_or(false))
        .await?;
    Ok(Json(TopologyGraphResponse { nodes, edges }))
}

#[allow(clippy::too_many_lines)]
async fn sync_topology(
    State(state): State<ApiState>,
    Json(request): Json<TopologySyncRequest>,
) -> Result<Json<TopologySyncRun>, ApiError> {
    let scope_key = normalized_topology_text(&request.scope_key, "scope_key", 256)?;
    let source = normalized_topology_text(&request.source, "source", 128)?;
    if request.nodes.len() > 2_000 {
        return Err(ApiError::BadRequest(
            "nodes must contain at most 2000 items".to_owned(),
        ));
    }
    if request.edges.len() > 5_000 {
        return Err(ApiError::BadRequest(
            "edges must contain at most 5000 items".to_owned(),
        ));
    }

    let observed_at = now_utc();
    let mut nodes = Vec::with_capacity(request.nodes.len());
    let mut node_ids = HashMap::with_capacity(request.nodes.len());
    let mut node_keys = HashSet::with_capacity(request.nodes.len());
    for input in request.nodes {
        let external_key = normalized_topology_text(&input.external_key, "node.external_key", 256)?;
        if !node_keys.insert(external_key.clone()) {
            return Err(ApiError::BadRequest(format!(
                "duplicate node external_key: {external_key}"
            )));
        }
        let name = normalized_topology_text(&input.name, "node.name", 256)?;
        let address =
            normalized_optional_topology_text(input.address.as_deref(), "node.address", 2_048)?;
        ensure_safe_topology_metadata(&input.metadata, "node.metadata")?;
        if let Some(host_id) = input.host_id
            && state.repositories.hosts.get(host_id).await?.is_none()
        {
            return Err(ApiError::BadRequest(format!(
                "node.host_id does not exist: {host_id}"
            )));
        }
        let existing = state
            .repositories
            .topology
            .get_node_by_external_key(&external_key)
            .await?;
        let id = existing
            .as_ref()
            .map_or_else(TopologyNodeId::new, |node| node.id);
        node_ids.insert(external_key.clone(), id);
        nodes.push(TopologyNode {
            id,
            external_key,
            host_id: input
                .host_id
                .or_else(|| existing.as_ref().and_then(|node| node.host_id)),
            name,
            kind: input.kind,
            status: input.status,
            address,
            ports: normalized_ports(input.ports)?,
            metadata: input.metadata,
            created_at: existing
                .as_ref()
                .map_or(observed_at, |node| node.created_at),
            updated_at: observed_at,
            last_observed_at: observed_at,
            active: true,
        });
    }

    let mut edges = Vec::with_capacity(request.edges.len());
    let mut edge_keys = HashSet::with_capacity(request.edges.len());
    for input in request.edges {
        let external_key = normalized_topology_text(&input.external_key, "edge.external_key", 256)?;
        if !edge_keys.insert(external_key.clone()) {
            return Err(ApiError::BadRequest(format!(
                "duplicate edge external_key: {external_key}"
            )));
        }
        let from = normalized_topology_text(&input.from, "edge.from", 256)?;
        let to = normalized_topology_text(&input.to, "edge.to", 256)?;
        if from == to {
            return Err(ApiError::BadRequest(
                "topology edges cannot point a node to itself".to_owned(),
            ));
        }
        ensure_safe_topology_metadata(&input.metadata, "edge.metadata")?;
        let source_node_id = resolve_topology_node_id(&state, &node_ids, &from).await?;
        let target_node_id = resolve_topology_node_id(&state, &node_ids, &to).await?;
        let existing = state
            .repositories
            .topology
            .get_edge_by_external_key(&external_key)
            .await?;
        edges.push(TopologyEdge {
            id: existing
                .as_ref()
                .map_or_else(TopologyEdgeId::new, |edge| edge.id),
            external_key,
            source_node_id,
            target_node_id,
            relation: input.relation,
            metadata: input.metadata,
            created_at: existing
                .as_ref()
                .map_or(observed_at, |edge| edge.created_at),
            updated_at: observed_at,
            last_observed_at: observed_at,
            active: true,
        });
    }

    let run = state
        .repositories
        .topology
        .sync_snapshot(
            &scope_key,
            &source,
            &nodes,
            &edges,
            TopologySyncRunId::new(),
            observed_at,
        )
        .await?;
    Ok(Json(run))
}

async fn resolve_topology_node_id(
    state: &ApiState,
    current_snapshot: &HashMap<String, TopologyNodeId>,
    external_key: &str,
) -> Result<TopologyNodeId, ApiError> {
    if let Some(id) = current_snapshot.get(external_key) {
        return Ok(*id);
    }
    state
        .repositories
        .topology
        .get_node_by_external_key(external_key)
        .await?
        .filter(|node| node.active)
        .map(|node| node.id)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "edge references unknown or inactive topology node: {external_key}"
            ))
        })
}

async fn list_credentials(
    State(state): State<ApiState>,
) -> Result<(HeaderMap, Json<Vec<CredentialMetadata>>), ApiError> {
    Ok((
        no_store_headers(),
        Json(state.repositories.credentials.list_metadata().await?),
    ))
}

async fn list_credential_bindings(
    State(state): State<ApiState>,
) -> Result<(HeaderMap, Json<Vec<CredentialBindingView>>), ApiError> {
    Ok((
        no_store_headers(),
        Json(state.repositories.credential_bindings.list_views().await?),
    ))
}

async fn store_topology_credential(
    State(state): State<ApiState>,
    Path(node_id): Path<String>,
    Json(request): Json<StoreTopologyCredentialRequest>,
) -> Result<(HeaderMap, Json<CredentialBindingView>), ApiError> {
    let node_id = parse_topology_node_id(&node_id)?;
    if state
        .repositories
        .topology
        .get_node(node_id)
        .await?
        .is_none()
    {
        return Err(ApiError::NotFound);
    }
    let name = normalized_topology_text(&request.name, "name", 256)?;
    let username_hint =
        normalized_optional_topology_text(request.username_hint.as_deref(), "username_hint", 256)?;
    let purpose = normalized_topology_text(&request.purpose, "purpose", 128)?;
    validate_credential_secret(&request.secret)?;
    let master_password = state
        .vault_master_password
        .clone()
        .ok_or(ApiError::VaultUnavailable)?;
    let secret = request.secret;
    let encrypted_blob = tokio::task::spawn_blocking(move || {
        CredentialVault::encrypt(master_password.as_ref(), &secret)
    })
    .await
    .map_err(|_| ApiError::Internal("credential encryption task failed"))?
    .map_err(|_| ApiError::Internal("credential encryption failed"))?;
    let observed_at = now_utc();
    let existing = state.repositories.credentials.get_by_name(&name).await?;
    let metadata = CredentialMetadata {
        id: existing
            .as_ref()
            .map_or_else(remote_hosts_domain::CredentialId::new, |item| {
                item.metadata.id
            }),
        name,
        kind: request.kind,
        username_hint,
        created_at: existing
            .as_ref()
            .map_or(observed_at, |item| item.metadata.created_at),
        updated_at: observed_at,
        last_used_at: existing
            .as_ref()
            .and_then(|item| item.metadata.last_used_at),
    };
    state
        .repositories
        .credentials
        .upsert(&StoredCredential {
            metadata: metadata.clone(),
            encrypted_blob_json: serde_json::to_value(encrypted_blob)
                .map_err(|_| ApiError::Internal("credential serialization failed"))?,
        })
        .await?;
    let binding = CredentialBinding {
        id: CredentialBindingId::new(),
        topology_node_id: node_id,
        credential_id: metadata.id,
        purpose: purpose.clone(),
        created_at: observed_at,
    };
    state
        .repositories
        .credential_bindings
        .insert(&binding)
        .await?;
    let views = state.repositories.credential_bindings.list_views().await?;
    views
        .into_iter()
        .find(|view| {
            view.topology_node_id == node_id
                && view.credential.id == metadata.id
                && view.purpose == purpose
        })
        .map(|view| (no_store_headers(), Json(view)))
        .ok_or(ApiError::Internal(
            "credential binding was not visible after storage",
        ))
}

async fn admin_overview(
    State(state): State<ApiState>,
) -> Result<(HeaderMap, Json<AdminOverviewResponse>), ApiError> {
    let hosts = state.repositories.hosts.list().await?;
    let mut host_views = Vec::with_capacity(hosts.len());
    for host in hosts {
        let access_paths = state
            .repositories
            .access_paths
            .list_for_host(host.id)
            .await?;
        let mut path_views = Vec::with_capacity(access_paths.len());
        for access_path in access_paths {
            let health = state
                .repositories
                .access_path_health
                .get(access_path.id)
                .await?;
            path_views.push(AdminAccessPathView {
                access_path,
                health,
            });
        }
        host_views.push(AdminHostView {
            host,
            access_paths: path_views,
        });
    }
    let (nodes, edges) = state.repositories.topology.list_graph(true).await?;
    let credentials = state.repositories.credentials.list_metadata().await?;
    Ok((
        no_store_headers(),
        Json(AdminOverviewResponse {
            hosts: host_views,
            environments: state.repositories.environments.list().await?,
            connectors: state.repositories.connectors.list().await?,
            nodes,
            edges,
            credential_bindings: state.repositories.credential_bindings.list_views().await?,
            credential_count: credentials.len(),
            vault_unlocked: state.vault_master_password.is_some(),
        }),
    ))
}

async fn list_hosts(State(state): State<ApiState>) -> Result<Json<Vec<Host>>, ApiError> {
    Ok(Json(state.repositories.hosts.list().await?))
}

async fn get_host(
    State(state): State<ApiState>,
    Path(host_id): Path<String>,
) -> Result<Json<Host>, ApiError> {
    let host_id = parse_host_id(&host_id)?;
    let host = state
        .repositories
        .hosts
        .get(host_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(host))
}

async fn list_access_paths(
    State(state): State<ApiState>,
    Path(host_id): Path<String>,
) -> Result<Json<Vec<AccessPath>>, ApiError> {
    let host_id = parse_host_id(&host_id)?;
    Ok(Json(
        state
            .repositories
            .access_paths
            .list_enabled_for_host(host_id)
            .await?,
    ))
}

async fn resolve_access(
    State(state): State<ApiState>,
    Path(host_id): Path<String>,
) -> Result<Json<ResolveAccessResponse>, ApiError> {
    let host_id = parse_host_id(&host_id)?;
    let access_paths = state
        .repositories
        .access_paths
        .list_enabled_for_host(host_id)
        .await?;
    let mut candidates = Vec::with_capacity(access_paths.len());

    for access_path in access_paths {
        let connector = match access_path.connector_id {
            Some(connector_id) => state.repositories.connectors.get(connector_id).await?,
            None => None,
        };
        let health = state
            .repositories
            .access_path_health
            .get(access_path.id)
            .await?;
        candidates.push(AccessCandidate {
            access_path,
            connector,
            health,
        });
    }

    let resolution = AccessResolver::resolve(&candidates).map_err(ApiError::AccessResolution)?;
    Ok(Json(ResolveAccessResponse {
        selected_access_path: resolution.selected.access_path,
        reason: resolution.reason,
        used_cached_state: resolution.used_cached_state,
    }))
}

async fn get_host_state(
    State(state): State<ApiState>,
    Path(host_id): Path<String>,
) -> Result<Json<HostStateResponse>, ApiError> {
    let host_id = parse_host_id(&host_id)?;
    let access_paths = state
        .repositories
        .access_paths
        .list_enabled_for_host(host_id)
        .await?;
    let mut health = Vec::new();
    for access_path in &access_paths {
        if let Some(snapshot) = state
            .repositories
            .access_path_health
            .get(access_path.id)
            .await?
        {
            health.push(snapshot);
        }
    }
    let facts = state.repositories.host_facts.list_for_host(host_id).await?;
    let sessions = state
        .repositories
        .connection_sessions
        .list_for_host(host_id)
        .await?;
    let connector_snapshots = connector_snapshots_for_paths(&state, &access_paths).await?;
    let connector_state = if !connector_snapshots.is_empty()
        && connector_snapshots
            .iter()
            .all(|snapshot| snapshot.snapshot.state == EntityState::ConnectorOffline)
    {
        connector_snapshots
            .first()
            .map(|snapshot| snapshot.snapshot.clone())
    } else {
        None
    };
    let aggregate = HostStateAggregator::aggregate(&HostStateInput {
        connector_state,
        access_paths: health.clone(),
        sessions: sessions.clone(),
        facts: facts.clone(),
    });

    Ok(Json(HostStateResponse {
        host_id,
        aggregate,
        facts,
        access_path_health: health,
        sessions,
        connector_snapshots,
    }))
}

async fn list_command_profiles() -> Json<CommandProfilesResponse> {
    let profiles = CommandProfileCatalog::list_builtin();
    Json(CommandProfilesResponse {
        count: profiles.len(),
        profiles,
    })
}

async fn record_connector_heartbeat(
    State(state): State<ApiState>,
    Path(connector_id): Path<String>,
    Json(request): Json<ConnectorHeartbeatRequest>,
) -> Result<Json<ConnectorHeartbeatResponse>, ApiError> {
    let connector_id = parse_connector_id(&connector_id)?;
    let observed_at = now_utc();
    let (old_state, connector) = state
        .repositories
        .connectors
        .update_heartbeat(
            connector_id,
            request.state,
            request.version.as_deref(),
            request.current_network.as_deref(),
            observed_at,
        )
        .await?
        .ok_or(ApiError::NotFound)?;

    let outcome = ConnectorStateTracker::record_heartbeat(
        connector_id,
        old_state,
        connector.state.clone(),
        observed_at,
    );
    if let Some(event) = &outcome.event {
        state.repositories.state_events.insert(event).await?;
    }

    Ok(Json(ConnectorHeartbeatResponse {
        connector,
        snapshot: outcome.snapshot,
        event: outcome.event,
    }))
}

async fn list_connector_events(
    State(state): State<ApiState>,
    Path(connector_id): Path<String>,
) -> Result<Json<Vec<StateEvent>>, ApiError> {
    let connector_id = parse_connector_id(&connector_id)?;
    let events = state
        .repositories
        .state_events
        .list_for_entity("connector", &connector_id.to_string(), 50)
        .await?;
    Ok(Json(events))
}

async fn wait_runtime_events(
    State(state): State<ApiState>,
    Json(request): Json<WaitRuntimeEventsRequest>,
) -> Result<Json<WaitRuntimeEventsResponse>, ApiError> {
    if request.entity_id.is_some() && request.entity_type.is_none() {
        return Err(ApiError::BadRequest(
            "entity_id requires entity_type".to_owned(),
        ));
    }
    let entity_type = normalized_event_filter(request.entity_type, "entity_type")?;
    let entity_id = normalized_event_filter(request.entity_id, "entity_id")?;
    let start_cursor = match request.start_mode {
        RuntimeEventStartMode::LiveOnly => {
            if request.after_cursor.is_some() {
                return Err(ApiError::BadRequest(
                    "after_cursor is forbidden when start_mode is live_only".to_owned(),
                ));
            }
            state.repositories.state_events.latest_sequence().await?
        }
        RuntimeEventStartMode::AfterCursor => request.after_cursor.ok_or_else(|| {
            ApiError::BadRequest(
                "after_cursor is required when start_mode is after_cursor".to_owned(),
            )
        })?,
    };
    let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(5_000).min(60_000));
    let deadline = std::time::Instant::now() + timeout;
    let limit = request.limit.unwrap_or(50).clamp(1, 200);

    loop {
        let events = state
            .repositories
            .state_events
            .list_after(
                start_cursor,
                entity_type.as_deref(),
                entity_id.as_deref(),
                limit,
            )
            .await?;
        if let Some(next_cursor) = events.last().map(|event| event.sequence) {
            return Ok(Json(WaitRuntimeEventsResponse {
                start_cursor,
                next_cursor,
                timed_out: false,
                count: events.len(),
                events,
            }));
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return Ok(Json(WaitRuntimeEventsResponse {
                start_cursor,
                next_cursor: start_cursor,
                timed_out: true,
                count: 0,
                events: Vec::new(),
            }));
        }
        tokio::time::sleep(Duration::from_millis(200).min(deadline - now)).await;
    }
}

async fn list_workspaces(
    State(state): State<ApiState>,
    Path(host_id): Path<String>,
) -> Result<Json<Vec<AgentWorkspace>>, ApiError> {
    let host_id = parse_host_id(&host_id)?;
    Ok(Json(
        state.repositories.workspaces.list_for_host(host_id).await?,
    ))
}

async fn create_workspace(
    State(state): State<ApiState>,
    Path(host_id): Path<String>,
    Json(request): Json<CreateWorkspaceRequest>,
) -> Result<(StatusCode, Json<AgentWorkspace>), ApiError> {
    let host_id = parse_host_id(&host_id)?;
    let access_paths = state
        .repositories
        .access_paths
        .list_enabled_for_host(host_id)
        .await?;
    let access_path = access_paths
        .into_iter()
        .find(|path| path.id == request.access_path_id)
        .ok_or(ApiError::BadRequest(
            "access_path_id is not enabled for this host".to_owned(),
        ))?;
    let connector_id =
        request
            .connector_id
            .or(access_path.connector_id)
            .ok_or(ApiError::BadRequest(
                "workspace creation requires a connector_id".to_owned(),
            ))?;

    if state
        .repositories
        .connectors
        .get(connector_id)
        .await?
        .is_none()
    {
        return Err(ApiError::BadRequest(
            "connector_id does not exist".to_owned(),
        ));
    }

    let active_workspace_count = state
        .repositories
        .workspaces
        .list_for_host(host_id)
        .await?
        .into_iter()
        .filter(|workspace| {
            matches!(
                workspace.state,
                WorkspaceState::Idle | WorkspaceState::Working
            )
        })
        .count()
        .try_into()
        .unwrap_or(u32::MAX);

    let workspace = WorkspaceSupervisor::default().create_workspace(
        WorkspaceCreateCommand {
            host_id,
            access_path_id: access_path.id,
            agent_session_id: None,
            connector_id,
            label: request.label,
            cwd: request.cwd,
            policy_profile: request
                .policy_profile
                .unwrap_or_else(|| "default".to_owned()),
            ttl_seconds: request.ttl_seconds.unwrap_or(3600),
        },
        active_workspace_count,
    )?;
    state.repositories.workspaces.insert(&workspace).await?;
    Ok((StatusCode::CREATED, Json(workspace)))
}

async fn get_workspace(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
) -> Result<Json<AgentWorkspace>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let workspace = state
        .repositories
        .workspaces
        .get(workspace_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(workspace))
}

async fn run_workspace_operation(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Json(request): Json<RunWorkspaceOperationRequest>,
) -> Result<(StatusCode, Json<RunWorkspaceOperationResponse>), ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let response = queue_workspace_operation(
        &state,
        workspace_id,
        &request.command_profile,
        request.args,
        request.intent,
        request.timeout_seconds,
        request.output_limit_bytes,
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn list_workspace_operations(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<ListWorkspaceOperationsQuery>,
) -> Result<Json<Vec<OperationRun>>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    if state
        .repositories
        .workspaces
        .get(workspace_id)
        .await?
        .is_none()
    {
        return Err(ApiError::NotFound);
    }
    Ok(Json(
        state
            .repositories
            .operations
            .list_for_workspace(workspace_id, query.limit.unwrap_or(50).clamp(1, 200))
            .await?,
    ))
}

async fn read_workspace_output(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<ReadWorkspaceOutputQuery>,
) -> Result<Json<WorkspaceOutputResponse>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let workspace = state
        .repositories
        .workspaces
        .get(workspace_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let operation_id = query
        .operation_id
        .as_deref()
        .map(parse_operation_id)
        .transpose()?;
    if operation_id.is_none() && query.after_sequence.is_some() {
        return Err(ApiError::BadRequest(
            "after_sequence requires operation_id".to_owned(),
        ));
    }
    if let Some(operation_id) = operation_id {
        let operation = state
            .repositories
            .operations
            .get(operation_id)
            .await?
            .ok_or(ApiError::NotFound)?;
        if operation.workspace_id != Some(workspace_id) {
            return Err(ApiError::BadRequest(
                "operation does not belong to workspace".to_owned(),
            ));
        }
    }
    let chunks = state
        .repositories
        .operation_output_chunks
        .list_for_workspace(
            workspace_id,
            operation_id,
            query.after_sequence,
            query.limit.unwrap_or(50).clamp(1, 200),
        )
        .await?;
    let recent_operations = state
        .repositories
        .operations
        .list_for_workspace(workspace_id, 10)
        .await?;
    Ok(Json(WorkspaceOutputResponse {
        workspace,
        count: chunks.len(),
        chunks,
        recent_operations,
    }))
}

async fn list_workspace_output_artifacts(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<ListWorkspaceOutputArtifactsQuery>,
) -> Result<Json<WorkspaceOutputArtifactsResponse>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let workspace = state
        .repositories
        .workspaces
        .get(workspace_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let operation_id = query
        .operation_id
        .as_deref()
        .map(parse_operation_id)
        .transpose()?;
    if let Some(operation_id) = operation_id {
        let operation = state
            .repositories
            .operations
            .get(operation_id)
            .await?
            .ok_or(ApiError::NotFound)?;
        if operation.workspace_id != Some(workspace_id) {
            return Err(ApiError::BadRequest(
                "operation does not belong to workspace".to_owned(),
            ));
        }
    }
    let artifacts = state
        .repositories
        .operation_output_artifacts
        .list_for_workspace(
            workspace_id,
            operation_id,
            query.limit.unwrap_or(50).clamp(1, 200),
        )
        .await?;
    Ok(Json(WorkspaceOutputArtifactsResponse {
        workspace,
        count: artifacts.len(),
        artifacts,
    }))
}

async fn get_output_artifact(
    State(state): State<ApiState>,
    Path(artifact_id): Path<String>,
) -> Result<Json<OperationOutputArtifact>, ApiError> {
    let artifact_id = parse_output_artifact_id(&artifact_id)?;
    let artifact = state
        .repositories
        .operation_output_artifacts
        .get(artifact_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(artifact))
}

async fn wait_workspace_state(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Json(request): Json<WaitWorkspaceStateRequest>,
) -> Result<Json<WaitWorkspaceStateResponse>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let desired_states = request.desired_states.unwrap_or_else(default_wait_states);
    let timeout_ms = request.timeout_ms.unwrap_or(5000).clamp(0, 60_000);
    let poll_interval_ms = request.poll_interval_ms.unwrap_or(250).clamp(100, 5000);
    let started_at = std::time::Instant::now();
    let deadline = started_at + Duration::from_millis(timeout_ms);

    loop {
        let workspace = state
            .repositories
            .workspaces
            .get(workspace_id)
            .await?
            .ok_or(ApiError::NotFound)?;
        if desired_states.contains(&workspace.state) {
            return Ok(Json(WaitWorkspaceStateResponse {
                matched: true,
                workspace,
                desired_states,
                elapsed_ms: elapsed_ms(started_at),
                retry_after_ms: None,
            }));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(Json(WaitWorkspaceStateResponse {
                matched: false,
                workspace,
                desired_states,
                elapsed_ms: elapsed_ms(started_at),
                retry_after_ms: Some(poll_interval_ms),
            }));
        }
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }
}

async fn close_workspace(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
) -> Result<Json<AgentWorkspace>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let workspace = state
        .repositories
        .workspaces
        .update_state(workspace_id, WorkspaceState::Closed, now_utc())
        .await?
        .ok_or(ApiError::NotFound)?;
    state
        .repositories
        .pty_sessions
        .close_for_workspace(workspace_id, workspace.last_activity_at)
        .await?;
    Ok(Json(workspace))
}

async fn update_workspace_state(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Json(request): Json<UpdateWorkspaceStateRequest>,
) -> Result<Json<AgentWorkspace>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let workspace = state
        .repositories
        .workspaces
        .update_state(workspace_id, request.state, now_utc())
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(workspace))
}

async fn list_workspace_pty_sessions(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
) -> Result<Json<Vec<PtySession>>, ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    Ok(Json(
        state
            .repositories
            .pty_sessions
            .list_for_workspace(workspace_id)
            .await?,
    ))
}

async fn read_pty_output(
    State(state): State<ApiState>,
    Path(pty_session_id): Path<String>,
    Query(query): Query<ReadPtyOutputQuery>,
) -> Result<Json<PtyOutputResponse>, ApiError> {
    let pty_session_id = parse_pty_session_id(&pty_session_id)?;
    let pty_session = state
        .repositories
        .pty_sessions
        .get(pty_session_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let chunks = state
        .repositories
        .pty_output_chunks
        .list_for_session(
            pty_session_id,
            query.after_sequence,
            query.limit.unwrap_or(50).clamp(1, 200),
        )
        .await?;
    Ok(Json(PtyOutputResponse {
        pty_session,
        count: chunks.len(),
        chunks,
    }))
}

async fn queue_pty_input(
    State(state): State<ApiState>,
    Path(pty_session_id): Path<String>,
    Json(request): Json<QueuePtyInputRequest>,
) -> Result<(StatusCode, Json<PtyInputEvent>), ApiError> {
    let pty_session_id = parse_pty_session_id(&pty_session_id)?;
    let pty_session = state
        .repositories
        .pty_sessions
        .get(pty_session_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let workspace = state
        .repositories
        .workspaces
        .get(pty_session.workspace_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let next_sequence = state
        .repositories
        .pty_input_events
        .next_sequence(pty_session_id)
        .await?;
    let plan = PtySessionSupervisor::default().queue_input(
        &pty_session,
        &workspace,
        next_sequence,
        PtySessionInputCommand {
            input: request.input,
            requested_by: request.requested_by,
            idempotency_key: None,
        },
    )?;
    state
        .repositories
        .pty_input_events
        .insert(&plan.event, &plan.input_text)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(plan.event)))
}

async fn list_pty_input_events(
    State(state): State<ApiState>,
    Path(pty_session_id): Path<String>,
    Query(query): Query<ListPtyInputEventsQuery>,
) -> Result<Json<PtyInputEventsResponse>, ApiError> {
    let pty_session_id = parse_pty_session_id(&pty_session_id)?;
    let pty_session = state
        .repositories
        .pty_sessions
        .get(pty_session_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let events = state
        .repositories
        .pty_input_events
        .list_for_session(
            pty_session_id,
            query.after_sequence,
            query.limit.unwrap_or(50).clamp(1, 200),
        )
        .await?;
    Ok(Json(PtyInputEventsResponse {
        pty_session,
        count: events.len(),
        input_events: events,
    }))
}

async fn open_workspace_pty_session(
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
    Json(request): Json<OpenPtySessionRequest>,
) -> Result<(StatusCode, Json<PtySession>), ApiError> {
    let workspace_id = parse_workspace_id(&workspace_id)?;
    let workspace = state
        .repositories
        .workspaces
        .get(workspace_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let connection = if let Some(session_id) = request.session_id.as_deref() {
        let session_id = parse_session_id(session_id)?;
        let connection = state
            .repositories
            .connection_sessions
            .get(session_id)
            .await?
            .ok_or(ApiError::NotFound)?;
        if connection.access_path_id != workspace.access_path_id
            || connection.connector_id != workspace.connector_id
        {
            return Err(ApiError::BadRequest(
                "connection session does not belong to this workspace route".to_owned(),
            ));
        }
        connection
    } else if let Some(connection) = state
        .repositories
        .connection_sessions
        .find_reusable(workspace.access_path_id, workspace.connector_id)
        .await?
    {
        connection
    } else {
        let now = now_utc();
        let connection = ConnectionSession {
            session_id: SessionId::new(),
            access_path_id: workspace.access_path_id,
            connector_id: workspace.connector_id,
            state: EntityState::Resolving,
            created_at: now,
            last_used_at: now,
            open_channels: 0,
            reused_count: 0,
            failure_count: 0,
            last_error: None,
        };
        state
            .repositories
            .connection_sessions
            .upsert(&connection)
            .await?;
        connection
    };
    let session_id = connection.session_id;
    let active_ptys = state
        .repositories
        .pty_sessions
        .count_active_for_host(workspace.host_id)
        .await?;
    let pty = PtySessionSupervisor::default().open_session(
        &workspace,
        &connection,
        active_ptys,
        PtySessionOpenCommand {
            session_id,
            cwd: request.cwd,
        },
    )?;
    state.repositories.pty_sessions.upsert(&pty).await?;
    Ok((StatusCode::CREATED, Json(pty)))
}

async fn heartbeat_pty_session(
    State(state): State<ApiState>,
    Path(pty_session_id): Path<String>,
    Json(request): Json<HeartbeatPtySessionRequest>,
) -> Result<Json<PtySession>, ApiError> {
    let pty_session_id = parse_pty_session_id(&pty_session_id)?;
    let pty = state
        .repositories
        .pty_sessions
        .get(pty_session_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let updated = PtySessionSupervisor::default().heartbeat(
        pty,
        PtySessionHeartbeatCommand {
            state: request.state,
            foreground_process: request.foreground_process,
            cwd: request.cwd,
            recent_output_ref: request.recent_output_ref,
            last_exit_code: request.last_exit_code,
            input_allowed: request.input_allowed,
        },
    )?;
    state.repositories.pty_sessions.upsert(&updated).await?;
    if updated.state != WorkspaceState::Closed {
        state
            .repositories
            .workspaces
            .update_state(
                updated.workspace_id,
                updated.state.clone(),
                updated.last_activity_at,
            )
            .await?;
    }
    Ok(Json(updated))
}

async fn close_pty_session(
    State(state): State<ApiState>,
    Path(pty_session_id): Path<String>,
    Json(request): Json<ClosePtySessionRequest>,
) -> Result<Json<PtySession>, ApiError> {
    let pty_session_id = parse_pty_session_id(&pty_session_id)?;
    let pty = state
        .repositories
        .pty_sessions
        .get(pty_session_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let closed = PtySessionSupervisor::default().close(pty, request.last_exit_code);
    state.repositories.pty_sessions.upsert(&closed).await?;
    if state
        .repositories
        .pty_sessions
        .count_active_for_workspace(closed.workspace_id)
        .await?
        == 0
    {
        state
            .repositories
            .workspaces
            .update_state(
                closed.workspace_id,
                WorkspaceState::Idle,
                closed.last_activity_at,
            )
            .await?;
    }
    Ok(Json(closed))
}

async fn reap_expired_pty_sessions(
    State(state): State<ApiState>,
    Json(request): Json<ReapExpiredPtySessionsRequest>,
) -> Result<Json<ReapExpiredPtySessionsResponse>, ApiError> {
    let idle_ttl_seconds = request.idle_ttl_seconds.unwrap_or(3600).clamp(60, 86_400);
    let limit = request.limit.unwrap_or(100).clamp(1, 500);
    let reaped = state
        .repositories
        .pty_sessions
        .close_expired(now_utc(), idle_ttl_seconds, limit)
        .await?;
    Ok(Json(ReapExpiredPtySessionsResponse {
        count: reaped.len(),
        pty_sessions: reaped,
    }))
}

async fn queue_workspace_operation(
    state: &ApiState,
    workspace_id: WorkspaceId,
    command_profile: &str,
    args: Vec<String>,
    intent: Option<String>,
    timeout_seconds: Option<u64>,
    output_limit_bytes: Option<usize>,
) -> Result<RunWorkspaceOperationResponse, ApiError> {
    let workspace = state
        .repositories
        .workspaces
        .get(workspace_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let policy = remote_hosts_core::ServerProtectionPolicy::default();
    let mut profile = CommandProfileCatalog::resolve_builtin(command_profile, args, &policy)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    if let Some(timeout_seconds) = timeout_seconds {
        profile.timeout_seconds = timeout_seconds;
    }
    if let Some(output_limit_bytes) = output_limit_bytes {
        profile.output_limit_bytes = output_limit_bytes;
    }
    profile
        .validate()
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let queued_operations = state
        .repositories
        .operations
        .count_queued_for_host(workspace.host_id)
        .await?;
    let active_exec_channels = state
        .repositories
        .operations
        .count_running_for_host(workspace.host_id)
        .await?;
    let plan = WorkspaceOperationSupervisor::new(policy).queue_operation(&WorkspaceRunCommand {
        workspace,
        command_profile: profile,
        intent,
        idempotency_key: None,
        queued_operations,
        active_exec_channels,
        active_probe_jobs: 0,
        overload_cooldown_active: false,
    })?;

    state
        .repositories
        .operations
        .insert(&plan.operation)
        .await?;
    state
        .repositories
        .operation_output_chunks
        .insert(&plan.initial_output_chunk)
        .await?;
    let workspace = state
        .repositories
        .workspaces
        .update_state(workspace_id, plan.workspace_state, now_utc())
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(RunWorkspaceOperationResponse {
        operation: plan.operation,
        workspace,
        initial_output_chunk: plan.initial_output_chunk,
        protection_decision: plan.decision,
    })
}

async fn connector_snapshots_for_paths(
    state: &ApiState,
    access_paths: &[AccessPath],
) -> Result<Vec<ConnectorSnapshot>, ApiError> {
    let mut snapshots = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let now = now_utc();

    for connector_id in access_paths
        .iter()
        .filter_map(|access_path| access_path.connector_id)
    {
        if !seen.insert(connector_id) {
            continue;
        }
        if let Some(connector) = state.repositories.connectors.get(connector_id).await? {
            let observed_at = connector.last_seen_at.unwrap_or(now);
            let visible_state = if connector.last_seen_at.is_some() {
                connector.state.clone()
            } else {
                EntityState::ConnectorOffline
            };
            let snapshot = ConnectorStateTracker::snapshot(visible_state, observed_at, now, 60);
            snapshots.push(ConnectorSnapshot {
                connector,
                snapshot,
            });
        }
    }

    Ok(snapshots)
}

fn parse_host_id(input: &str) -> Result<HostId, ApiError> {
    Ok(HostId::from(Uuid::parse_str(input)?))
}

fn parse_connector_id(input: &str) -> Result<ConnectorId, ApiError> {
    Ok(ConnectorId::from(Uuid::parse_str(input)?))
}

fn parse_workspace_id(input: &str) -> Result<WorkspaceId, ApiError> {
    Ok(WorkspaceId::from(Uuid::parse_str(input)?))
}

fn parse_session_id(input: &str) -> Result<SessionId, ApiError> {
    Ok(SessionId::from(Uuid::parse_str(input)?))
}

fn parse_pty_session_id(input: &str) -> Result<PtySessionId, ApiError> {
    Ok(PtySessionId::from(Uuid::parse_str(input)?))
}

fn parse_operation_id(input: &str) -> Result<OperationId, ApiError> {
    Ok(OperationId::from(Uuid::parse_str(input)?))
}

fn parse_output_artifact_id(input: &str) -> Result<OperationOutputArtifactId, ApiError> {
    Ok(OperationOutputArtifactId::from(Uuid::parse_str(input)?))
}

fn parse_topology_node_id(input: &str) -> Result<TopologyNodeId, ApiError> {
    Ok(TopologyNodeId::from(Uuid::parse_str(input)?))
}

fn normalized_topology_text(value: &str, field: &str, max_len: usize) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError::BadRequest(format!("{field} must not be empty")));
    }
    if value.len() > max_len {
        return Err(ApiError::BadRequest(format!(
            "{field} must be at most {max_len} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(value.to_owned())
}

fn normalized_optional_topology_text(
    value: Option<&str>,
    field: &str,
    max_len: usize,
) -> Result<Option<String>, ApiError> {
    value
        .map(|value| normalized_topology_text(value, field, max_len))
        .transpose()
}

fn normalized_ports(mut ports: Vec<u16>) -> Result<Vec<u16>, ApiError> {
    if ports.contains(&0) {
        return Err(ApiError::BadRequest(
            "node.ports must contain values between 1 and 65535".to_owned(),
        ));
    }
    ports.sort_unstable();
    ports.dedup();
    if ports.len() > 256 {
        return Err(ApiError::BadRequest(
            "node.ports must contain at most 256 unique ports".to_owned(),
        ));
    }
    Ok(ports)
}

fn ensure_safe_topology_metadata(value: &serde_json::Value, field: &str) -> Result<(), ApiError> {
    match value {
        serde_json::Value::Object(object) => {
            for (key, child) in object {
                let normalized_key: String = key
                    .chars()
                    .filter(char::is_ascii_alphanumeric)
                    .flat_map(char::to_lowercase)
                    .collect();
                if [
                    "password",
                    "passwd",
                    "token",
                    "secret",
                    "apikey",
                    "privatekey",
                    "credential",
                    "connectionstring",
                ]
                .iter()
                .any(|needle| normalized_key.contains(needle))
                {
                    return Err(ApiError::BadRequest(format!(
                        "{field} contains secret-like key '{key}'; use a credential binding"
                    )));
                }
                ensure_safe_topology_metadata(child, field)?;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                ensure_safe_topology_metadata(item, field)?;
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
    Ok(())
}

fn validate_credential_secret(secret: &CredentialSecret) -> Result<(), ApiError> {
    let values = [
        secret.password.as_deref(),
        secret.private_key_pem.as_deref(),
        secret.private_key_passphrase.as_deref(),
        secret.sudo_password.as_deref(),
        secret.token.as_deref(),
        secret.secret_text.as_deref(),
    ];
    if !secret.use_ssh_agent
        && !values
            .iter()
            .flatten()
            .any(|value| !value.trim().is_empty())
    {
        return Err(ApiError::BadRequest(
            "secret must contain password, key, token, secret_text, or SSH-agent access".to_owned(),
        ));
    }
    if values.iter().flatten().any(|value| value.len() > 1_048_576) {
        return Err(ApiError::BadRequest(
            "individual secret fields must not exceed 1 MiB".to_owned(),
        ));
    }
    Ok(())
}

fn normalized_event_filter(value: Option<String>, field: &str) -> Result<Option<String>, ApiError> {
    value
        .map(|value| {
            let value = value.trim();
            if value.is_empty() {
                return Err(ApiError::BadRequest(format!("{field} must not be empty")));
            }
            if value.len() > 128 {
                return Err(ApiError::BadRequest(format!(
                    "{field} must be at most 128 characters"
                )));
            }
            Ok(value.to_owned())
        })
        .transpose()
}

fn default_wait_states() -> Vec<WorkspaceState> {
    vec![
        WorkspaceState::Idle,
        WorkspaceState::Done,
        WorkspaceState::Failed,
        WorkspaceState::Throttled,
        WorkspaceState::Blocked,
        WorkspaceState::Closed,
    ]
}

fn elapsed_ms(started_at: std::time::Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn default_topology_status() -> TopologyNodeStatus {
    TopologyNodeStatus::Unknown
}

fn empty_json_object() -> serde_json::Value {
    serde_json::json!({})
}

/// Query for the infrastructure topology graph.
#[derive(Clone, Debug, Deserialize)]
pub struct TopologyQuery {
    /// Include nodes and edges no longer present in their latest source snapshot.
    pub include_inactive: Option<bool>,
}

/// One node supplied by a topology producer.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySyncNodeInput {
    /// Stable source-controlled key.
    pub external_key: String,
    /// Optional primary host-registry link.
    pub host_id: Option<HostId>,
    /// Human-readable name.
    pub name: String,
    /// Resource category.
    pub kind: TopologyNodeKind,
    /// Last observed status.
    #[serde(default = "default_topology_status")]
    pub status: TopologyNodeStatus,
    /// Address, DNS name, URL, virtual IP, or subnet.
    pub address: Option<String>,
    /// Exposed or listened ports.
    #[serde(default)]
    pub ports: Vec<u16>,
    /// Non-secret extensible metadata.
    #[serde(default = "empty_json_object")]
    pub metadata: serde_json::Value,
}

/// One directed edge supplied by a topology producer.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySyncEdgeInput {
    /// Stable source-controlled key.
    pub external_key: String,
    /// Source node external key.
    pub from: String,
    /// Target node external key.
    pub to: String,
    /// Relationship category.
    pub relation: TopologyRelation,
    /// Non-secret extensible metadata.
    #[serde(default = "empty_json_object")]
    pub metadata: serde_json::Value,
}

/// Authoritative snapshot for one producer and topology scope.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySyncRequest {
    /// Reconciliation scope, such as `host:<uuid>` or `cluster:factory-a`.
    pub scope_key: String,
    /// Snapshot producer.
    pub source: String,
    /// Current nodes owned by this source in this scope.
    #[serde(default)]
    pub nodes: Vec<TopologySyncNodeInput>,
    /// Current edges owned by this source in this scope.
    #[serde(default)]
    pub edges: Vec<TopologySyncEdgeInput>,
}

/// Infrastructure topology graph response.
#[derive(Clone, Debug, Serialize)]
pub struct TopologyGraphResponse {
    /// Topology nodes.
    pub nodes: Vec<TopologyNode>,
    /// Directed topology edges.
    pub edges: Vec<TopologyEdge>,
}

/// Store or rotate an encrypted credential and bind it to a topology node.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreTopologyCredentialRequest {
    /// Stable human-readable credential name. Reusing the name rotates the encrypted value.
    pub name: String,
    /// Credential category.
    pub kind: CredentialKind,
    /// Optional username hint visible in the UI.
    pub username_hint: Option<String>,
    /// Purpose of this binding.
    pub purpose: String,
    /// Secret material encrypted before database storage.
    pub secret: CredentialSecret,
}

/// Access path and current health shown in the management overview.
#[derive(Clone, Debug, Serialize)]
pub struct AdminAccessPathView {
    /// Configured SSH access path.
    pub access_path: AccessPath,
    /// Latest health snapshot.
    pub health: Option<AccessPathHealth>,
}

/// Host registry record with all configured access paths.
#[derive(Clone, Debug, Serialize)]
pub struct AdminHostView {
    /// Host registry record.
    pub host: Host,
    /// Access routes, including disabled routes.
    pub access_paths: Vec<AdminAccessPathView>,
}

/// Management dashboard bootstrap response.
#[derive(Clone, Debug, Serialize)]
pub struct AdminOverviewResponse {
    /// All registered hosts and routes.
    pub hosts: Vec<AdminHostView>,
    /// Network environments.
    pub environments: Vec<remote_hosts_domain::Environment>,
    /// Connector workers.
    pub connectors: Vec<Connector>,
    /// Active and inactive topology nodes.
    pub nodes: Vec<TopologyNode>,
    /// Active and inactive topology edges.
    pub edges: Vec<TopologyEdge>,
    /// Public bindings to encrypted credentials.
    pub credential_bindings: Vec<CredentialBindingView>,
    /// Total encrypted credentials, including SSH route credentials.
    pub credential_count: usize,
    /// Whether this HTTP process can accept credential writes.
    pub vault_unlocked: bool,
}

/// Health response.
#[derive(Clone, Debug, Serialize)]
pub struct HealthResponse {
    /// Health status.
    pub status: &'static str,
    /// Service name.
    pub service: &'static str,
    /// Check timestamp.
    pub checked_at: String,
}

/// Host state response.
#[derive(Clone, Debug, Serialize)]
pub struct HostStateResponse {
    /// Host id.
    pub host_id: HostId,
    /// Aggregated state.
    pub aggregate: remote_hosts_core::HostStateAggregate,
    /// Known facts included for agent context.
    pub facts: Vec<HostFact>,
    /// Access path health snapshots.
    pub access_path_health: Vec<AccessPathHealth>,
    /// Active and recent connection sessions.
    pub sessions: Vec<ConnectionSession>,
    /// Connector snapshots associated with this host.
    pub connector_snapshots: Vec<ConnectorSnapshot>,
}

/// Resolve access response.
#[derive(Clone, Debug, Serialize)]
pub struct ResolveAccessResponse {
    /// Selected access path.
    pub selected_access_path: AccessPath,
    /// Selection reason.
    pub reason: String,
    /// Whether cached state contributed to resolution.
    pub used_cached_state: bool,
}

/// Connector snapshot response.
#[derive(Clone, Debug, Serialize)]
pub struct ConnectorSnapshot {
    /// Connector registry record.
    pub connector: Connector,
    /// Agent-visible connector state.
    pub snapshot: StateSnapshot,
}

/// Command profile list response.
#[derive(Clone, Debug, Serialize)]
pub struct CommandProfilesResponse {
    /// Number of returned profiles.
    pub count: usize,
    /// Built-in command profiles.
    pub profiles: Vec<remote_hosts_core::CommandProfileInfo>,
}

/// Connector heartbeat request.
#[derive(Clone, Debug, Deserialize)]
pub struct ConnectorHeartbeatRequest {
    /// Observed connector state.
    pub state: EntityState,
    /// Optional connector version update.
    pub version: Option<String>,
    /// Optional current network label update.
    pub current_network: Option<String>,
}

/// Connector heartbeat response.
#[derive(Clone, Debug, Serialize)]
pub struct ConnectorHeartbeatResponse {
    /// Updated connector.
    pub connector: Connector,
    /// Agent-visible state snapshot.
    pub snapshot: StateSnapshot,
    /// Transition event, when state changed.
    pub event: Option<StateEvent>,
}

/// Starting behavior for a runtime event wait.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventStartMode {
    /// Ignore retained history and wait only for events created after this request starts.
    LiveOnly,
    /// Replay events strictly after the supplied cursor.
    AfterCursor,
}

/// Runtime event wait request.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRuntimeEventsRequest {
    /// Explicitly choose live-only delivery or cursor-based replay.
    pub start_mode: RuntimeEventStartMode,
    /// Required for `after_cursor` and forbidden for `live_only`.
    pub after_cursor: Option<u64>,
    /// Optional entity type filter.
    pub entity_type: Option<String>,
    /// Optional entity id filter; requires `entity_type`.
    pub entity_id: Option<String>,
    /// Long-poll timeout in milliseconds. Defaults to 5000 and is capped at 60000.
    pub timeout_ms: Option<u64>,
    /// Maximum events returned. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Runtime event wait response.
#[derive(Clone, Debug, Serialize)]
pub struct WaitRuntimeEventsResponse {
    /// Cursor from which this wait began.
    pub start_cursor: u64,
    /// Cursor to use for the next replay wait.
    pub next_cursor: u64,
    /// Whether the wait ended without a matching event.
    pub timed_out: bool,
    /// Number of events returned.
    pub count: usize,
    /// Sequenced transition events.
    pub events: Vec<SequencedStateEvent>,
}

/// Create workspace request.
#[derive(Clone, Debug, Deserialize)]
pub struct CreateWorkspaceRequest {
    /// Access path to bind the workspace to.
    pub access_path_id: remote_hosts_domain::AccessPathId,
    /// Connector override. Defaults to the access path connector.
    pub connector_id: Option<ConnectorId>,
    /// Workspace label.
    pub label: String,
    /// Optional current working directory.
    pub cwd: Option<String>,
    /// Optional policy profile.
    pub policy_profile: Option<String>,
    /// Optional TTL in seconds.
    pub ttl_seconds: Option<u64>,
}

/// Update workspace state request.
#[derive(Clone, Debug, Deserialize)]
pub struct UpdateWorkspaceStateRequest {
    /// New workspace state.
    pub state: WorkspaceState,
}

/// Open PTY session request.
#[derive(Clone, Debug, Deserialize)]
pub struct OpenPtySessionRequest {
    /// Optional existing connection session id. The service resolves or creates one when omitted.
    pub session_id: Option<String>,
    /// Optional initial current working directory.
    pub cwd: Option<String>,
}

/// PTY heartbeat request.
#[derive(Clone, Debug, Deserialize)]
pub struct HeartbeatPtySessionRequest {
    /// PTY state.
    pub state: WorkspaceState,
    /// Foreground process summary.
    pub foreground_process: Option<String>,
    /// Current working directory.
    pub cwd: Option<String>,
    /// Recent output artifact reference.
    pub recent_output_ref: Option<String>,
    /// Last foreground process exit code.
    pub last_exit_code: Option<i32>,
    /// Whether input remains allowed.
    pub input_allowed: bool,
}

/// Close PTY session request.
#[derive(Clone, Debug, Deserialize)]
pub struct ClosePtySessionRequest {
    /// Last foreground process exit code.
    pub last_exit_code: Option<i32>,
}

/// Reap expired PTY sessions request.
#[derive(Clone, Debug, Deserialize)]
pub struct ReapExpiredPtySessionsRequest {
    /// Idle TTL in seconds. Defaults to 3600 and is clamped to 60..=86400.
    pub idle_ttl_seconds: Option<u64>,
    /// Maximum number of PTYs to close. Defaults to 100 and is capped at 500.
    pub limit: Option<u32>,
}

/// Reap expired PTY sessions response.
#[derive(Clone, Debug, Serialize)]
pub struct ReapExpiredPtySessionsResponse {
    /// Number of reaped PTY sessions.
    pub count: usize,
    /// Reaped PTY session records.
    pub pty_sessions: Vec<PtySession>,
}

/// Read PTY output query.
#[derive(Clone, Debug, Deserialize)]
pub struct ReadPtyOutputQuery {
    /// Only return chunks after this sequence number.
    pub after_sequence: Option<u64>,
    /// Maximum number of chunks. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// PTY output response.
#[derive(Clone, Debug, Serialize)]
pub struct PtyOutputResponse {
    /// PTY session record.
    pub pty_session: PtySession,
    /// Number of returned chunks.
    pub count: usize,
    /// Redacted PTY output chunks.
    pub chunks: Vec<PtyOutputChunk>,
}

/// Queue PTY input request.
#[derive(Clone, Debug, Deserialize)]
pub struct QueuePtyInputRequest {
    /// Raw input to enqueue for connector-owned PTY delivery.
    pub input: String,
    /// Optional requester label.
    pub requested_by: Option<String>,
}

/// List PTY input events query.
#[derive(Clone, Debug, Deserialize)]
pub struct ListPtyInputEventsQuery {
    /// Only return events after this sequence number.
    pub after_sequence: Option<u64>,
    /// Maximum number of events. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// PTY input events response.
#[derive(Clone, Debug, Serialize)]
pub struct PtyInputEventsResponse {
    /// PTY session record.
    pub pty_session: PtySession,
    /// Number of returned events.
    pub count: usize,
    /// Public input event metadata.
    pub input_events: Vec<PtyInputEvent>,
}

/// Run workspace operation request.
#[derive(Clone, Debug, Deserialize)]
pub struct RunWorkspaceOperationRequest {
    /// Built-in command profile name.
    pub command_profile: String,
    /// Structured arguments.
    pub args: Vec<String>,
    /// Optional operation intent.
    pub intent: Option<String>,
    /// Optional timeout override in seconds. Shell profiles allow up to 7200.
    pub timeout_seconds: Option<u64>,
    /// Optional captured output limit override in bytes, up to 8 MiB.
    pub output_limit_bytes: Option<usize>,
}

/// Run workspace operation response.
#[derive(Clone, Debug, Serialize)]
pub struct RunWorkspaceOperationResponse {
    /// Queued operation.
    pub operation: OperationRun,
    /// Updated workspace.
    pub workspace: AgentWorkspace,
    /// Initial system output chunk.
    pub initial_output_chunk: OperationOutputChunk,
    /// Policy decision that allowed the operation.
    pub protection_decision: ProtectionDecision,
}

/// List workspace operations query.
#[derive(Clone, Debug, Deserialize)]
pub struct ListWorkspaceOperationsQuery {
    /// Maximum number of operations. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Read workspace output query.
#[derive(Clone, Debug, Deserialize)]
pub struct ReadWorkspaceOutputQuery {
    /// Optional operation id.
    pub operation_id: Option<String>,
    /// Only return chunks after this sequence number.
    pub after_sequence: Option<u64>,
    /// Maximum number of chunks. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Workspace output response.
#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceOutputResponse {
    /// Workspace record.
    pub workspace: AgentWorkspace,
    /// Number of returned chunks.
    pub count: usize,
    /// Output chunks.
    pub chunks: Vec<OperationOutputChunk>,
    /// Recent operations in this workspace.
    pub recent_operations: Vec<OperationRun>,
}

/// List workspace output artifacts query.
#[derive(Clone, Debug, Deserialize)]
pub struct ListWorkspaceOutputArtifactsQuery {
    /// Optional operation id.
    pub operation_id: Option<String>,
    /// Maximum number of artifacts. Defaults to 50 and is capped at 200.
    pub limit: Option<u32>,
}

/// Workspace output artifact response.
#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceOutputArtifactsResponse {
    /// Workspace record.
    pub workspace: AgentWorkspace,
    /// Number of returned artifacts.
    pub count: usize,
    /// Output artifacts.
    pub artifacts: Vec<OperationOutputArtifact>,
}

/// Wait workspace state request.
#[derive(Clone, Debug, Deserialize)]
pub struct WaitWorkspaceStateRequest {
    /// Desired states. Defaults to idle/done/failed/throttled/blocked/closed.
    pub desired_states: Option<Vec<WorkspaceState>>,
    /// Timeout in milliseconds. Defaults to 5000 and is capped at 60000.
    pub timeout_ms: Option<u64>,
    /// Poll interval in milliseconds. Defaults to 250 and is clamped to 100..=5000.
    pub poll_interval_ms: Option<u64>,
}

/// Wait workspace state response.
#[derive(Clone, Debug, Serialize)]
pub struct WaitWorkspaceStateResponse {
    /// Whether the workspace reached a desired state.
    pub matched: bool,
    /// Latest workspace record.
    pub workspace: AgentWorkspace,
    /// Desired states used for the wait.
    pub desired_states: Vec<WorkspaceState>,
    /// Elapsed wait time in milliseconds.
    pub elapsed_ms: u64,
    /// Suggested retry delay when not matched.
    pub retry_after_ms: Option<u64>,
}

/// API errors.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Database error.
    #[error(transparent)]
    Database(#[from] DbError),
    /// UUID parse error.
    #[error("invalid id: {0}")]
    InvalidId(#[from] uuid::Error),
    /// Resource not found.
    #[error("resource not found")]
    NotFound,
    /// Request is invalid.
    #[error("{0}")]
    BadRequest(String),
    /// Credential writes require a configured local vault key.
    #[error("credential vault is locked; configure --vault-master-password-file")]
    VaultUnavailable,
    /// Internal operation failed without exposing sensitive implementation details.
    #[error("{0}")]
    Internal(&'static str),
    /// Workspace supervisor rejected the request.
    #[error(transparent)]
    WorkspaceSupervisor(#[from] WorkspaceSupervisorError),
    /// Workspace operation rejected the request.
    #[error(transparent)]
    WorkspaceOperation(#[from] WorkspaceOperationError),
    /// PTY session supervisor rejected the request.
    #[error(transparent)]
    PtySessionSupervisor(#[from] PtySessionSupervisorError),
    /// Access cannot be resolved.
    #[error(transparent)]
    AccessResolution(#[from] AccessResolutionError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let error = self.to_string();
        let status = match &self {
            Self::Database(_) | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::WorkspaceSupervisor(WorkspaceSupervisorError::PolicyDenied(_))
            | Self::WorkspaceOperation(WorkspaceOperationError::PolicyDenied(_))
            | Self::PtySessionSupervisor(PtySessionSupervisorError::PolicyDenied(_)) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::InvalidId(_) | Self::BadRequest(_) | Self::WorkspaceSupervisor(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::WorkspaceOperation(_) | Self::PtySessionSupervisor(_) => StatusCode::BAD_REQUEST,
            Self::AccessResolution(error)
                if matches!(
                    error.state,
                    remote_hosts_domain::EntityState::Throttled
                        | remote_hosts_domain::EntityState::TargetOverloaded
                        | remote_hosts_domain::EntityState::RateLimited
                ) =>
            {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::VaultUnavailable | Self::AccessResolution(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        let body = Json(ErrorResponse { error });
        (status, body).into_response()
    }
}

#[derive(Clone, Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

/// Returns a request id maker for future API middleware.
pub fn request_id_maker() -> MakeRequestUuid {
    MakeRequestUuid
}

#[cfg(test)]
mod tests {
    use axum::{body, http::Request};
    use remote_hosts_db::{Repositories, connect_sqlite, migrate};
    use remote_hosts_domain::{
        AccessPath, AccessPathHealth, AccessPathId, AgentWorkspace, ConnectionMode,
        ConnectionSession, Connector, ConnectorId, CredentialId, CredentialKind,
        CredentialMetadata, EntityState, Environment, EnvironmentId, EnvironmentKind, Host, HostId,
        HostKind, OutputStream, Protocol, PtyOutputChunk, PtyOutputChunkId, PtySessionId,
        RiskLevel, RouteType, SessionId, StoredCredential, TrustLevel, WorkspaceId, WorkspaceState,
        now_utc,
    };
    use secrecy::SecretString;
    use serde_json::json;
    use tower::ServiceExt;

    use super::{ApiState, router_with_state};

    #[tokio::test]
    async fn list_hosts_returns_registered_hosts() -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();
        repos
            .hosts
            .insert(&Host {
                id: HostId::new(),
                name: "macstudio-home".to_owned(),
                display_name: "Mac Studio Home".to_owned(),
                kind: HostKind::Macos,
                owner: None,
                tags: vec!["home".to_owned()],
                description: None,
                risk_level: RiskLevel::Personal,
                created_at: now,
                updated_at: now,
            })
            .await?;

        let app = router_with_state(ApiState::new(repos));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/hosts")
                    .body(body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(body[0]["name"], "macstudio-home");
        Ok(())
    }

    #[tokio::test]
    async fn resolve_access_returns_healthy_path() -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();

        let host = Host {
            id: HostId::new(),
            name: "company-4090-a".to_owned(),
            display_name: "Company 4090 A".to_owned(),
            kind: HostKind::GpuServer,
            owner: None,
            tags: vec!["gpu".to_owned()],
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        repos.hosts.insert(&host).await?;

        let environment = Environment {
            id: EnvironmentId::new(),
            name: "company-lan".to_owned(),
            kind: EnvironmentKind::CompanyLan,
            description: None,
            trust_level: TrustLevel::Trusted,
            notes: None,
        };
        repos.environments.insert(&environment).await?;

        let credential_id = CredentialId::new();
        repos
            .credentials
            .insert(&StoredCredential {
                metadata: CredentialMetadata {
                    id: credential_id,
                    name: "4090 ssh".to_owned(),
                    kind: CredentialKind::SshPrivateKey,
                    username_hint: Some("ops".to_owned()),
                    created_at: now,
                    updated_at: now,
                    last_used_at: None,
                },
                encrypted_blob_json: json!({"version": 1}),
            })
            .await?;

        let access_path = AccessPath {
            id: AccessPathId::new(),
            host_id: host.id,
            environment_id: environment.id,
            connector_id: None,
            protocol: Protocol::Ssh,
            address: "10.0.0.10".to_owned(),
            port: 22,
            username: "ops".to_owned(),
            credential_id,
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority: 1,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 1,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        };
        repos.access_paths.insert(&access_path).await?;
        repos
            .access_path_health
            .upsert(&AccessPathHealth {
                access_path_id: access_path.id,
                state: EntityState::Healthy,
                last_checked_at: Some(now),
                latency_ms: Some(8),
                failure_count: 0,
                last_error_code: None,
                next_retry_at: None,
            })
            .await?;

        let app = router_with_state(ApiState::new(repos));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/hosts/{}/resolve-access", host.id))
                    .body(body::Body::empty())?,
            )
            .await?;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(body["selected_access_path"]["address"], "10.0.0.10");
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn connector_heartbeat_records_state_event() -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();

        let host = Host {
            id: HostId::new(),
            name: "home-macstudio".to_owned(),
            display_name: "Home Mac Studio".to_owned(),
            kind: HostKind::Macos,
            owner: None,
            tags: vec!["home".to_owned()],
            description: None,
            risk_level: RiskLevel::Personal,
            created_at: now,
            updated_at: now,
        };
        repos.hosts.insert(&host).await?;
        let environment = Environment {
            id: EnvironmentId::new(),
            name: "home-lan".to_owned(),
            kind: EnvironmentKind::HomeLan,
            description: None,
            trust_level: TrustLevel::Owned,
            notes: None,
        };
        repos.environments.insert(&environment).await?;
        let connector = Connector {
            id: ConnectorId::new(),
            name: "home-connector".to_owned(),
            environment_id: environment.id,
            host_id: Some(host.id),
            version: "0.1.0".to_owned(),
            state: EntityState::ConnectorOffline,
            last_seen_at: None,
            current_network: None,
        };
        repos.connectors.upsert(&connector).await?;

        let app = router_with_state(ApiState::new(repos));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/connectors/{}/heartbeat", connector.id))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "state": "healthy",
                            "version": "0.2.0",
                            "current_network": "home-wifi"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
        let response_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(response_body["connector"]["state"], "healthy");
        assert_eq!(response_body["snapshot"]["state"], "healthy");
        assert_eq!(response_body["event"]["old_state"], "connector_offline");

        let events_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/connectors/{}/events", connector.id))
                    .body(body::Body::empty())?,
            )
            .await?;
        assert_eq!(events_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(events_response.into_body(), usize::MAX).await?;
        let events_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(events_body.as_array().map(Vec::len), Some(1));

        let live_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runtime-events/wait")
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "start_mode": "live_only",
                            "entity_type": "connector",
                            "entity_id": connector.id,
                            "timeout_ms": 0
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(live_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(live_response.into_body(), usize::MAX).await?;
        let live_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(live_body["start_cursor"], 1);
        assert_eq!(live_body["timed_out"], true);
        assert_eq!(live_body["events"], json!([]));

        let replay_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runtime-events/wait")
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "start_mode": "after_cursor",
                            "after_cursor": 0,
                            "entity_type": "connector",
                            "entity_id": connector.id,
                            "timeout_ms": 0
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(replay_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(replay_response.into_body(), usize::MAX).await?;
        let replay_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(replay_body["next_cursor"], 1);
        assert_eq!(replay_body["timed_out"], false);
        assert_eq!(replay_body["count"], 1);
        assert_eq!(replay_body["events"][0]["sequence"], 1);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn workspace_creation_supports_bounded_multi_conversation_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();

        let host = Host {
            id: HostId::new(),
            name: "company-4090-b".to_owned(),
            display_name: "Company 4090 B".to_owned(),
            kind: HostKind::GpuServer,
            owner: None,
            tags: vec!["gpu".to_owned()],
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        repos.hosts.insert(&host).await?;
        let environment = Environment {
            id: EnvironmentId::new(),
            name: "company-lan-workspace".to_owned(),
            kind: EnvironmentKind::CompanyLan,
            description: None,
            trust_level: TrustLevel::Trusted,
            notes: None,
        };
        repos.environments.insert(&environment).await?;
        let connector = Connector {
            id: ConnectorId::new(),
            name: "office-connector-workspace".to_owned(),
            environment_id: environment.id,
            host_id: None,
            version: "0.1.0".to_owned(),
            state: EntityState::Healthy,
            last_seen_at: Some(now),
            current_network: Some("company".to_owned()),
        };
        repos.connectors.upsert(&connector).await?;
        let credential_id = CredentialId::new();
        repos
            .credentials
            .insert(&StoredCredential {
                metadata: CredentialMetadata {
                    id: credential_id,
                    name: "4090 workspace ssh".to_owned(),
                    kind: CredentialKind::SshPrivateKey,
                    username_hint: Some("ops".to_owned()),
                    created_at: now,
                    updated_at: now,
                    last_used_at: None,
                },
                encrypted_blob_json: json!({"version": 1}),
            })
            .await?;
        let access_path = AccessPath {
            id: AccessPathId::new(),
            host_id: host.id,
            environment_id: environment.id,
            connector_id: Some(connector.id),
            protocol: Protocol::Ssh,
            address: "10.0.0.20".to_owned(),
            port: 22,
            username: "ops".to_owned(),
            credential_id,
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority: 1,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 1,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        };
        repos.access_paths.insert(&access_path).await?;

        let app = router_with_state(ApiState::new(repos));
        let create_body = json!({
            "access_path_id": access_path.id,
            "label": "agent-main",
            "cwd": "/tmp"
        })
        .to_string();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/hosts/{}/workspaces", host.id))
                    .header("content-type", "application/json")
                    .body(body::Body::from(create_body.clone()))?,
            )
            .await?;

        assert_eq!(response.status(), axum::http::StatusCode::CREATED);
        let bytes = body::to_bytes(response.into_body(), usize::MAX).await?;
        let response_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(response_body["state"], "idle");
        let workspace_id = response_body["id"]
            .as_str()
            .ok_or("workspace id should be a string")?;

        let run_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/workspaces/{workspace_id}/operations"))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "command_profile": "host.uptime",
                            "args": [],
                            "intent": "check whether the host is responsive"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(run_response.status(), axum::http::StatusCode::ACCEPTED);
        let bytes = body::to_bytes(run_response.into_body(), usize::MAX).await?;
        let run_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(run_body["operation"]["state"], "queued");
        assert_eq!(run_body["workspace"]["state"], "working");
        let operation_id = run_body["operation"]["id"]
            .as_str()
            .ok_or("operation id should be a string")?;

        let output_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/workspaces/{workspace_id}/output?operation_id={operation_id}"
                    ))
                    .body(body::Body::empty())?,
            )
            .await?;
        assert_eq!(output_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(output_response.into_body(), usize::MAX).await?;
        let output_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(output_body["chunks"][0]["stream"], "system");

        for _ in 1..32 {
            let additional_response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/v1/hosts/{}/workspaces", host.id))
                        .header("content-type", "application/json")
                        .body(body::Body::from(create_body.clone()))?,
                )
                .await?;
            assert_eq!(
                additional_response.status(),
                axum::http::StatusCode::CREATED
            );
        }

        let limited_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/hosts/{}/workspaces", host.id))
                    .header("content-type", "application/json")
                    .body(body::Body::from(create_body))?,
            )
            .await?;
        assert_eq!(
            limited_response.status(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );

        let list_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/hosts/{}/workspaces", host.id))
                    .body(body::Body::empty())?,
            )
            .await?;
        let bytes = body::to_bytes(list_response.into_body(), usize::MAX).await?;
        let list_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(list_body.as_array().map(Vec::len), Some(32));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn pty_lifecycle_routes_manage_workspace_state() -> Result<(), Box<dyn std::error::Error>>
    {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let now = now_utc();

        let host = Host {
            id: HostId::new(),
            name: "pty-host".to_owned(),
            display_name: "PTY Host".to_owned(),
            kind: HostKind::Linux,
            owner: None,
            tags: Vec::new(),
            description: None,
            risk_level: RiskLevel::Development,
            created_at: now,
            updated_at: now,
        };
        repos.hosts.insert(&host).await?;
        let environment = Environment {
            id: EnvironmentId::new(),
            name: "pty-env".to_owned(),
            kind: EnvironmentKind::CompanyLan,
            description: None,
            trust_level: TrustLevel::Trusted,
            notes: None,
        };
        repos.environments.insert(&environment).await?;
        let connector = Connector {
            id: ConnectorId::new(),
            name: "pty-connector".to_owned(),
            environment_id: environment.id,
            host_id: None,
            version: "0.1.0".to_owned(),
            state: EntityState::Healthy,
            last_seen_at: Some(now),
            current_network: Some("test".to_owned()),
        };
        repos.connectors.upsert(&connector).await?;
        let credential_id = CredentialId::new();
        repos
            .credentials
            .insert(&StoredCredential {
                metadata: CredentialMetadata {
                    id: credential_id,
                    name: "pty credential".to_owned(),
                    kind: CredentialKind::SshPrivateKey,
                    username_hint: Some("ops".to_owned()),
                    created_at: now,
                    updated_at: now,
                    last_used_at: None,
                },
                encrypted_blob_json: json!({"version": 1}),
            })
            .await?;
        let access_path = AccessPath {
            id: AccessPathId::new(),
            host_id: host.id,
            environment_id: environment.id,
            connector_id: Some(connector.id),
            protocol: Protocol::Ssh,
            address: "10.0.0.50".to_owned(),
            port: 22,
            username: "ops".to_owned(),
            credential_id,
            route_type: RouteType::Lan,
            proxy_chain: Vec::new(),
            priority: 1,
            enabled: true,
            connection_mode: ConnectionMode::Pooled,
            idle_ttl_seconds: 600,
            keepalive_seconds: 30,
            max_concurrent_channels: 1,
            max_new_connections_per_minute: 1,
            requires_tty: false,
            notes: None,
        };
        repos.access_paths.insert(&access_path).await?;
        let workspace = AgentWorkspace {
            id: WorkspaceId::new(),
            agent_session_id: None,
            host_id: host.id,
            access_path_id: access_path.id,
            connector_id: connector.id,
            label: "agent-main".to_owned(),
            cwd: Some("/tmp".to_owned()),
            state: WorkspaceState::Idle,
            policy_profile: "default".to_owned(),
            created_at: now,
            last_activity_at: now,
            ttl_seconds: 3600,
        };
        repos.workspaces.insert(&workspace).await?;
        let session = ConnectionSession {
            session_id: SessionId::new(),
            access_path_id: access_path.id,
            connector_id: connector.id,
            state: EntityState::Connected,
            created_at: now,
            last_used_at: now,
            open_channels: 1,
            reused_count: 0,
            failure_count: 0,
            last_error: None,
        };
        repos.connection_sessions.upsert(&session).await?;

        let app = router_with_state(ApiState::new(repos.clone()));
        let open_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/workspaces/{}/pty-sessions", workspace.id))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "session_id": session.session_id.to_string(),
                            "cwd": "/tmp"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(open_response.status(), axum::http::StatusCode::CREATED);
        let bytes = body::to_bytes(open_response.into_body(), usize::MAX).await?;
        let open_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        let pty_session_id = open_body["pty_session_id"]
            .as_str()
            .ok_or("pty_session_id should be a string")?;
        assert_eq!(open_body["input_allowed"], true);
        let pty_session_id_value = PtySessionId::from(uuid::Uuid::parse_str(pty_session_id)?);
        repos
            .pty_output_chunks
            .insert(&PtyOutputChunk {
                id: PtyOutputChunkId::new(),
                pty_session_id: pty_session_id_value,
                workspace_id: workspace.id,
                stream: OutputStream::Stdout,
                sequence: 0,
                redacted_text: "ready password=[REDACTED]".to_owned(),
                byte_len: 25,
                truncated: false,
                created_at: now,
            })
            .await?;

        let pty_output_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/pty-sessions/{pty_session_id}/output?limit=10"))
                    .body(body::Body::empty())?,
            )
            .await?;
        assert_eq!(pty_output_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(pty_output_response.into_body(), usize::MAX).await?;
        let pty_output_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(pty_output_body["count"], 1);
        assert_eq!(pty_output_body["chunks"][0]["stream"], "stdout");
        assert!(
            pty_output_body["chunks"][0]["redacted_text"]
                .as_str()
                .unwrap_or_default()
                .contains("[REDACTED]")
        );
        assert!(!pty_output_body.to_string().contains("hunter2"));

        let input_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/pty-sessions/{pty_session_id}/input"))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "input": "echo hello\n",
                            "requested_by": "agent"
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(input_response.status(), axum::http::StatusCode::ACCEPTED);
        let bytes = body::to_bytes(input_response.into_body(), usize::MAX).await?;
        let input_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(input_body["state"], "queued");
        assert_eq!(
            input_body["redacted_input_summary"],
            "11 bytes queued for pty input"
        );
        assert!(!input_body.to_string().contains("echo hello"));

        let input_events_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/pty-sessions/{pty_session_id}/input-events?limit=10"
                    ))
                    .body(body::Body::empty())?,
            )
            .await?;
        assert_eq!(input_events_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(input_events_response.into_body(), usize::MAX).await?;
        let input_events_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(input_events_body["count"], 1);
        assert_eq!(input_events_body["input_events"][0]["state"], "queued");
        assert!(!input_events_body.to_string().contains("echo hello"));

        let heartbeat_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/pty-sessions/{pty_session_id}/heartbeat"))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "state": "working",
                            "foreground_process": "python train.py",
                            "cwd": "/srv/app",
                            "recent_output_ref": "artifact:latest",
                            "last_exit_code": null,
                            "input_allowed": true
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(heartbeat_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(heartbeat_response.into_body(), usize::MAX).await?;
        let heartbeat_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(heartbeat_body["state"], "working");

        let workspace_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/workspaces/{}", workspace.id))
                    .body(body::Body::empty())?,
            )
            .await?;
        let bytes = body::to_bytes(workspace_response.into_body(), usize::MAX).await?;
        let workspace_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(workspace_body["state"], "working");

        let close_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/pty-sessions/{pty_session_id}/close"))
                    .header("content-type", "application/json")
                    .body(body::Body::from(json!({"last_exit_code": 0}).to_string()))?,
            )
            .await?;
        assert_eq!(close_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(close_response.into_body(), usize::MAX).await?;
        let close_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(close_body["state"], "closed");
        assert_eq!(close_body["input_allowed"], false);
        assert_eq!(close_body["last_exit_code"], 0);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn topology_snapshot_sync_is_idempotent_and_marks_missing_members_inactive()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let app = router_with_state(ApiState::new(repos));

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/topology/sync")
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "scope_key": "cluster:factory-a",
                            "source": "inventory-agent",
                            "nodes": [
                                {
                                    "external_key": "cluster:factory-a",
                                    "name": "Factory A",
                                    "kind": "cluster"
                                },
                                {
                                    "external_key": "proxy:factory-a",
                                    "name": "Factory ingress",
                                    "kind": "reverse_proxy",
                                    "address": "10.20.0.10",
                                    "metadata": {"software": "nginx"}
                                },
                                {
                                    "external_key": "service:factory-a-api",
                                    "name": "Factory API",
                                    "kind": "business_service",
                                    "address": "10.20.0.21",
                                    "ports": [8080]
                                }
                            ],
                            "edges": [
                                {
                                    "external_key": "proxy-to-api",
                                    "from": "proxy:factory-a",
                                    "to": "service:factory-a-api",
                                    "relation": "proxies_to"
                                }
                            ]
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(first.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(first.into_body(), usize::MAX).await?;
        let first_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(first_body["active_node_count"], 3);
        assert_eq!(first_body["active_edge_count"], 1);

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/topology/sync")
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "scope_key": "cluster:factory-a",
                            "source": "inventory-agent",
                            "nodes": [
                                {
                                    "external_key": "cluster:factory-a",
                                    "name": "Factory A",
                                    "kind": "cluster"
                                },
                                {
                                    "external_key": "proxy:factory-a",
                                    "name": "Factory ingress v2",
                                    "kind": "reverse_proxy",
                                    "address": "10.20.0.10",
                                    "metadata": {"software": "caddy"}
                                }
                            ],
                            "edges": []
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(second.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(second.into_body(), usize::MAX).await?;
        let second_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(second_body["active_node_count"], 2);
        assert_eq!(second_body["inactive_node_count"], 1);
        assert_eq!(second_body["inactive_edge_count"], 1);

        let graph = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/topology?include_inactive=true")
                    .body(body::Body::empty())?,
            )
            .await?;
        assert_eq!(graph.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(graph.into_body(), usize::MAX).await?;
        let graph_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(graph_body["nodes"].as_array().map(Vec::len), Some(3));
        assert_eq!(graph_body["edges"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            graph_body["nodes"]
                .as_array()
                .and_then(|nodes| nodes
                    .iter()
                    .find(|node| node["external_key"] == "proxy:factory-a"))
                .map(|node| node["name"].clone()),
            Some(json!("Factory ingress v2"))
        );
        assert_eq!(
            graph_body["nodes"]
                .as_array()
                .and_then(|nodes| nodes
                    .iter()
                    .find(|node| node["external_key"] == "service:factory-a-api"))
                .map(|node| node["active"].clone()),
            Some(json!(false))
        );

        let admin = app
            .oneshot(Request::builder().uri("/admin").body(body::Body::empty())?)
            .await?;
        assert_eq!(admin.status(), axum::http::StatusCode::OK);
        assert_eq!(
            admin
                .headers()
                .get("cache-control")
                .and_then(|value| value.to_str().ok()),
            Some("no-store, max-age=0")
        );
        assert!(
            admin
                .headers()
                .get("content-security-policy")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("frame-ancestors 'none'"))
        );
        let bytes = body::to_bytes(admin.into_body(), usize::MAX).await?;
        let html = String::from_utf8(bytes.to_vec())?;
        assert!(html.contains("Remote Hosts"));
        assert!(html.contains("/v1/admin/overview"));
        Ok(())
    }

    #[tokio::test]
    async fn topology_metadata_rejects_secret_like_fields() -> Result<(), Box<dyn std::error::Error>>
    {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let app = router_with_state(ApiState::new(repos.clone()));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/topology/sync")
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "scope_key": "service:unsafe",
                            "source": "manual",
                            "nodes": [{
                                "external_key": "service:unsafe",
                                "name": "Unsafe service",
                                "kind": "business_service",
                                "metadata": {"admin_password": "must-not-be-stored"}
                            }],
                            "edges": []
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        assert!(
            repos
                .topology
                .get_node_by_external_key("service:unsafe")
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn topology_credentials_are_encrypted_bound_and_never_returned()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = connect_sqlite("sqlite::memory:").await?;
        migrate(&pool).await?;
        let repos = Repositories::new(pool);
        let app = router_with_state(ApiState::with_vault_master_password(
            repos.clone(),
            SecretString::from("local-test-vault-key".to_owned()),
        ));
        let sync_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/topology/sync")
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "scope_key": "service:billing",
                            "source": "manual",
                            "nodes": [{
                                "external_key": "service:billing",
                                "name": "Billing API",
                                "kind": "business_service"
                            }],
                            "edges": []
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(sync_response.status(), axum::http::StatusCode::OK);
        let node = repos
            .topology
            .get_node_by_external_key("service:billing")
            .await?
            .ok_or("topology node should exist")?;
        let plaintext = "internal-billing-password";
        let credential_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/topology/nodes/{}/credentials", node.id))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "name": "billing-admin",
                            "kind": "basic_auth",
                            "username_hint": "admin",
                            "purpose": "admin",
                            "secret": {"password": plaintext}
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(credential_response.status(), axum::http::StatusCode::OK);
        let bytes = body::to_bytes(credential_response.into_body(), usize::MAX).await?;
        let credential_body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(credential_body["credential"]["name"], "billing-admin");
        assert_eq!(credential_body["purpose"], "admin");
        assert!(!credential_body.to_string().contains(plaintext));

        let stored = repos
            .credentials
            .get_by_name("billing-admin")
            .await?
            .ok_or("credential should exist")?;
        assert!(!stored.encrypted_blob_json.to_string().contains(plaintext));

        let overview_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/overview")
                    .body(body::Body::empty())?,
            )
            .await?;
        let bytes = body::to_bytes(overview_response.into_body(), usize::MAX).await?;
        let overview_text = String::from_utf8(bytes.to_vec())?;
        assert!(overview_text.contains("billing-admin"));
        assert!(!overview_text.contains(plaintext));

        let locked_app = router_with_state(ApiState::new(repos));
        let locked_response = locked_app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/topology/nodes/{}/credentials", node.id))
                    .header("content-type", "application/json")
                    .body(body::Body::from(
                        json!({
                            "name": "billing-readonly",
                            "kind": "basic_auth",
                            "username_hint": "viewer",
                            "purpose": "readonly",
                            "secret": {"password": "another-secret"}
                        })
                        .to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(
            locked_response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        Ok(())
    }
}
