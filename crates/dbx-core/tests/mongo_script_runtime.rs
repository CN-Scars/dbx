#![cfg(feature = "mongo-js-runtime")]

use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use dbx_core::connection::AppState;
use dbx_core::models::connection::ConnectionConfig;
use dbx_core::mongo_script::{
    execute_mongo_script, execute_mongo_script_managed_core, mongo_script_error_kind, MongoScriptErrorKind,
    MongoScriptHost, MongoScriptLimits, MongoScriptOperation, MongoScriptOutput, MongoScriptRequest, MongoScriptResult,
};
use dbx_core::mongo_shell::{MongoCollectionMethod, MongoDatabaseMethod};
use dbx_core::storage::Storage;
use serde_json::{json, Value};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct RecordingHost {
    operations: Mutex<Vec<MongoScriptOperation>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl RecordingHost {
    fn operations(&self) -> Vec<MongoScriptOperation> {
        self.operations.lock().unwrap().clone()
    }
}

#[async_trait]
impl MongoScriptHost for RecordingHost {
    async fn execute(&self, operation: MongoScriptOperation) -> Result<Value, String> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.operations.lock().unwrap().push(operation.clone());

        let result = match &operation {
            MongoScriptOperation::SelectDatabase { database } => Ok(json!({ "selected": database })),
            MongoScriptOperation::DatabaseCall { method, args, .. } => match method {
                MongoDatabaseMethod::Version => Ok(json!("5.0.18")),
                MongoDatabaseMethod::RunCommand if args.first().is_some_and(|value| value.get("fail").is_some()) => {
                    Err("expected host failure".to_string())
                }
                MongoDatabaseMethod::RunCommand => Ok(args.first().cloned().unwrap_or(Value::Null)),
                MongoDatabaseMethod::CreateUser => Ok(json!({ "acknowledged": true })),
            },
            MongoScriptOperation::CollectionCall { method, args, .. } => match method {
                MongoCollectionMethod::FindOne if args.first().is_some_and(|value| value.get("extended").is_some()) => {
                    Ok(json!({
                        "id": { "$oid": "507f1f77bcf86cd799439011" },
                        "createdAt": { "$date": "2026-08-12T00:00:00.000Z" },
                        "nested": [{ "owner": { "$oid": "507f191e810c19729de860ea" } }]
                    }))
                }
                MongoCollectionMethod::FindOne => Ok(args.first().cloned().unwrap_or(Value::Null)),
                MongoCollectionMethod::Find | MongoCollectionMethod::Aggregate => Ok(json!([
                    { "index": 1, "kind": "first" },
                    { "index": 2, "kind": "second" }
                ])),
                MongoCollectionMethod::Drop => Err("expected host failure".to_string()),
                _ => Ok(json!({ "ok": 1 })),
            },
        };
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

struct PendingHost {
    started: Arc<Notify>,
}

#[async_trait]
impl MongoScriptHost for PendingHost {
    async fn execute(&self, _operation: MongoScriptOperation) -> Result<Value, String> {
        self.started.notify_one();
        pending::<Result<Value, String>>().await
    }
}

fn request(source: impl Into<String>) -> MongoScriptRequest {
    MongoScriptRequest {
        connection_id: "test-connection".to_string(),
        database: "initial_database".to_string(),
        source: source.into(),
        execution_id: Some("test-execution".to_string()),
        max_rows: 100,
        timeout_secs: None,
        dangerous_operation_confirmed: true,
    }
}

async fn execute(source: &str) -> Result<dbx_core::mongo_script::MongoScriptResult, String> {
    execute_mongo_script(
        request(source),
        MongoScriptLimits::default(),
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
}

async fn app_state() -> (Arc<AppState>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(&dir.path().join("storage.db")).await.unwrap();
    let state = Arc::new(AppState::new_with_plugin_dir(storage, dir.path().join("plugins")));
    (state, dir)
}

#[test]
fn transport_contract_uses_camel_case_and_preserves_nested_results() {
    let value = json!({
        "connectionId": "mongo-1",
        "database": "app",
        "source": "printjson({ nested: [1, { ok: true }] });",
        "executionId": "script-1",
        "maxRows": 250,
        "timeoutSecs": 5,
        "dangerousOperationConfirmed": true
    });
    let request: MongoScriptRequest = serde_json::from_value(value).unwrap();
    assert!(request.dangerous_operation_confirmed);
    assert_eq!(request.execution_id.as_deref(), Some("script-1"));

    let omitted_confirmation: MongoScriptRequest = serde_json::from_value(json!({
        "connectionId": "mongo-1",
        "database": "app",
        "source": "1 + 1",
        "maxRows": 250
    }))
    .unwrap();
    assert!(!omitted_confirmation.dangerous_operation_confirmed);

    let serialized = serde_json::to_value(MongoScriptResult {
        final_value: Some(json!({ "nested": [1, { "ok": true }] })),
        output: vec![MongoScriptOutput::Json(json!({ "items": [{ "id": 1 }] }))],
        operation_count: 2,
        succeeded_operation_count: 2,
        current_database: "app".to_string(),
        truncated: false,
    })
    .unwrap();
    assert_eq!(serialized["finalValue"]["nested"][1]["ok"], json!(true));
    assert_eq!(serialized["output"][0]["kind"], json!("json"));
    assert_eq!(serialized["succeededOperationCount"], json!(2));
    assert_eq!(serialized["currentDatabase"], json!("app"));
}

#[tokio::test]
async fn managed_execution_requires_confirmation_before_starting_the_runtime() {
    let (state, _dir) = app_state().await;
    let mut unconfirmed = request("1 + 1");
    unconfirmed.dangerous_operation_confirmed = false;

    let error = execute_mongo_script_managed_core(state, unconfirmed, MongoScriptLimits::default()).await.unwrap_err();
    assert_eq!(mongo_script_error_kind(&error), Some(MongoScriptErrorKind::Safety));
    assert!(error.contains("requires explicit dangerous-operation confirmation"));
}

#[tokio::test]
async fn managed_execution_blocks_read_only_connections_before_starting_the_runtime() {
    let (state, _dir) = app_state().await;
    let config: ConnectionConfig = serde_json::from_value(json!({
        "id": "test-connection",
        "name": "Read-only MongoDB",
        "db_type": "mongodb",
        "host": "localhost",
        "port": 27017,
        "username": "tester",
        "password": "",
        "database": "initial_database",
        "read_only": true
    }))
    .unwrap();
    state.configs.write().await.insert(config.id.clone(), config);

    let error =
        execute_mongo_script_managed_core(state, request("1 + 1"), MongoScriptLimits::default()).await.unwrap_err();
    assert_eq!(mongo_script_error_kind(&error), Some(MongoScriptErrorKind::Safety));
    assert!(error.contains("Read-only MongoDB"));
    assert!(error.contains("Run MongoDB shell JavaScript blocked"));
}

#[tokio::test]
async fn managed_execution_registers_the_whole_script_once_and_cancels_it() {
    let (state, _dir) = app_state().await;
    let execution_id = "managed-script-cancel";
    let mut script_request = request("while (true) {}");
    script_request.execution_id = Some(execution_id.to_string());
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        execute_mongo_script_managed_core(task_state, script_request, MongoScriptLimits::default()).await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let diagnostics = state.running_queries.diagnostics();
            if diagnostics.active_execution_ids == [execution_id]
                && diagnostics.active_by_connection.get("test-connection") == Some(&1)
                && diagnostics.interrupt_registrations == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the whole script must be registered exactly once");

    assert!(state.running_queries.cancel(execution_id));
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancellation must interrupt the managed script")
        .unwrap()
        .unwrap_err();
    assert_eq!(mongo_script_error_kind(&error), Some(MongoScriptErrorKind::Cancelled));
    let diagnostics = state.running_queries.diagnostics();
    assert!(diagnostics.active_execution_ids.is_empty());
    assert_eq!(diagnostics.interrupt_registrations, 0);
}

#[tokio::test]
async fn executes_variables_loops_functions_and_final_values() {
    let result = execute(
        r#"
        function sumEven(limit) {
          let total = 0;
          for (let index = 0; index < limit; index += 1) {
            if (index % 2 === 0) total += index;
          }
          return total;
        }
        const total = sumEven(8);
        print("total", total);
        ({ total, branch: total === 12 ? "expected" : "unexpected" });
        "#,
    )
    .await
    .unwrap();

    assert_eq!(result.final_value, Some(json!({ "total": 12, "branch": "expected" })));
    assert_eq!(result.output, vec![MongoScriptOutput::Text("total 12".to_string())]);
    assert_eq!(result.operation_count, 0);
    assert_eq!(result.succeeded_operation_count, 0);
    assert!(!result.truncated);
}

#[tokio::test]
async fn captures_json_output_and_truncates_without_stopping_the_script() {
    let limits = MongoScriptLimits {
        max_output_items: 2,
        max_output_bytes: 128,
        max_value_bytes: 32,
        ..MongoScriptLimits::default()
    };
    let result = execute_mongo_script(
        request(
            r#"
            printjson({ index: 1, tags: ["mongo", "quickjs"] });
            print("second");
            print("x".repeat(256));
            42;
            "#,
        ),
        limits,
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        result.output,
        vec![
            MongoScriptOutput::Json(json!({ "index": 1, "tags": ["mongo", "quickjs"] })),
            MongoScriptOutput::Text("second".to_string()),
        ]
    );
    assert_eq!(result.final_value, Some(json!(42)));
    assert!(result.truncated);
}

#[tokio::test]
async fn serializes_host_calls_and_preserves_order_and_catchable_errors() {
    let host = Arc::new(RecordingHost::default());
    let result = execute_mongo_script(
        request(
            r#"
            const values = [];
            for (let index = 0; index < 3; index += 1) {
              values.push(db.items.findOne({ index }));
            }
            let caught = false;
            try {
              db.items.drop();
            } catch (error) {
              caught = error.message.includes("expected host failure");
            }
            ({ values, caught });
            "#,
        ),
        MongoScriptLimits::default(),
        host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        result.final_value,
        Some(json!({ "values": [{ "index": 0 }, { "index": 1 }, { "index": 2 }], "caught": true }))
    );
    assert_eq!(result.operation_count, 4);
    assert_eq!(result.succeeded_operation_count, 3);
    assert_eq!(host.max_active.load(Ordering::SeqCst), 1);
    let operations = host.operations();
    assert!(operations[..3].iter().all(|operation| matches!(
        operation,
        MongoScriptOperation::CollectionCall { method: MongoCollectionMethod::FindOne, .. }
    )));
    assert!(matches!(operations[3], MongoScriptOperation::CollectionCall { method: MongoCollectionMethod::Drop, .. }));
}

#[tokio::test]
async fn round_trips_object_id_and_iso_date_extended_json() {
    let host = Arc::new(RecordingHost::default());
    let result = execute_mongo_script(
        request(
            r#"
            const sent = {
              id: ObjectId("507f1f77bcf86cd799439011"),
              createdAt: ISODate("2026-08-12T00:00:00Z"),
            };
            const received = db.items.findOne({ extended: true, sent });
            ({
              sent,
              received,
              objectIdText: received.id.toString(),
              isoText: received.createdAt.toISOString(),
              nestedOwnerText: received.nested[0].owner.toString(),
            });
            "#,
        ),
        MongoScriptLimits::default(),
        host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let operation = host.operations().into_iter().next().unwrap();
    let MongoScriptOperation::CollectionCall { args, .. } = operation else {
        panic!("expected a collection call");
    };
    assert_eq!(args[0]["sent"]["id"], json!({ "$oid": "507f1f77bcf86cd799439011" }));
    assert_eq!(args[0]["sent"]["createdAt"], json!({ "$date": "2026-08-12T00:00:00.000Z" }));

    let final_value = result.final_value.unwrap();
    assert_eq!(final_value["received"]["id"], json!({ "$oid": "507f1f77bcf86cd799439011" }));
    assert_eq!(final_value["objectIdText"], json!("ObjectId(\"507f1f77bcf86cd799439011\")"));
    assert_eq!(final_value["isoText"], json!("2026-08-12T00:00:00.000Z"));
    assert_eq!(final_value["nestedOwnerText"], json!("ObjectId(\"507f191e810c19729de860ea\")"));
}

#[tokio::test]
async fn tracks_the_current_database_after_a_successful_selection() {
    let result = execute(
        r#"
        db = db.getSiblingDB("analytics");
        db.getName();
        "#,
    )
    .await
    .unwrap();

    assert_eq!(result.current_database, "analytics");
    assert_eq!(result.operation_count, 1);
    assert_eq!(result.succeeded_operation_count, 1);
}

#[tokio::test]
async fn does_not_expose_node_browser_or_system_globals() {
    let result = execute(
        r#"
        ({
          require: typeof globalThis.require,
          process: typeof globalThis.process,
          fetch: typeof globalThis.fetch,
          webSocket: typeof globalThis.WebSocket,
          document: typeof globalThis.document,
          tauri: typeof globalThis.__TAURI__,
          deno: typeof globalThis.Deno,
          bun: typeof globalThis.Bun,
        });
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        result.final_value,
        Some(json!({
            "require": "undefined",
            "process": "undefined",
            "fetch": "undefined",
            "webSocket": "undefined",
            "document": "undefined",
            "tauri": "undefined",
            "deno": "undefined",
            "bun": "undefined",
        }))
    );

    let module_error = execute("import('dbx-unavailable-module');").await.unwrap_err();
    assert!(matches!(
        mongo_script_error_kind(&module_error),
        Some(MongoScriptErrorKind::Runtime | MongoScriptErrorKind::Serialization)
    ));
}

#[tokio::test]
async fn reports_runtime_exceptions_and_invalid_requests() {
    let runtime_error = execute("throw new Error('expected runtime failure');").await.unwrap_err();
    assert_eq!(mongo_script_error_kind(&runtime_error), Some(MongoScriptErrorKind::Runtime));
    assert!(runtime_error.contains("expected runtime failure"));

    let mut invalid = request("1");
    invalid.connection_id.clear();
    let invalid_error = execute_mongo_script(
        invalid,
        MongoScriptLimits::default(),
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(mongo_script_error_kind(&invalid_error), Some(MongoScriptErrorKind::InvalidRequest));
}

#[tokio::test]
async fn enforces_operation_value_memory_and_stack_limits() {
    let operation_limits = MongoScriptLimits { max_operations: 2, ..MongoScriptLimits::default() };
    let operation_error = execute_mongo_script(
        request(
            r#"
            for (let index = 0; index < 3; index += 1) {
              db.items.findOne({ index });
            }
            "#,
        ),
        operation_limits,
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(operation_error.contains("operation limit of 2 exceeded"));
    assert_eq!(mongo_script_error_kind(&operation_error), Some(MongoScriptErrorKind::ResourceLimit));

    let value_limits = MongoScriptLimits { max_value_depth: 3, ..MongoScriptLimits::default() };
    let value_error = execute_mongo_script(
        request("({ one: { two: { three: true } } });"),
        value_limits,
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(mongo_script_error_kind(&value_error), Some(MongoScriptErrorKind::ResourceLimit));

    let memory_limits = MongoScriptLimits { memory_limit_bytes: 2 * 1024 * 1024, ..MongoScriptLimits::default() };
    let memory_error = execute_mongo_script(
        request("new Uint8Array(32 * 1024 * 1024);"),
        memory_limits,
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(mongo_script_error_kind(&memory_error), Some(MongoScriptErrorKind::Runtime));

    let stack_limits = MongoScriptLimits { stack_limit_bytes: 64 * 1024, ..MongoScriptLimits::default() };
    let stack_error = execute_mongo_script(
        request("function recurse() { return recurse(); } recurse();"),
        stack_limits,
        Arc::new(RecordingHost::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(mongo_script_error_kind(&stack_error), Some(MongoScriptErrorKind::Runtime));
}

#[tokio::test]
async fn categorizes_uncaught_host_errors() {
    let error = execute(
        r#"
        db.items.drop();
        "#,
    )
    .await
    .unwrap_err();

    assert_eq!(mongo_script_error_kind(&error), Some(MongoScriptErrorKind::Host));
    assert!(error.contains("expected host failure"));
}

#[tokio::test]
async fn timeout_interrupts_cpu_only_javascript() {
    let limits = MongoScriptLimits { safety_timeout: Duration::from_millis(40), ..MongoScriptLimits::default() };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        execute_mongo_script(
            request("while (true) {}"),
            limits,
            Arc::new(RecordingHost::default()),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("the runtime interrupt handler must return promptly")
    .unwrap_err();

    assert_eq!(mongo_script_error_kind(&result), Some(MongoScriptErrorKind::Timeout));
}

#[tokio::test]
async fn cancellation_releases_a_worker_waiting_for_a_host_call() {
    let started = Arc::new(Notify::new());
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(execute_mongo_script(
        request(
            r#"
            try {
              db.items.findOne({ blocked: true });
            } catch (_) {
              // Cancellation must still win even when user code catches the host exception.
            }
            "caught";
            "#,
        ),
        MongoScriptLimits::default(),
        Arc::new(PendingHost { started: Arc::clone(&started) }),
        cancellation.clone(),
    ));

    started.notified().await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancellation must release the runtime worker")
        .unwrap()
        .unwrap_err();
    assert_eq!(mongo_script_error_kind(&error), Some(MongoScriptErrorKind::Cancelled));
}

#[tokio::test]
async fn executes_the_issue_loop_in_order_and_reports_one_summary() {
    let host = Arc::new(RecordingHost::default());
    let result = execute_mongo_script(
        request(
            r#"
            for(var index=0;index<1000;index++){
              db.large_test.insertOne({_id:index})
            }
            "#,
        ),
        MongoScriptLimits::default(),
        host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(result.final_value, Some(json!({ "ok": 1 })));
    assert_eq!(result.operation_count, 1000);
    assert_eq!(result.succeeded_operation_count, 1000);
    let operations = host.operations();
    assert_eq!(operations.len(), 1000);
    for (index, operation) in operations.iter().enumerate() {
        let MongoScriptOperation::CollectionCall { database, collection, method, args, cursor } = operation else {
            panic!("expected an insertOne collection call");
        };
        assert_eq!(database, "initial_database");
        assert_eq!(collection, "large_test");
        assert_eq!(*method, MongoCollectionMethod::InsertOne);
        assert_eq!(args, &[json!({ "_id": index })]);
        assert!(cursor.is_none());
    }
}

#[tokio::test]
async fn supports_data_dependent_branches_and_database_collection_helpers() {
    let host = Arc::new(RecordingHost::default());
    let result = execute_mongo_script(
        request(
            r#"
            const source = db.getCollection("items").findOne({ enabled: true });
            if (source.enabled) {
              db = db.getSiblingDB("analytics");
              db.getCollection("audit.logs").insertOne({ source: "items", enabled: source.enabled });
            }
            ({ database: db.getName(), source });
            "#,
        ),
        MongoScriptLimits::default(),
        host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(result.current_database, "analytics");
    assert_eq!(result.operation_count, 3);
    assert_eq!(result.succeeded_operation_count, 3);
    assert_eq!(result.final_value, Some(json!({ "database": "analytics", "source": { "enabled": true } })));
    let operations = host.operations();
    assert!(matches!(
        &operations[1],
        MongoScriptOperation::SelectDatabase { database } if database == "analytics"
    ));
    assert!(matches!(
        &operations[2],
        MongoScriptOperation::CollectionCall {
            database,
            collection,
            method: MongoCollectionMethod::InsertOne,
            ..
        } if database == "analytics" && collection == "audit.logs"
    ));
}

#[tokio::test]
async fn materializes_bounded_find_and_aggregate_cursors_once() {
    let find_host = Arc::new(RecordingHost::default());
    let find_result = execute_mongo_script(
        request(
            r#"
            db.items.find({ active: true }, { name: 1 })
              .sort({ name: 1 })
              .collation({ locale: "en", strength: 1 })
              .skip(2)
              .limit(5);
            "#,
        ),
        MongoScriptLimits::default(),
        find_host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        find_result.final_value,
        Some(json!([{ "index": 1, "kind": "first" }, { "index": 2, "kind": "second" }]))
    );
    let operations = find_host.operations();
    let [MongoScriptOperation::CollectionCall { method, args, cursor, .. }] = operations.as_slice() else {
        panic!("expected exactly one find call");
    };
    assert_eq!(*method, MongoCollectionMethod::Find);
    assert_eq!(args, &[json!({ "active": true }), json!({ "name": 1 })]);
    let cursor = cursor.as_deref().expect("find cursor options must be present");
    assert_eq!(cursor.sort, Some(json!({ "name": 1 })));
    assert_eq!(cursor.collation, Some(json!({ "locale": "en", "strength": 1 })));
    assert_eq!(cursor.skip, 2);
    assert_eq!(cursor.limit, 5);

    let aggregate_host = Arc::new(RecordingHost::default());
    let aggregate_result = execute_mongo_script(
        request(
            r#"
            const indexes = [];
            const cursor = db.items.aggregate([{ $match: { active: true } }]);
            cursor.forEach((document) => indexes.push(document.index));
            cursor.toArray();
            indexes;
            "#,
        ),
        MongoScriptLimits::default(),
        aggregate_host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(aggregate_result.final_value, Some(json!([1, 2])));
    assert_eq!(aggregate_result.operation_count, 1);
    assert!(matches!(
        aggregate_host.operations().as_slice(),
        [MongoScriptOperation::CollectionCall { method: MongoCollectionMethod::Aggregate, cursor: None, .. }]
    ));

    let terminal_host = Arc::new(RecordingHost::default());
    execute_mongo_script(
        request(
            r#"
            const count = db.items.find({ active: true }).count();
            const plan = db.items.find({ active: true }).sort({ name: 1 }).explain("executionStats");
            ({ count, plan });
            "#,
        ),
        MongoScriptLimits::default(),
        terminal_host.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let operations = terminal_host.operations();
    assert!(matches!(
        operations.as_slice(),
        [
            MongoScriptOperation::CollectionCall { method: MongoCollectionMethod::Count, cursor: None, .. },
            MongoScriptOperation::CollectionCall { method: MongoCollectionMethod::FindExplain, cursor: Some(_), .. }
        ]
    ));
    let MongoScriptOperation::CollectionCall { cursor: Some(cursor), .. } = &operations[1] else {
        panic!("expected explain cursor options");
    };
    assert_eq!(cursor.sort, Some(json!({ "name": 1 })));
    assert_eq!(cursor.explain_verbosity.as_deref(), Some("executionStats"));
}

#[tokio::test]
async fn reports_partial_progress_and_rejects_unsupported_facade_methods() {
    let partial_error = execute(
        r#"
        db.items.findOne({ _id: 1 });
        db.items.drop();
        "#,
    )
    .await
    .unwrap_err();
    assert_eq!(mongo_script_error_kind(&partial_error), Some(MongoScriptErrorKind::Host));
    assert!(partial_error.contains("1 of 2 attempted operations succeeded"));

    let unsupported_error = execute("db.items.bulkWrite([]);").await.unwrap_err();
    assert_eq!(mongo_script_error_kind(&unsupported_error), Some(MongoScriptErrorKind::Runtime));
    assert!(unsupported_error.contains("Unsupported MongoDB collection method: bulkWrite"));

    let forged_database_error = execute(r#"db = { __dbxDatabaseName: "admin" };"#).await.unwrap_err();
    assert_eq!(mongo_script_error_kind(&forged_database_error), Some(MongoScriptErrorKind::Runtime));
    assert!(forged_database_error.contains("db can only be assigned a DBX MongoDB database handle"));
}
