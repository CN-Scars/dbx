#![cfg(feature = "mongo-js-runtime")]

use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use dbx_core::mongo_script::{
    execute_mongo_script, mongo_script_error_kind, MongoScriptErrorKind, MongoScriptHost, MongoScriptLimits,
    MongoScriptOperation, MongoScriptOutput, MongoScriptRequest,
};
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
            MongoScriptOperation::DatabaseCall { method, args, .. }
            | MongoScriptOperation::CollectionCall { method, args, .. } => match method.as_str() {
                "echo" => Ok(args.first().cloned().unwrap_or(Value::Null)),
                "extendedJson" => Ok(json!({
                    "id": { "$oid": "507f1f77bcf86cd799439011" },
                    "createdAt": { "$date": "2026-08-12T00:00:00.000Z" },
                    "nested": [{ "owner": { "$oid": "507f191e810c19729de860ea" } }]
                })),
                "fail" => Err("expected host failure".to_string()),
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
              values.push(__dbxHostCall({
                kind: "collectionCall",
                database: "alpha",
                collection: "items",
                method: "echo",
                args: [index],
              }));
            }
            let caught = false;
            try {
              __dbxHostCall({ kind: "databaseCall", database: "alpha", method: "fail", args: [] });
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

    assert_eq!(result.final_value, Some(json!({ "values": [0, 1, 2], "caught": true })));
    assert_eq!(result.operation_count, 4);
    assert_eq!(result.succeeded_operation_count, 3);
    assert_eq!(host.max_active.load(Ordering::SeqCst), 1);
    let operations = host.operations();
    let methods = operations
        .iter()
        .map(|operation| match operation {
            MongoScriptOperation::DatabaseCall { method, .. } | MongoScriptOperation::CollectionCall { method, .. } => {
                method.as_str()
            }
            MongoScriptOperation::SelectDatabase { .. } => "selectDatabase",
        })
        .collect::<Vec<_>>();
    assert_eq!(methods, vec!["echo", "echo", "echo", "fail"]);
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
            const received = __dbxHostCall({
              kind: "databaseCall",
              database: "alpha",
              method: "extendedJson",
              args: [sent],
            });
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
    let MongoScriptOperation::DatabaseCall { args, .. } = operation else {
        panic!("expected a database call");
    };
    assert_eq!(args[0]["id"], json!({ "$oid": "507f1f77bcf86cd799439011" }));
    assert_eq!(args[0]["createdAt"], json!({ "$date": "2026-08-12T00:00:00.000Z" }));

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
        __dbxHostCall({ kind: "selectDatabase", database: "analytics" });
        "selected";
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
              __dbxHostCall({ kind: "databaseCall", database: "alpha", method: "echo", args: [index] });
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
        __dbxHostCall({ kind: "databaseCall", database: "alpha", method: "fail", args: [] });
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
              __dbxHostCall({ kind: "databaseCall", database: "alpha", method: "blocked", args: [] });
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
