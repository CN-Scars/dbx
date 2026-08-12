use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rquickjs::{CatchResultExt, Context, Ctx, Exception, Function, Runtime, Value as JsValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_STACK_LIMIT_BYTES: usize = 512 * 1024;
const DEFAULT_MAX_OPERATIONS: usize = 10_000;
const DEFAULT_MAX_OUTPUT_ITEMS: usize = 1_000;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_VALUE_DEPTH: usize = 64;
const DEFAULT_MAX_VALUE_NODES: usize = 100_000;
const DEFAULT_MAX_VALUE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_SAFETY_TIMEOUT: Duration = Duration::from_secs(30);

const RAW_HOST_CALL_GLOBAL: &str = "__dbxRawHostCall";
const OUTPUT_GLOBAL: &str = "__dbxCaptureOutput";

const RUNTIME_BOOTSTRAP: &str = r#"
(() => {
  function ObjectId(value) {
    if (!(this instanceof ObjectId)) return new ObjectId(value);
    if (typeof value !== "string" || !/^[0-9a-fA-F]{24}$/.test(value)) {
      throw new TypeError("ObjectId requires a 24-character hexadecimal string");
    }
    Object.defineProperty(this, "$oid", {
      value: value.toLowerCase(),
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
  ObjectId.prototype.toString = function () {
    return `ObjectId("${this.$oid}")`;
  };
  ObjectId.prototype.valueOf = function () { return this.$oid; };

  function ISODate(value) {
    if (!(this instanceof ISODate)) return new ISODate(value);
    const date = value === undefined ? new Date() : new Date(value);
    if (Number.isNaN(date.valueOf())) {
      throw new TypeError("ISODate requires a valid date value");
    }
    Object.defineProperty(this, "$date", {
      value: date.toISOString(),
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
  ISODate.prototype.toISOString = function () { return this.$date; };
  ISODate.prototype.toString = function () {
    return `ISODate("${this.$date}")`;
  };
  ISODate.prototype.valueOf = function () { return this.$date; };

  function reviveExtendedJson(value) {
    if (value === null || typeof value !== "object") return value;
    if (Array.isArray(value)) return value.map(reviveExtendedJson);

    const keys = Object.keys(value);
    if (keys.length === 1 && typeof value.$oid === "string") {
      return ObjectId(value.$oid);
    }
    if (keys.length === 1 && Object.prototype.hasOwnProperty.call(value, "$date")) {
      if (typeof value.$date === "string") return ISODate(value.$date);
      if (value.$date && typeof value.$date.$numberLong === "string") {
        return ISODate(Number(value.$date.$numberLong));
      }
    }
    for (const key of keys) value[key] = reviveExtendedJson(value[key]);
    return value;
  }

  function printable(value) {
    if (typeof value === "string") return value;
    if (value === undefined) return "undefined";
    try {
      const encoded = JSON.stringify(value);
      return encoded === undefined ? String(value) : encoded;
    } catch (_) {
      return String(value);
    }
  }

  Object.defineProperties(globalThis, {
    ObjectId: { value: ObjectId, writable: false, configurable: false },
    ISODate: { value: ISODate, writable: false, configurable: false },
    __dbxReviveExtendedJson: { value: reviveExtendedJson, writable: false, configurable: false },
    __dbxHostCall: {
      value: (operation) => reviveExtendedJson(JSON.parse(__dbxRawHostCall(operation))),
      writable: false,
      configurable: false,
    },
    print: {
      value: (...values) => __dbxCaptureOutput({ kind: "text", value: values.map(printable).join(" ") }),
      writable: false,
      configurable: false,
    },
    printjson: {
      value: (value) => __dbxCaptureOutput({ kind: "json", value }),
      writable: false,
      configurable: false,
    },
  });
})();
"#;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MongoScriptRequest {
    pub connection_id: String,
    pub database: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    pub max_rows: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum MongoScriptOperation {
    SelectDatabase {
        database: String,
    },
    DatabaseCall {
        database: String,
        method: String,
        #[serde(default)]
        args: Vec<Value>,
    },
    CollectionCall {
        database: String,
        collection: String,
        method: String,
        #[serde(default)]
        args: Vec<Value>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum MongoScriptOutput {
    Text(String),
    Json(Value),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MongoScriptResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_value: Option<Value>,
    pub output: Vec<MongoScriptOutput>,
    pub operation_count: usize,
    pub succeeded_operation_count: usize,
    pub current_database: String,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MongoScriptErrorKind {
    Cancelled,
    Host,
    InvalidRequest,
    ResourceLimit,
    Runtime,
    Serialization,
    Timeout,
}

impl MongoScriptErrorKind {
    pub fn code(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Host => "host",
            Self::InvalidRequest => "invalid_request",
            Self::ResourceLimit => "resource_limit",
            Self::Runtime => "runtime",
            Self::Serialization => "serialization",
            Self::Timeout => "timeout",
        }
    }
}

pub fn mongo_script_error_kind(error: &str) -> Option<MongoScriptErrorKind> {
    let code = error.strip_prefix("[mongo_script.")?.split_once(']')?.0;
    match code {
        "cancelled" => Some(MongoScriptErrorKind::Cancelled),
        "host" => Some(MongoScriptErrorKind::Host),
        "invalid_request" => Some(MongoScriptErrorKind::InvalidRequest),
        "resource_limit" => Some(MongoScriptErrorKind::ResourceLimit),
        "runtime" => Some(MongoScriptErrorKind::Runtime),
        "serialization" => Some(MongoScriptErrorKind::Serialization),
        "timeout" => Some(MongoScriptErrorKind::Timeout),
        _ => None,
    }
}

fn script_error(kind: MongoScriptErrorKind, message: impl AsRef<str>) -> String {
    format!("[mongo_script.{}] {}", kind.code(), message.as_ref())
}

#[derive(Clone, Debug)]
pub struct MongoScriptLimits {
    pub memory_limit_bytes: usize,
    pub stack_limit_bytes: usize,
    pub max_operations: usize,
    pub max_output_items: usize,
    pub max_output_bytes: usize,
    pub max_value_depth: usize,
    pub max_value_nodes: usize,
    pub max_value_bytes: usize,
    pub safety_timeout: Duration,
}

impl Default for MongoScriptLimits {
    fn default() -> Self {
        Self {
            memory_limit_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
            stack_limit_bytes: DEFAULT_STACK_LIMIT_BYTES,
            max_operations: DEFAULT_MAX_OPERATIONS,
            max_output_items: DEFAULT_MAX_OUTPUT_ITEMS,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            max_value_depth: DEFAULT_MAX_VALUE_DEPTH,
            max_value_nodes: DEFAULT_MAX_VALUE_NODES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            safety_timeout: DEFAULT_SAFETY_TIMEOUT,
        }
    }
}

impl MongoScriptLimits {
    fn validate(&self) -> Result<(), String> {
        let values = [
            ("memory_limit_bytes", self.memory_limit_bytes),
            ("stack_limit_bytes", self.stack_limit_bytes),
            ("max_operations", self.max_operations),
            ("max_output_items", self.max_output_items),
            ("max_output_bytes", self.max_output_bytes),
            ("max_value_depth", self.max_value_depth),
            ("max_value_nodes", self.max_value_nodes),
            ("max_value_bytes", self.max_value_bytes),
        ];
        if let Some((name, _)) = values.into_iter().find(|(_, value)| *value == 0) {
            return Err(script_error(
                MongoScriptErrorKind::InvalidRequest,
                format!("{name} must be greater than zero"),
            ));
        }
        if self.safety_timeout.is_zero() {
            return Err(script_error(MongoScriptErrorKind::InvalidRequest, "safety_timeout must be greater than zero"));
        }
        Ok(())
    }
}

#[async_trait]
pub trait MongoScriptHost: Send + Sync {
    async fn execute(&self, operation: MongoScriptOperation) -> Result<Value, String>;
}

struct HostRequest {
    operation: MongoScriptOperation,
    reply: oneshot::Sender<Result<Value, String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum InterruptReason {
    Running = 0,
    Cancelled = 1,
    TimedOut = 2,
}

struct ExecutionState {
    interrupt_reason: AtomicU8,
    operation_count: AtomicUsize,
    succeeded_operation_count: AtomicUsize,
}

impl ExecutionState {
    fn new() -> Self {
        Self {
            interrupt_reason: AtomicU8::new(InterruptReason::Running as u8),
            operation_count: AtomicUsize::new(0),
            succeeded_operation_count: AtomicUsize::new(0),
        }
    }

    fn interrupt(&self, reason: InterruptReason) {
        let _ = self.interrupt_reason.compare_exchange(
            InterruptReason::Running as u8,
            reason as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn reason(&self) -> InterruptReason {
        match self.interrupt_reason.load(Ordering::SeqCst) {
            value if value == InterruptReason::Cancelled as u8 => InterruptReason::Cancelled,
            value if value == InterruptReason::TimedOut as u8 => InterruptReason::TimedOut,
            _ => InterruptReason::Running,
        }
    }

    fn try_start_operation(&self, limit: usize) -> Result<(), String> {
        self.operation_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| (count < limit).then_some(count + 1))
            .map(|_| ())
            .map_err(|_| {
                script_error(
                    MongoScriptErrorKind::ResourceLimit,
                    format!("MongoDB shell operation limit of {limit} exceeded"),
                )
            })
    }
}

#[derive(Default)]
struct OutputState {
    items: Vec<MongoScriptOutput>,
    bytes: usize,
    truncated: bool,
}

impl OutputState {
    fn capture(&mut self, output: MongoScriptOutput, limits: &MongoScriptLimits) -> Result<(), String> {
        let output_value = serde_json::to_value(&output)
            .map_err(|error| script_error(MongoScriptErrorKind::Serialization, error.to_string()))?;
        validate_json_shape(&output_value, limits)?;
        let bytes = serde_json::to_vec(&output)
            .map_err(|error| script_error(MongoScriptErrorKind::Serialization, error.to_string()))?
            .len();
        if self.items.len() >= limits.max_output_items || self.bytes.saturating_add(bytes) > limits.max_output_bytes {
            self.truncated = true;
            return Ok(());
        }
        self.bytes += bytes;
        self.items.push(output);
        Ok(())
    }
}

struct WorkerResult {
    final_value: Option<Value>,
}

struct InterruptOnDrop(Arc<ExecutionState>);

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        self.0.interrupt(InterruptReason::Cancelled);
    }
}

pub async fn execute_mongo_script(
    request: MongoScriptRequest,
    limits: MongoScriptLimits,
    host: Arc<dyn MongoScriptHost>,
    cancellation: CancellationToken,
) -> Result<MongoScriptResult, String> {
    validate_request(&request, &limits)?;

    let timeout = request
        .timeout_secs
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .map(|requested| requested.min(limits.safety_timeout))
        .unwrap_or(limits.safety_timeout);
    let deadline = Instant::now() + timeout;
    let state = Arc::new(ExecutionState::new());
    let _interrupt_on_drop = InterruptOnDrop(Arc::clone(&state));
    let output = Arc::new(Mutex::new(OutputState::default()));
    let current_database = Arc::new(Mutex::new(request.database.clone()));
    let (operation_tx, mut operation_rx) = mpsc::channel::<HostRequest>(1);

    let mut worker =
        spawn_runtime_worker(request.source, limits.clone(), Arc::clone(&state), Arc::clone(&output), operation_tx);
    let mut operation_channel_open = true;

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                state.interrupt(InterruptReason::Cancelled);
                drop(operation_rx);
                let _ = worker.await;
                return Err(interrupt_error(InterruptReason::Cancelled));
            }
            _ = tokio::time::sleep_until(deadline) => {
                state.interrupt(InterruptReason::TimedOut);
                drop(operation_rx);
                let _ = worker.await;
                return Err(interrupt_error(InterruptReason::TimedOut));
            }
            worker_result = &mut worker => {
                return finish_worker(
                    worker_result,
                    &state,
                    &output,
                    &current_database,
                );
            }
            host_request = operation_rx.recv(), if operation_channel_open => {
                let Some(host_request) = host_request else {
                    operation_channel_open = false;
                    continue;
                };
                let operation = host_request.operation.clone();
                let mut interrupted = None;
                let host_result = tokio::select! {
                    result = host.execute(host_request.operation) => result.and_then(|value| {
                        validate_json_value(&value, &limits)?;
                        Ok(value)
                    }),
                    _ = cancellation.cancelled() => {
                        state.interrupt(InterruptReason::Cancelled);
                        interrupted = Some(InterruptReason::Cancelled);
                        Err(interrupt_error(InterruptReason::Cancelled))
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        state.interrupt(InterruptReason::TimedOut);
                        interrupted = Some(InterruptReason::TimedOut);
                        Err(interrupt_error(InterruptReason::TimedOut))
                    }
                };

                if host_result.is_ok() {
                    state.succeeded_operation_count.fetch_add(1, Ordering::SeqCst);
                    if let MongoScriptOperation::SelectDatabase { database } = operation {
                        let mut current = current_database.lock().map_err(|_| {
                            script_error(MongoScriptErrorKind::Runtime, "MongoDB script database state is unavailable")
                        })?;
                        *current = database;
                    }
                }
                let _ = host_request.reply.send(host_result);

                if let Some(reason) = interrupted {
                    drop(operation_rx);
                    let _ = worker.await;
                    return Err(interrupt_error(reason));
                }
            }
        }
    }
}

fn validate_request(request: &MongoScriptRequest, limits: &MongoScriptLimits) -> Result<(), String> {
    limits.validate()?;
    if request.connection_id.trim().is_empty() {
        return Err(script_error(MongoScriptErrorKind::InvalidRequest, "connection_id must not be empty"));
    }
    if request.database.trim().is_empty() {
        return Err(script_error(MongoScriptErrorKind::InvalidRequest, "database must not be empty"));
    }
    if request.source.trim().is_empty() {
        return Err(script_error(MongoScriptErrorKind::InvalidRequest, "source must not be empty"));
    }
    if request.max_rows == 0 {
        return Err(script_error(MongoScriptErrorKind::InvalidRequest, "max_rows must be greater than zero"));
    }
    Ok(())
}

fn spawn_runtime_worker(
    source: String,
    limits: MongoScriptLimits,
    state: Arc<ExecutionState>,
    output: Arc<Mutex<OutputState>>,
    operation_tx: mpsc::Sender<HostRequest>,
) -> JoinHandle<Result<WorkerResult, String>> {
    tokio::task::spawn_blocking(move || run_runtime(source, limits, state, output, operation_tx))
}

fn run_runtime(
    source: String,
    limits: MongoScriptLimits,
    state: Arc<ExecutionState>,
    output: Arc<Mutex<OutputState>>,
    operation_tx: mpsc::Sender<HostRequest>,
) -> Result<WorkerResult, String> {
    let runtime = Runtime::new().map_err(|error| {
        script_error(MongoScriptErrorKind::Runtime, format!("Could not create JavaScript runtime: {error}"))
    })?;
    runtime.set_memory_limit(limits.memory_limit_bytes);
    runtime.set_max_stack_size(limits.stack_limit_bytes);
    let interrupt_state = Arc::clone(&state);
    runtime.set_interrupt_handler(Some(Box::new(move || interrupt_state.reason() != InterruptReason::Running)));

    let context = Context::full(&runtime).map_err(|error| {
        script_error(MongoScriptErrorKind::Runtime, format!("Could not create JavaScript context: {error}"))
    })?;
    context.with(|context| {
        install_host_call(context.clone(), Arc::clone(&state), &limits, operation_tx)?;
        install_output_capture(context.clone(), Arc::clone(&output), &limits)?;
        context
            .eval::<(), _>(RUNTIME_BOOTSTRAP)
            .catch(&context)
            .map_err(|error| script_error_from_js(error.to_string()))?;

        let value = context
            .eval::<JsValue<'_>, _>(source)
            .catch(&context)
            .map_err(|error| script_error_from_js(error.to_string()))?;
        let value = if let Some(promise) = value.as_promise() {
            promise.finish::<JsValue<'_>>().catch(&context).map_err(|error| script_error_from_js(error.to_string()))?
        } else {
            value
        };
        let final_value = if value.is_undefined() {
            None
        } else {
            let value = rquickjs_serde::from_value::<Value>(value)
                .map_err(|error| script_error(MongoScriptErrorKind::Serialization, error.to_string()))?;
            validate_json_value(&value, &limits)?;
            Some(value)
        };
        Ok(WorkerResult { final_value })
    })
}

fn install_host_call(
    context: Ctx<'_>,
    state: Arc<ExecutionState>,
    limits: &MongoScriptLimits,
    operation_tx: mpsc::Sender<HostRequest>,
) -> Result<(), String> {
    let limits = limits.clone();
    let host_call = Function::new(context.clone(), move |context: Ctx<'_>, value: JsValue<'_>| {
        let operation = rquickjs_serde::from_value::<MongoScriptOperation>(value)
            .map_err(|error| Exception::throw_type(&context, &format!("Invalid MongoDB host operation: {error}")))?;
        let operation_value = serde_json::to_value(&operation).map_err(|error| {
            Exception::throw_internal(&context, &format!("Could not serialize host operation: {error}"))
        })?;
        validate_json_value(&operation_value, &limits).map_err(|error| Exception::throw_range(&context, &error))?;
        state.try_start_operation(limits.max_operations).map_err(|error| Exception::throw_range(&context, &error))?;

        let (reply, response) = oneshot::channel();
        operation_tx
            .blocking_send(HostRequest { operation, reply })
            .map_err(|_| Exception::throw_internal(&context, "MongoDB script host coordinator is unavailable"))?;
        let result = response
            .blocking_recv()
            .map_err(|_| Exception::throw_internal(&context, "MongoDB script host response channel closed"))?
            .map_err(|error| Exception::throw_message(&context, &script_error(MongoScriptErrorKind::Host, error)))?;
        serde_json::to_string(&result).map_err(|error| {
            Exception::throw_internal(&context, &format!("Could not encode MongoDB host result: {error}"))
        })
    })
    .map_err(|error| script_error(MongoScriptErrorKind::Runtime, error.to_string()))?;
    context
        .globals()
        .set(RAW_HOST_CALL_GLOBAL, host_call)
        .map_err(|error| script_error(MongoScriptErrorKind::Runtime, error.to_string()))
}

fn install_output_capture(
    context: Ctx<'_>,
    output: Arc<Mutex<OutputState>>,
    limits: &MongoScriptLimits,
) -> Result<(), String> {
    let limits = limits.clone();
    let capture = Function::new(context.clone(), move |context: Ctx<'_>, value: JsValue<'_>| {
        let output_item = rquickjs_serde::from_value::<MongoScriptOutput>(value)
            .map_err(|error| Exception::throw_type(&context, &format!("Invalid script output: {error}")))?;
        output
            .lock()
            .map_err(|_| Exception::throw_internal(&context, "MongoDB script output state is unavailable"))?
            .capture(output_item, &limits)
            .map_err(|error| Exception::throw_range(&context, &error))
    })
    .map_err(|error| script_error(MongoScriptErrorKind::Runtime, error.to_string()))?;
    context
        .globals()
        .set(OUTPUT_GLOBAL, capture)
        .map_err(|error| script_error(MongoScriptErrorKind::Runtime, error.to_string()))
}

fn finish_worker(
    worker_result: Result<Result<WorkerResult, String>, tokio::task::JoinError>,
    state: &ExecutionState,
    output: &Mutex<OutputState>,
    current_database: &Mutex<String>,
) -> Result<MongoScriptResult, String> {
    let reason = state.reason();
    if reason != InterruptReason::Running {
        return Err(interrupt_error(reason));
    }
    let worker_result = worker_result.map_err(|error| {
        script_error(MongoScriptErrorKind::Runtime, format!("JavaScript runtime worker failed: {error}"))
    })??;
    let output = output
        .lock()
        .map_err(|_| script_error(MongoScriptErrorKind::Runtime, "MongoDB script output state is unavailable"))?;
    let current_database = current_database
        .lock()
        .map_err(|_| script_error(MongoScriptErrorKind::Runtime, "MongoDB script database state is unavailable"))?;
    Ok(MongoScriptResult {
        final_value: worker_result.final_value,
        output: output.items.clone(),
        operation_count: state.operation_count.load(Ordering::SeqCst),
        succeeded_operation_count: state.succeeded_operation_count.load(Ordering::SeqCst),
        current_database: current_database.clone(),
        truncated: output.truncated,
    })
}

fn interrupt_error(reason: InterruptReason) -> String {
    match reason {
        InterruptReason::Cancelled => {
            script_error(MongoScriptErrorKind::Cancelled, "MongoDB shell execution cancelled")
        }
        InterruptReason::TimedOut => script_error(MongoScriptErrorKind::Timeout, "MongoDB shell execution timed out"),
        InterruptReason::Running => script_error(MongoScriptErrorKind::Runtime, "MongoDB shell execution interrupted"),
    }
}

fn script_error_from_js(message: String) -> String {
    for kind in [MongoScriptErrorKind::Host, MongoScriptErrorKind::ResourceLimit] {
        let marker = format!("[mongo_script.{}]", kind.code());
        if let Some(index) = message.find(&marker) {
            return message[index..].to_string();
        }
    }
    script_error(MongoScriptErrorKind::Runtime, message)
}

fn validate_json_shape(value: &Value, limits: &MongoScriptLimits) -> Result<(), String> {
    let mut stack = vec![(value, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > limits.max_value_nodes {
            return Err(script_error(
                MongoScriptErrorKind::ResourceLimit,
                format!("MongoDB script value node limit of {} exceeded", limits.max_value_nodes),
            ));
        }
        if depth > limits.max_value_depth {
            return Err(script_error(
                MongoScriptErrorKind::ResourceLimit,
                format!("MongoDB script value depth limit of {} exceeded", limits.max_value_depth),
            ));
        }
        match value {
            Value::Array(values) => stack.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => stack.extend(values.values().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

fn validate_json_value(value: &Value, limits: &MongoScriptLimits) -> Result<(), String> {
    validate_json_shape(value, limits)?;
    let bytes = serde_json::to_vec(value)
        .map_err(|error| script_error(MongoScriptErrorKind::Serialization, error.to_string()))?
        .len();
    if bytes > limits.max_value_bytes {
        return Err(script_error(
            MongoScriptErrorKind::ResourceLimit,
            format!("MongoDB script value size limit of {} bytes exceeded", limits.max_value_bytes),
        ));
    }
    Ok(())
}
