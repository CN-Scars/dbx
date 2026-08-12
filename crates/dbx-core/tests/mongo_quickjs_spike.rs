#![cfg(feature = "mongo-js-runtime")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use rquickjs::{Context, Error, Function, Promise, Runtime, Value};

const TEST_MEMORY_LIMIT: usize = 8 * 1024 * 1024;
const TEST_STACK_LIMIT: usize = 128 * 1024;
const TEST_COMPLETION_TIMEOUT: Duration = Duration::from_secs(1);

fn runtime_with_limits() -> Runtime {
    let runtime = Runtime::new().unwrap();
    runtime.set_memory_limit(TEST_MEMORY_LIMIT);
    runtime.set_max_stack_size(TEST_STACK_LIMIT);
    runtime
}

#[test]
fn executes_javascript_control_flow_and_calls_a_host_function() {
    let runtime = runtime_with_limits();
    let context = Context::full(&runtime).unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));

    context.with(|context| {
        let observed = Arc::clone(&observed);
        let record = Function::new(context.clone(), move |value: i32| {
            observed.lock().unwrap().push(value);
        })
        .unwrap();
        context.globals().set("record", record).unwrap();

        let total: i32 = context
            .eval(
                r#"
                let total = 0;
                for (let index = 0; index < 5; index += 1) {
                    if (index % 2 === 0) {
                        record(index);
                        total += index;
                    }
                }
                total;
                "#,
            )
            .unwrap();
        assert_eq!(total, 6);
    });

    assert_eq!(*observed.lock().unwrap(), vec![0, 2, 4]);
}

#[test]
fn round_trips_nested_javascript_values_through_serde() {
    let runtime = runtime_with_limits();
    let context = Context::full(&runtime).unwrap();

    let actual = context.with(|context| {
        let value: Value<'_> = context
            .eval(
                r#"({
                    cursor: {
                        firstBatch: [{ id: 1, tags: ["quickjs", "mongodb"] }],
                        exhausted: true
                    }
                })"#,
            )
            .unwrap();
        rquickjs_serde::from_value::<serde_json::Value>(value).unwrap()
    });

    assert_eq!(actual["cursor"]["firstBatch"][0]["id"], 1);
    assert_eq!(actual["cursor"]["firstBatch"][0]["tags"][1], "mongodb");
    assert_eq!(actual["cursor"]["exhausted"], true);
}

#[test]
fn starts_without_node_or_system_access_globals() {
    let runtime = runtime_with_limits();
    let context = Context::full(&runtime).unwrap();

    context.with(|context| {
        let global_types: String = context
            .eval(
                r#"[
                    typeof globalThis.require,
                    typeof globalThis.process,
                    typeof globalThis.fetch,
                    typeof globalThis.Deno,
                    typeof globalThis.Bun
                ].join(",")"#,
            )
            .unwrap();

        assert_eq!(global_types, "undefined,undefined,undefined,undefined,undefined");

        let import_result =
            context.eval::<Promise<'_>, _>(r#"import("dbx-spike-module")"#).and_then(|promise| promise.finish::<()>());
        assert!(import_result.is_err(), "module loading must remain unavailable");
    });
}

#[test]
fn enforces_memory_and_stack_limits() {
    let memory_runtime = runtime_with_limits();
    let memory_context = Context::full(&memory_runtime).unwrap();
    let memory_result = memory_context.with(|context| context.eval::<(), _>("new Uint8Array(64 * 1024 * 1024);"));
    assert!(memory_result.is_err(), "allocation above the runtime memory limit must fail");

    let stack_runtime = runtime_with_limits();
    let stack_context = Context::full(&stack_runtime).unwrap();
    let stack_result = stack_context.with(|context| {
        context.eval::<(), _>(
            r#"
            function recurse() {
                return recurse();
            }
            recurse();
            "#,
        )
    });
    assert!(stack_result.is_err(), "recursion above the runtime stack limit must fail");
}

#[test]
fn operation_budget_stops_excessive_host_calls() {
    const OPERATION_LIMIT: usize = 32;

    let runtime = runtime_with_limits();
    let context = Context::full(&runtime).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));

    let result = context.with(|context| {
        let calls = Arc::clone(&calls);
        let host_call = Function::new(context.clone(), move || -> Result<(), Error> {
            let operation = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if operation > OPERATION_LIMIT {
                return Err(Error::new_from_js_message(
                    "host function",
                    "JavaScript",
                    "MongoDB shell operation budget exceeded",
                ));
            }
            Ok(())
        })
        .unwrap();
        context.globals().set("hostCall", host_call).unwrap();
        context.eval::<(), _>("for (let index = 0; index < 1000; index += 1) hostCall();")
    });

    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), OPERATION_LIMIT + 1);
}

#[test]
fn interrupt_handler_stops_infinite_javascript() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();

    let worker = {
        let cancelled = Arc::clone(&cancelled);
        thread::spawn(move || {
            let runtime = runtime_with_limits();
            runtime.set_interrupt_handler(Some(Box::new(move || cancelled.load(Ordering::SeqCst))));
            let context = Context::full(&runtime).unwrap();
            ready_tx.send(()).unwrap();
            let result = context.with(|context| context.eval::<(), _>("while (true) {}"));
            result_tx.send(result.map_err(|error| error.to_string())).unwrap();
        })
    };

    ready_rx.recv_timeout(TEST_COMPLETION_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(20));
    cancelled.store(true, Ordering::SeqCst);

    let result = result_rx
        .recv_timeout(TEST_COMPLETION_TIMEOUT)
        .expect("the interrupt handler must return control to the host promptly");
    assert!(result.is_err());
    worker.join().unwrap();
}

#[test]
fn cooperative_host_call_cancellation_releases_the_runtime_worker() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let (host_started_tx, host_started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();

    let worker = {
        let cancelled = Arc::clone(&cancelled);
        thread::spawn(move || {
            let runtime = runtime_with_limits();
            let interrupt_cancelled = Arc::clone(&cancelled);
            runtime.set_interrupt_handler(Some(Box::new(move || interrupt_cancelled.load(Ordering::SeqCst))));
            let context = Context::full(&runtime).unwrap();

            let result = context.with(|context| {
                let host_cancelled = Arc::clone(&cancelled);
                let host_call = Function::new(context.clone(), move || -> Result<(), Error> {
                    host_started_tx.send(()).unwrap();
                    while !host_cancelled.load(Ordering::SeqCst) {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(Error::new_from_js_message("host function", "JavaScript", "MongoDB shell execution cancelled"))
                })
                .unwrap();
                context.globals().set("blockingHostCall", host_call).unwrap();
                context.eval::<(), _>("blockingHostCall();")
            });

            result_tx.send(result.map_err(|error| error.to_string())).unwrap();
        })
    };

    host_started_rx.recv_timeout(TEST_COMPLETION_TIMEOUT).unwrap();
    cancelled.store(true, Ordering::SeqCst);

    let result = result_rx
        .recv_timeout(TEST_COMPLETION_TIMEOUT)
        .expect("a cancelled host call must not permanently occupy the runtime worker");
    assert!(result.is_err());
    worker.join().unwrap();
}
