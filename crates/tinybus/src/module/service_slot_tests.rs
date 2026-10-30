use super::*;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tokio::sync::oneshot;

#[derive(Debug, PartialEq, Eq)]
struct HostError(&'static str);

fn slot<K, S>() -> ModuleServiceSlot<K, S> {
    ModuleServiceSlot::new()
}

#[tokio::test]
async fn the_same_key_reuses_the_service_and_allows_mutating_operations() {
    let slot = slot::<&'static str, usize>();
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let first_opens = opens.clone();
    let first_closes = closes.clone();
    let first = slot
        .with(
            "alice",
            move || async move {
                first_opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, crate::Error>(4usize)
            },
            move |service| async move {
                first_closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = service;
                Ok(())
            },
            |service| {
                Box::pin(async move {
                    *service += 1;
                    Ok::<_, crate::Error>(*service)
                })
            },
        )
        .await
        .expect("first operation should open and use the service");
    assert_eq!(first, 5);

    let second_opens = opens.clone();
    let second_closes = closes.clone();
    let second = slot
        .with(
            "alice",
            move || async move {
                second_opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, crate::Error>(99usize)
            },
            move |service| async move {
                second_closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = service;
                Ok(())
            },
            |service| {
                Box::pin(async move {
                    *service += 1;
                    Ok::<_, crate::Error>(*service)
                })
            },
        )
        .await
        .expect("matching key should reuse the installed service");

    assert_eq!(second, 6);
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(closes.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn changing_the_key_closes_the_installed_service_before_opening_its_replacement() {
    let slot = slot::<&'static str, &'static str>();
    let events = Arc::new(Mutex::new(Vec::new()));

    let opened_events = events.clone();
    let closed_events = events.clone();
    slot.with(
        "alice",
        move || async move {
            opened_events.lock().unwrap().push("open alice");
            Ok::<_, crate::Error>("alice service")
        },
        move |service| async move {
            let _ = service;
            closed_events.lock().unwrap().push("close alice");
            Err(crate::Error::failed("old service close failed"))
        },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("initial service should open");

    let opened_events = events.clone();
    slot.with(
        "bob",
        move || async move {
            opened_events.lock().unwrap().push("open bob");
            Ok::<_, crate::Error>("bob service")
        },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("replacement open should determine the result after a close error");

    assert_eq!(
        *events.lock().unwrap(),
        ["open alice", "close alice", "open bob"]
    );
}

#[tokio::test]
async fn explicit_close_returns_its_error_and_clears_the_slot() {
    let slot = slot::<u8, u8>();
    slot.with(
        1,
        || async { Ok::<_, crate::Error>(7) },
        |_| async { Err(crate::Error::failed("close failed")) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should install");

    assert!(slot.close().await.is_err());
    assert!(
        slot.close().await.is_ok(),
        "an empty slot closes idempotently"
    );

    let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = opened.clone();
    slot.with(
        1,
        move || async move {
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, crate::Error>(8)
        },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should reopen after failed explicit close");
    assert!(opened.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn a_failed_open_leaves_the_slot_empty() {
    let slot = slot::<u8, u8>();
    assert!(
        slot.with(
            1,
            || async { Err(crate::Error::failed("open failed")) },
            |_| async { Ok(()) },
            |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
        )
        .await
        .is_err()
    );

    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = opens.clone();
    slot.with(
        1,
        move || async move {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, crate::Error>(2)
        },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("a later open should succeed");
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_partial_eq_key_and_host_error_type_need_no_tinybus_policy() {
    let slot = ModuleServiceSlot::<f64, u8, HostError>::new();
    let result = slot
        .with(
            1.25,
            || async { Ok::<_, HostError>(7) },
            |_| async { Err(HostError("host close error")) },
            |service| {
                Box::pin(async move {
                    *service += 1;
                    Ok::<_, HostError>(*service)
                })
            },
        )
        .await
        .expect("the host error type should flow through the generic slot");

    assert_eq!(result, 8);
    assert_eq!(slot.close().await, Err(HostError("host close error")));
    assert_eq!(slot.close().await, Ok(()));
}

#[tokio::test]
async fn close_waits_for_the_current_operation_to_finish() {
    let slot = Arc::new(slot::<u8, u8>());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let with_slot = slot.clone();
    let operation = tokio::spawn(async move {
        with_slot
            .with(
                1,
                || async { Ok::<_, crate::Error>(7) },
                |_| async { Ok(()) },
                move |_| {
                    Box::pin(async move {
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<_, crate::Error>(())
                    })
                },
            )
            .await
    });
    entered_rx
        .await
        .expect("operation should enter before close");

    let closing_slot = slot.clone();
    let mut close = Box::pin(closing_slot.close());
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(close.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "close must wait for the locked operation"
    );

    let _ = release_tx.send(());
    operation
        .await
        .expect("operation task should finish")
        .expect("operation should succeed");
    close.await.expect("close should succeed");
}

#[tokio::test]
async fn shutdown_registration_is_idempotent_and_can_be_repeated_after_cleanup_error() {
    let slot = Box::leak(Box::new(slot::<u8, u8>()));
    slot.with(
        1,
        || async { Ok::<_, crate::Error>(7) },
        |_| async { Err(crate::Error::failed("shutdown close failed")) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should install");

    let hooks = Arc::new(Mutex::new(Vec::<ShutdownHook>::new()));
    let registered = hooks.clone();
    slot.on_shutdown(move |hook| registered.lock().unwrap().push(hook));
    let registered = hooks.clone();
    slot.on_shutdown(move |hook| registered.lock().unwrap().push(hook));
    assert_eq!(
        hooks.lock().unwrap().len(),
        1,
        "registration should be idempotent"
    );

    let hook = hooks.lock().unwrap().remove(0);
    hook().await;

    let registered = hooks.clone();
    slot.on_shutdown(move |hook| registered.lock().unwrap().push(hook));
    assert_eq!(
        hooks.lock().unwrap().len(),
        1,
        "cleanup error must reset registration"
    );
}

#[tokio::test]
async fn try_close_with_keeps_the_same_service_and_mutable_cache_after_failure() {
    let slot = slot::<&'static str, usize>();
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stored_closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let opened = opens.clone();
    let stored = stored_closes.clone();
    slot.with(
        "alice",
        move || async move {
            opened.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, crate::Error>(3)
        },
        move |_| async move {
            stored.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        |service| {
            Box::pin(async move {
                *service += 4;
                Ok::<_, crate::Error>(*service)
            })
        },
    )
    .await
    .expect("service should install and update its cache");

    let failed = slot
        .try_close_with(|_| Box::pin(async { Err(crate::Error::failed("close not acknowledged")) }))
        .await;
    assert!(failed.is_err(), "the host close error should be returned");

    let opened = opens.clone();
    let value = slot
        .with(
            "alice",
            move || async move {
                opened.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, crate::Error>(99)
            },
            |_| async { Ok(()) },
            |service| Box::pin(async move { Ok::<_, crate::Error>(*service) }),
        )
        .await
        .expect("matching key should reuse the retained service");
    assert_eq!(value, 7, "the service-scoped mutable cache should survive");
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);

    slot.try_close_with(|_| Box::pin(async { Ok(()) }))
        .await
        .expect("a later acknowledged close should retire the service");
    assert_eq!(
        stored_closes.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the stored closer must not run after the host callback already closed it"
    );
    slot.close()
        .await
        .expect("the acknowledged close should leave an empty slot");
    assert_eq!(
        stored_closes.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "closing the empty slot must not invoke the stored closer"
    );
}

#[tokio::test]
async fn try_close_with_on_an_empty_slot_does_not_call_the_host_callback() {
    let slot = slot::<u8, u8>();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let called = calls.clone();

    slot.try_close_with(move |_| {
        called.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    })
    .await
    .expect("closing an empty slot should succeed");

    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_try_close_with_retains_the_entry_and_original_stored_closer() {
    let slot = Arc::new(slot::<&'static str, &'static str>());
    let events = Arc::new(Mutex::new(Vec::new()));
    let stored_events = events.clone();
    slot.with(
        "alice",
        || async { Ok::<_, crate::Error>("alice service") },
        move |_| async move {
            stored_events.lock().unwrap().push("stored close alice");
            Ok(())
        },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should install");

    let (started_tx, started_rx) = oneshot::channel();
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let closing_slot = slot.clone();
    let close = tokio::spawn(async move {
        closing_slot
            .try_close_with(move |_| {
                let _ = started_tx.send(());
                Box::pin(async move {
                    let _ = cancel_rx.await;
                    Ok(())
                })
            })
            .await
    });
    started_rx.await.expect("host close callback should start");
    close.abort();
    assert!(
        close
            .await
            .expect_err("cancelled close task should stop")
            .is_cancelled()
    );
    drop(cancel_tx);

    let observed = slot
        .with(
            "alice",
            || async { panic!("same-key access must not reopen the service") },
            |_| async { Ok(()) },
            |service| Box::pin(async move { Ok::<_, crate::Error>(*service) }),
        )
        .await
        .expect("cancellation should release the lock and retain the entry");
    assert_eq!(observed, "alice service");

    slot.with(
        "bob",
        || async { Ok::<_, crate::Error>("bob service") },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("a later replacement should proceed");
    assert_eq!(
        *events.lock().unwrap(),
        ["stored close alice"],
        "cancellation must preserve the original closer for subsequent replacement"
    );
}

#[tokio::test]
async fn try_close_with_waits_for_the_current_operation_to_finish() {
    let slot = Arc::new(slot::<u8, u8>());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let operation_slot = slot.clone();
    let operation = tokio::spawn(async move {
        operation_slot
            .with(
                1,
                || async { Ok::<_, crate::Error>(7) },
                |_| async { Ok(()) },
                move |_| {
                    Box::pin(async move {
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<_, crate::Error>(())
                    })
                },
            )
            .await
    });
    entered_rx
        .await
        .expect("operation should enter before explicit close");

    let closing_slot = slot.clone();
    let close_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = close_calls.clone();
    let mut close = Box::pin(closing_slot.try_close_with(move |_| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }));
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(close.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "explicit close must wait for the locked operation"
    );
    assert_eq!(
        close_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the host close callback must wait for the current operation"
    );

    let _ = release_tx.send(());
    operation
        .await
        .expect("operation task should finish")
        .expect("operation should succeed");
    close.await.expect("explicit close should succeed");
    assert_eq!(close_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
