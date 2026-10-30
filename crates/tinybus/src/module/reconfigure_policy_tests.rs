use super::*;

#[tokio::test(start_paused = true)]
async fn retry_delay_is_applied_between_typed_initializing_attempts() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let mut access = fixture().await;
    access.initializing_left = StdMutex::new(1);
    let calls = access.reinitialize_calls.clone();
    let (attempt_tx, attempt_rx) = tokio::sync::oneshot::channel();
    access.initializing_notifier = StdMutex::new(Some(attempt_tx));
    let module = Arc::new(
        ReconfigurableModule::new(access, ID).retry_while_initializing(2, Duration::from_secs(5)),
    );
    let config = serde_json::json!({ "route": "after-retry" });
    let retrying_module = module.clone();
    let retrying_config = config.clone();
    let retrying = tokio::spawn(async move { retrying_module.proxy(&retrying_config).await });

    attempt_rx.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    let proxy = retrying.await.unwrap().unwrap();
    tokio::time::resume();
    assert_eq!(read_config(&proxy).await, config);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn registry_and_enabled_gates_refuse_proxy_reconciliation() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let mut missing = fixture().await;
    missing.registered = false;
    let missing = ReconfigurableModule::new(missing, ID);
    assert_eq!(
        missing.proxy(&serde_json::json!({})).await.unwrap_err(),
        ModuleCallError::Unavailable
    );

    let mut disabled = fixture().await;
    disabled.enabled = false;
    let disabled = ReconfigurableModule::new(disabled, ID);
    assert_eq!(
        disabled.proxy(&serde_json::json!({})).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
}

#[tokio::test]
async fn unavailable_fault_policy_only_marks_when_enabled() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let mut access = fixture().await;
    access.forced_reinitialize_failure = Some(ReconfigureFailure::Unavailable);
    let fault_calls = access.fault_calls.clone();
    let module = ReconfigurableModule::new(access, ID).mark_faulted_on_unavailable(false);
    assert_eq!(
        module.proxy(&serde_json::json!({})).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(fault_calls.load(Ordering::SeqCst), 0);

    let mut access = fixture().await;
    access.forced_reinitialize_failure = Some(ReconfigureFailure::Unavailable);
    let fault_calls = access.fault_calls.clone();
    let module = ReconfigurableModule::new(access, ID).mark_faulted_on_unavailable(true);
    assert_eq!(
        module.proxy(&serde_json::json!({})).await.unwrap_err(),
        ModuleCallError::Unavailable
    );
    assert_eq!(fault_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lifecycle_and_member_proxy_failures_map_to_sanitized_outcomes() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    for (failure, expected) in [
        (ProxyFailure::Transport, ModuleCallError::TransportFailed),
        (
            ProxyFailure::Contract,
            ModuleCallError::IncompatibleContract,
        ),
    ] {
        let mut access = fixture().await;
        access.proxy_failure = Some(failure);
        let lifecycle = ReconfigurableModule::new(access, ID);
        assert_eq!(
            lifecycle
                .proxy(&serde_json::json!({ "route": "lifecycle" }))
                .await
                .unwrap_err(),
            expected
        );
    }

    let mut access = fixture().await;
    access.proxy_failure = Some(ProxyFailure::Contract);
    let member =
        ReconfigurableModule::new(access, ID).with_strategy(Reconfigure::Member("Configure"));
    assert_eq!(
        member
            .proxy(&serde_json::json!({ "route": "member" }))
            .await
            .unwrap_err(),
        ModuleCallError::ModuleFault
    );
}
