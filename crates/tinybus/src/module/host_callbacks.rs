//! Local, exact-instance confidential callbacks; never artifact attestation.

use super::*;
use crate::message::Header;
use crate::module::transport::AdmissionTermination;
use crate::transport::memory::MemoryTransport;
use crate::{
    CallContext, Connection, Interface, InterfaceName, MemberName, ObjectPath, StreamDescriptor,
    StreamReader, StreamRef, StreamWriter,
};
use async_trait::async_trait;
use tokio::sync::Notify;

/// An exact callback interface and its finite member allowlist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostCallbackEndpoint {
    /// Fixed object path; no subtree matching.
    pub path: ObjectPath,
    /// Fixed interface; no alias matching.
    pub interface: InterfaceName,
    /// Permitted methods, between one and 64 distinct names.
    pub members: Vec<MemberName>,
}

/// A finite set of ordinary (non-confidential) stream-carrying methods for
/// one exact host callback interface.
///
/// Stream bytes use ordinary TinyBus `Write` calls, so these members cannot
/// be confidential callbacks. Their authority is restricted to the exact
/// loaded module and callback instance that declared them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostStreamCallbackEndpoint {
    /// Fixed object path; no subtree matching.
    pub path: ObjectPath,
    /// Fixed interface; no alias matching.
    pub interface: InterfaceName,
    /// Permitted stream-carrying methods, between one and 64 distinct names.
    pub members: Vec<MemberName>,
}

/// A finite set of recipient-directed signals a host callback may emit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostCallbackSignal {
    /// Fixed object path; it must match a declared callback endpoint.
    pub path: ObjectPath,
    /// Fixed interface; it must match a declared callback endpoint.
    pub interface: InterfaceName,
    /// Permitted signal members, between one and 64 distinct names.
    pub members: Vec<MemberName>,
}

struct Lifetime {
    active: bool,
    used: bool,
    revoked: bool,
    module: Option<BusName>,
    running: usize,
    served: Vec<(ObjectPath, InterfaceName)>,
    registering: Vec<(ObjectPath, InterfaceName)>,
}

pub(crate) struct CallbackState {
    pub(crate) destination: BusName,
    endpoints: Vec<HostCallbackEndpoint>,
    stream_endpoints: Vec<HostStreamCallbackEndpoint>,
    signals: Vec<HostCallbackSignal>,
    module_transport: Mutex<Option<std::sync::Weak<ModuleTransport>>>,
    // A descendant callback future keeps its state alive, so retaining its parent
    // chain lets root drain still find it after an intermediate binding is dropped.
    // The reverse child links stay weak, avoiding an ownership cycle.
    _parent: Option<Arc<CallbackState>>,
    children: Mutex<Vec<std::sync::Weak<CallbackState>>>,
    lifetime: Mutex<Lifetime>,
    drained: Notify,
    physical_drained: std::sync::Condvar,
    #[cfg(test)]
    pub(crate) physical_wait_started: Notify,
}

impl CallbackState {
    fn required_endpoint_count(&self) -> usize {
        let mut interfaces = Vec::new();
        for (path, interface) in self
            .endpoints
            .iter()
            .map(|endpoint| (&endpoint.path, &endpoint.interface))
            .chain(
                self.stream_endpoints
                    .iter()
                    .map(|endpoint| (&endpoint.path, &endpoint.interface)),
            )
        {
            if !interfaces.iter().any(|(known_path, known_interface)| {
                *known_path == path && *known_interface == interface
            }) {
                interfaces.push((path, interface));
            }
        }
        interfaces.len()
    }

    /// Physical cleanup waits offruntime, without a watchdog or native exclusion.
    pub(crate) fn wait_drained(&self) {
        let mut state = self
            .lifetime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        state.revoked = true;
        #[cfg(test)]
        self.physical_wait_started.notify_one();
        while state.running != 0 {
            state = self
                .physical_drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);
        for child in self.live_children() {
            child.wait_drained();
        }
    }

    pub(crate) async fn drain(self: &Arc<Self>, deadline: Duration) -> Result<()> {
        tokio::time::timeout(deadline, async {
            HostCallbackGuard(self.clone()).drained().await;
            for child in self.live_descendants() {
                HostCallbackGuard(child).drained().await;
            }
        })
        .await
        .map_err(|_| Error::failed("module callback work exceeded lifecycle deadline"))
    }
    fn live_children(&self) -> Vec<Arc<CallbackState>> {
        let mut children = self.children.lock().expect("callback children lock");
        let live = children
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect();
        children.retain(|child| child.strong_count() != 0);
        live
    }
    fn live_descendants(&self) -> Vec<Arc<CallbackState>> {
        let mut pending = self.live_children();
        let mut descendants = Vec::new();
        while let Some(child) = pending.pop() {
            pending.extend(child.live_children());
            descendants.push(child);
        }
        descendants
    }
    fn module_live(&self) -> bool {
        self.module_transport
            .lock()
            .expect("callback module lock")
            .as_ref()
            .is_none_or(|module| module.upgrade().is_some_and(|module| !module.is_faulted()))
    }

    pub(crate) fn module_peer(&self) -> Option<BusName> {
        self.lifetime
            .lock()
            .expect("callback lifetime lock")
            .module
            .clone()
    }

    pub(crate) fn includes_peer(&self, peer: &BusName) -> bool {
        &self.destination == peer || self.module_peer().as_ref() == Some(peer)
    }

    pub(crate) fn revoke(&self) {
        let mut state = self.lifetime.lock().expect("callback lifetime lock");
        state.active = false;
        state.revoked = true;
    }

    pub(crate) fn matches(&self, header: &Header) -> bool {
        let state = self.lifetime.lock().expect("callback lifetime lock");
        state.active
            && self.module_live()
            && header.confidential
            && header.sender.as_ref() == state.module.as_ref()
            && state.module.is_some()
            && header.destination.as_ref() == Some(&self.destination)
            && self.endpoints.iter().any(|endpoint| {
                header.path.as_ref() == Some(&endpoint.path)
                    && header.interface.as_ref() == Some(&endpoint.interface)
                    && header
                        .member
                        .as_ref()
                        .is_some_and(|member| endpoint.members.contains(member))
            })
    }

    pub(crate) fn matches_directed_signal(
        &self,
        source: &BusName,
        destination: &BusName,
        header: &Header,
    ) -> bool {
        let state = self.lifetime.lock().expect("callback lifetime lock");
        state.active
            && self.module_live()
            && &self.destination == source
            && state.module.as_ref() == Some(destination)
            && self.signals.iter().any(|signal| {
                header.path.as_ref() == Some(&signal.path)
                    && header.interface.as_ref() == Some(&signal.interface)
                    && header
                        .member
                        .as_ref()
                        .is_some_and(|member| signal.members.contains(member))
            })
    }

    fn signal_destination(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
        member: &MemberName,
    ) -> Result<BusName> {
        let state = self.lifetime.lock().expect("callback lifetime lock");
        if !state.active
            || !self.module_live()
            || !self.signals.iter().any(|signal| {
                &signal.path == path
                    && &signal.interface == interface
                    && signal.members.contains(member)
            })
        {
            return Err(Error::failed("directed signal authority is unavailable"));
        }
        state
            .module
            .clone()
            .ok_or_else(|| Error::failed("directed signal recipient is unavailable"))
    }

    fn authorized_stream_module(&self, context: &CallContext) -> Result<BusName> {
        let state = self.lifetime.lock().expect("callback lifetime lock");
        if context.is_confidential()
            || self.stream_endpoints.is_empty()
            || !self.module_live()
            || !state.active
            || state.module.is_none()
            || context.authenticated_sender() != state.module.as_ref()
        {
            return Err(Error::failed("callback stream authority is unavailable"));
        }
        state
            .module
            .clone()
            .ok_or_else(|| Error::failed("callback stream recipient is unavailable"))
    }
}

/// Read-only liveness and authenticated module identity for a callback worker.
///
/// Check again after awaits and before publishing provider results. Revocation
/// prevents new work but cannot undo a provider write already in progress.
#[derive(Clone)]
pub struct HostCallbackGuard(Arc<CallbackState>);

impl HostCallbackGuard {
    /// The exact loaded module peer, installed before initialization can call back.
    pub fn module_peer(&self) -> Option<BusName> {
        self.0.module_peer()
    }

    /// Whether this confidential callback binding remains active for this sender.
    ///
    /// Ordinary stream callbacks are checked separately with
    /// [`HostCallbackGuard::authorizes_stream`].
    pub fn authorizes(&self, context: &CallContext) -> bool {
        let state = self.0.lifetime.lock().expect("callback lifetime lock");
        state.active
            && self.0.module_live()
            && context.is_confidential()
            && state.module.is_some()
            && context.authenticated_sender() == state.module.as_ref()
    }

    /// Whether this binding remains active for an ordinary stream callback
    /// from its exact authenticated module sender.
    pub fn authorizes_stream(&self, context: &CallContext) -> bool {
        self.0.authorized_stream_module(context).is_ok()
    }

    /// Number of callbacks whose user futures have not yet completed or dropped.
    /// A caller timeout does not remove an executing callback from this count.
    pub fn running(&self) -> usize {
        self.0
            .lifetime
            .lock()
            .expect("callback lifetime lock")
            .running
    }

    /// Wait for currently running user futures to complete or drop.
    /// Revoke first to prevent new admission. This proves no provider activity
    /// beyond the lifetime of the user future; detached provider tasks need
    /// their own completion fence.
    pub async fn drained(&self) {
        loop {
            let changed = self.0.drained.notified();
            if self.running() == 0 {
                return;
            }
            changed.await;
        }
    }
}

/// SDK-created local callback recipient, scoped once to one verified module.
///
/// This is intentionally not an attestation and cannot enroll an existing
/// connection, custom transport, well-known alias, or remote recipient. Drop
/// revokes immediately; explicit close also closes the owned local connection.
pub struct HostCallbackBinding {
    owner: std::sync::Weak<()>,
    broker: Broker,
    connection: Connection,
    state: Arc<CallbackState>,
    transport: Arc<MemoryTransport>,
    // Bounded test observers only; neither hook supplies production authority.
    #[cfg(test)]
    after_attach: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    fixture_controller: Mutex<Option<Arc<dyn Send + Sync>>>,
    #[cfg(test)]
    before_load_decision: Mutex<Option<(Arc<Notify>, tokio::sync::oneshot::Receiver<()>)>>,
    #[cfg(test)]
    after_load_termination: Mutex<Option<Arc<Notify>>>,
}

/// Outgoing operations on the binding's private connection.
///
/// This handle deliberately has no connection accessor, dereference,
/// conversion, or service registration surface. Clones retain only this API.
#[derive(Clone, Debug)]
pub struct HostCallbackProxy(crate::Proxy);

impl HostCallbackProxy {
    /// Override the deadline for outgoing calls.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.0 = self.0.with_timeout(timeout);
        self
    }
    /// The destination addressed by this handle.
    pub fn destination(&self) -> &BusName {
        self.0.destination()
    }
    /// The addressed object path.
    pub fn path(&self) -> &ObjectPath {
        self.0.path()
    }
    /// The addressed interface.
    pub fn interface(&self) -> &InterfaceName {
        self.0.interface()
    }
    /// Call a method and deserialize its ordinary reply.
    pub async fn call<R: serde::de::DeserializeOwned>(
        &self,
        member: &str,
        args: impl serde::Serialize,
    ) -> Result<R> {
        self.0.call(member, args).await
    }
    /// Call a method receiving a bounded streaming reply.
    pub async fn call_streaming<R: serde::de::DeserializeOwned>(
        &self,
        member: &str,
        args: impl serde::Serialize,
    ) -> Result<R> {
        self.0.call_streaming(member, args).await
    }
    /// Call an attested recipient with a confidential body.
    pub async fn call_confidential<R: serde::de::DeserializeOwned>(
        &self,
        member: &str,
        args: impl serde::Serialize,
    ) -> Result<R> {
        self.0.call_confidential(member, args).await
    }
    /// Call a broker module-control method with a redacted body.
    pub async fn call_sensitive<R: serde::de::DeserializeOwned>(
        &self,
        member: &str,
        args: impl serde::Serialize,
    ) -> Result<R> {
        self.0.call_sensitive(member, args).await
    }
    /// Read the broker's artifact attestation for the destination.
    pub async fn attestation(&self) -> Result<Option<crate::attest::Attestation>> {
        self.0.attestation().await
    }
    /// Whether the destination currently has an owner.
    pub async fn is_available(&self) -> Result<bool> {
        self.0.is_available().await
    }
    /// Subscribe to signals for the addressed object and interface.
    pub async fn receive_signal(
        &self,
        member: &str,
    ) -> Result<tokio::sync::broadcast::Receiver<crate::message::Message>> {
        self.0.receive_signal(member).await
    }
}

impl HostCallbackBinding {
    /// The exact SDK-created host destination for module configuration.
    pub fn unique_name(&self) -> &BusName {
        &self.state.destination
    }

    /// Accept a stream opened by this binding's currently authorized module.
    ///
    /// The raw connection remains private. The callback context supplies the
    /// exact authenticated sender, and the stream is accepted only from that
    /// sender while this binding remains active. This is for an explicitly
    /// declared ordinary stream callback; stream payloads are not confidential.
    ///
    /// # Errors
    /// Returns an error if the context is confidential, is not for this live
    /// binding, or the stream was not opened by its bound module.
    pub fn accept_stream_from_module(
        &self,
        context: &CallContext,
        stream: &StreamRef,
    ) -> Result<StreamReader> {
        let sender = self.authorized_stream_module(context)?;
        self.connection.accept_stream_from(stream, &sender)
    }

    /// Open a stream to this binding's currently authorized module.
    ///
    /// This is the callback-side counterpart of
    /// [`HostCallbackBinding::accept_stream_from_module`]. It does not expose
    /// the binding's connection or permit a caller-supplied destination.
    /// Like all streams, its payload uses ordinary TinyBus calls.
    ///
    /// # Errors
    /// Returns an error if the context is confidential, is not for this live
    /// binding, or the destination refuses the stream.
    pub async fn open_stream_to_module(
        &self,
        context: &CallContext,
        descriptor: StreamDescriptor,
        timeout: Duration,
    ) -> Result<StreamWriter> {
        let recipient = self.authorized_stream_module(context)?;
        self.connection
            .open_stream_with_timeout(&recipient, descriptor, timeout)
            .await
    }

    fn authorized_stream_module(&self, context: &CallContext) -> Result<BusName> {
        self.state.authorized_stream_module(context)
    }

    /// Construct an outgoing proxy using this private host connection.
    ///
    /// The outgoing handle cannot expose the owned callback connection:
    /// ```compile_fail
    /// # async fn example(binding: &tinybus::module::HostCallbackBinding) -> tinybus::Result<()> {
    /// let outgoing = binding.proxy("org.example.Module", "/module", "org.example.Module")?;
    /// let _: &tinybus::Connection = outgoing.connection();
    /// # Ok(())
    /// # }
    /// ```
    /// The proxy exposes no raw connection or service registration escape.
    pub fn proxy(
        &self,
        destination: &str,
        path: &str,
        interface: &str,
    ) -> Result<HostCallbackProxy> {
        self.connection
            .proxy(destination, path, interface)
            .map(HostCallbackProxy)
    }
    /// Read-only capability state for domain authorization and completion accounting.
    pub fn guard(&self) -> HostCallbackGuard {
        HostCallbackGuard(self.state.clone())
    }

    /// Emit a signal only to this binding's current verified module peer.
    ///
    /// The destination comes from the live binding and is never caller-chosen.
    /// The broker checks the exact declaration and active binding again when
    /// routing, closing the revoke/send race.
    ///
    /// # Errors
    /// Returns an error for an undeclared signal, inactive or revoked binding,
    /// missing recipient, or closed/backpressured transport.
    pub async fn emit_signal(
        &self,
        path: &ObjectPath,
        interface: &InterfaceName,
        member: &MemberName,
        body: impl serde::Serialize,
    ) -> Result<()> {
        let destination = self.state.signal_destination(path, interface, member)?;
        let message = crate::message::Message::directed_signal(
            destination,
            path.clone(),
            interface.clone(),
            member.clone(),
            crate::connection::to_body(&body)?,
        );
        self.connection.send(message).await
    }
    /// Immediately and idempotently prevent new route and user-dispatch admission.
    /// Running user futures remain accounted for until actual completion/drop.
    pub fn revoke(&self) {
        let mut router = self.broker.router.lock().expect("router lock");
        // Serialize against child creation: it holds this lock while checking
        // the parent and recording the child, so the walk sees every child
        // created before revocation and later attempts fail the liveness check.
        router.revoke_host_callbacks(&self.state);
        let descendants = self.state.live_descendants();
        for child in descendants {
            router.revoke_host_callbacks(&child);
        }
    }

    /// Create a fresh callback destination for the module bound to this live
    /// binding. The child has only the declared endpoints, and is revoked with
    /// this parent or when the module stops. Hosts use child bindings when a
    /// module operation needs a new isolated host-owned capability scope.
    ///
    /// # Errors
    /// Returns an error unless `context` is an authorized callback from the
    /// exact live module, or if endpoint declarations or registration fail.
    pub async fn create_child_binding(
        &self,
        context: &CallContext,
        endpoints: Vec<HostCallbackEndpoint>,
    ) -> Result<HostCallbackBinding> {
        if !HostCallbackGuard(self.state.clone()).authorizes(context) {
            return Err(Error::failed("callback child requires its live module"));
        }
        if endpoints.is_empty()
            || endpoints.len() > 32
            || endpoints.iter().any(|endpoint| {
                endpoint.members.is_empty()
                    || endpoint.members.len() > 64
                    || endpoint
                        .members
                        .iter()
                        .enumerate()
                        .any(|(index, member)| endpoint.members[..index].contains(member))
            })
            || endpoints.iter().enumerate().any(|(index, endpoint)| {
                endpoints[..index].iter().any(|previous| {
                    previous.path == endpoint.path && previous.interface == endpoint.interface
                })
            })
        {
            return Err(Error::failed(
                "callback child endpoint declaration is invalid",
            ));
        }
        let (host, peer) = MemoryTransport::pair();
        let unique = self.broker.attach(Arc::new(peer));
        let transport = Arc::new(host);
        let mut rollback = PeerRollback {
            broker: self.broker.clone(),
            peer: unique,
            transport: transport.clone(),
            committed: false,
        };
        let connection = Connection::attach(transport.clone());
        connection.handshake().await?;
        let destination = connection
            .unique_name()
            .ok_or_else(|| Error::failed("callback connection has no unique name"))?;
        let module_transport = self
            .state
            .module_transport
            .lock()
            .expect("callback module lock")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::failed("callback module is no longer live"))?;
        let child = Arc::new(CallbackState {
            destination,
            endpoints,
            stream_endpoints: Vec::new(),
            signals: Vec::new(),
            module_transport: Mutex::new(Some(Arc::downgrade(&module_transport))),
            _parent: Some(self.state.clone()),
            children: Mutex::new(Vec::new()),
            lifetime: Mutex::new(Lifetime {
                active: true,
                used: false,
                revoked: false,
                module: self.state.module_peer(),
                running: 0,
                served: Vec::new(),
                registering: Vec::new(),
            }),
            drained: Notify::new(),
            physical_drained: std::sync::Condvar::new(),
            #[cfg(test)]
            physical_wait_started: Notify::new(),
        });
        let mut router = self.broker.router.lock().expect("router lock");
        if !HostCallbackGuard(self.state.clone()).authorizes(context) {
            return Err(Error::failed("callback parent was revoked during creation"));
        }
        router.add_host_callbacks(child.clone())?;
        let mut children = self.state.children.lock().expect("callback children lock");
        children.retain(|existing| existing.strong_count() != 0);
        children.push(Arc::downgrade(&child));
        drop(children);
        drop(router);
        rollback.committed = true;
        Ok(HostCallbackBinding {
            owner: self.owner.clone(),
            broker: self.broker.clone(),
            connection,
            state: child,
            transport,
            #[cfg(test)]
            after_attach: Mutex::new(None),
            #[cfg(test)]
            fixture_controller: Mutex::new(None),
            #[cfg(test)]
            before_load_decision: Mutex::new(None),
            #[cfg(test)]
            after_load_termination: Mutex::new(None),
        })
    }
    /// Revoke and close the local connection. This does not cancel running user code.
    pub async fn close(&self) -> Result<()> {
        self.revoke();
        self.connection.close().await
    }
    /// Serve a declared endpoint with an unbypassable authenticated live guard.
    /// Every declared member must be implemented; duplicate registration fails.
    pub async fn serve_at(&self, path: ObjectPath, interface: impl Interface) -> Result<()> {
        let name = interface.name();
        let confidential_endpoint = self
            .state
            .endpoints
            .iter()
            .find(|endpoint| endpoint.path == path && endpoint.interface == name)
            .cloned();
        let stream_endpoint = self
            .state
            .stream_endpoints
            .iter()
            .find(|endpoint| endpoint.path == path && endpoint.interface == name)
            .cloned();
        if confidential_endpoint.is_none() && stream_endpoint.is_none() {
            return Err(Error::failed("callback endpoint is not declared"));
        }
        let provided = interface.members();
        if confidential_endpoint
            .iter()
            .flat_map(|endpoint| endpoint.members.iter())
            .chain(
                stream_endpoint
                    .iter()
                    .flat_map(|endpoint| endpoint.members.iter()),
            )
            .any(|member| !provided.contains(member))
        {
            return Err(Error::failed("callback endpoint members are missing"));
        }
        {
            let mut state = self.state.lifetime.lock().expect("callback lifetime lock");
            if state.used
                || state.revoked
                || state.served.contains(&(path.clone(), name.clone()))
                || state.registering.contains(&(path.clone(), name.clone()))
            {
                return Err(Error::failed(
                    "callback registration is immutable or duplicated",
                ));
            }
            state.registering.push((path.clone(), name.clone()));
        }
        let result = self
            .connection
            .serve_at(
                path.clone(),
                GuardedInterface {
                    interface,
                    confidential_endpoint,
                    stream_endpoint,
                    state: self.state.clone(),
                },
            )
            .await;
        {
            let mut state = self.state.lifetime.lock().expect("callback lifetime lock");
            state
                .registering
                .retain(|entry| entry != &(path.clone(), name.clone()));
            if result.is_ok() {
                state.served.push((path, name));
            }
        }
        result
    }
}

impl Drop for HostCallbackBinding {
    fn drop(&mut self) {
        self.revoke();
        // Drop cannot await. Closing the sender synchronously detaches the
        // owned peer without spawning a task that might never be polled.
        self.transport.close_outbound();
        self.broker.detach_unique(&self.state.destination);
    }
}

struct GuardedInterface<I> {
    interface: I,
    confidential_endpoint: Option<HostCallbackEndpoint>,
    stream_endpoint: Option<HostStreamCallbackEndpoint>,
    state: Arc<CallbackState>,
}

struct Running(Arc<CallbackState>);
impl Drop for Running {
    fn drop(&mut self) {
        let mut state = self
            .0
            .lifetime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.running -= 1;
        if state.running == 0 {
            self.0.drained.notify_waiters();
            self.0.physical_drained.notify_all();
        }
    }
}

#[async_trait]
impl<I: Interface> Interface for GuardedInterface<I> {
    fn name(&self) -> InterfaceName {
        self.confidential_endpoint
            .as_ref()
            .map(|endpoint| endpoint.interface.clone())
            .or_else(|| {
                self.stream_endpoint
                    .as_ref()
                    .map(|endpoint| endpoint.interface.clone())
            })
            .expect("declared callback interface")
    }
    fn members(&self) -> Vec<MemberName> {
        self.confidential_endpoint
            .iter()
            .flat_map(|endpoint| endpoint.members.iter().cloned())
            .chain(
                self.stream_endpoint
                    .iter()
                    .flat_map(|endpoint| endpoint.members.iter().cloned()),
            )
            .collect()
    }
    async fn call(&self, _: &MemberName, _: serde_json::Value) -> Result<serde_json::Value> {
        Err(Error::failed("authenticated scoped callback required"))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: serde_json::Value,
        context: &CallContext,
    ) -> Result<serde_json::Value> {
        // Serialized with revoke: queued deliveries cannot bypass the live
        // check by carrying an earlier route admission through an await.
        let _running = {
            let mut state = self.state.lifetime.lock().expect("callback lifetime lock");
            let confidential_member = self
                .confidential_endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint.members.contains(member));
            let stream_member = self
                .stream_endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint.members.contains(member));
            if !self.state.module_live()
                || confidential_member == stream_member
                || confidential_member && !context.is_confidential()
                || stream_member && context.is_confidential()
                || !state.active
                || state.module.is_none()
                || context.authenticated_sender() != state.module.as_ref()
            {
                return Err(Error::failed("scoped callback authority is unavailable"));
            }
            state.running += 1;
            Running(self.state.clone())
        };
        // The later-created user future is destroyed before Running on both
        // completion and cancellation; no response queue owns this accounting.
        let result = {
            let user = self.interface.call_with_context(member, args, context);
            user.await
        };
        drop(_running);
        result
    }
}

impl ModuleHost {
    /// Check that a callback binding can be attempted without consuming it.
    /// The loader repeats this check while atomically marking the binding used.
    pub(crate) fn validate_host_callback_binding(
        &self,
        binding: &HostCallbackBinding,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.inner.broker.router, &binding.broker.router)
            || !binding
                .owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, &self.inner.callback_owner))
        {
            return Err(Error::failed(
                "callback binding belongs to another host owner",
            ));
        }
        let state = binding
            .state
            .lifetime
            .lock()
            .expect("callback lifetime lock");
        if state.used
            || state.revoked
            || state.served.len() != binding.state.required_endpoint_count()
        {
            return Err(Error::failed(
                "callback binding is already used or endpoints are missing",
            ));
        }
        Ok(())
    }

    /// Whether this exact callback capability is retained by the named module.
    pub(crate) fn retains_host_callback_binding(
        &self,
        bus_name: &str,
        binding: &Arc<HostCallbackBinding>,
    ) -> bool {
        let peer = {
            let state = binding
                .state
                .lifetime
                .lock()
                .expect("callback lifetime lock");
            if !state.active || state.revoked || !binding.state.module_live() {
                return false;
            }
            state.module.clone()
        };
        let Some(peer) = peer else {
            return false;
        };
        self.inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .any(|module| {
                module.info.manifest.bus_name.as_str() == bus_name
                    && module.unique_name == peer
                    && module
                        ._callback_binding
                        .as_ref()
                        .is_some_and(|retained| Arc::ptr_eq(retained, binding))
            })
    }

    /// Return the live host callback capability retained by the exact loaded
    /// bus peer, if it was admitted with one.
    pub(crate) fn retained_host_callback_binding(
        &self,
        bus_name: &str,
    ) -> Option<Arc<HostCallbackBinding>> {
        let binding = self
            .inner
            .loaded
            .lock()
            .expect("module list lock")
            .iter()
            .find(|module| module.info.manifest.bus_name.as_str() == bus_name)?
            ._callback_binding
            .as_ref()?
            .clone();
        self.retains_host_callback_binding(bus_name, &binding)
            .then_some(binding)
    }

    /// Retain the resolver-owned callback capability with its exact admitted
    /// module, after verifying that the module was initialized through it.
    pub(crate) fn retain_host_callback_binding(
        &self,
        bus_name: &str,
        binding: Arc<HostCallbackBinding>,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.inner.broker.router, &binding.broker.router)
            || !binding
                .owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, &self.inner.callback_owner))
        {
            return Err(Error::failed(
                "callback binding belongs to another host owner",
            ));
        }
        let mut loaded = self.inner.loaded.lock().expect("module list lock");
        let module = loaded
            .iter_mut()
            .find(|module| module.info.manifest.bus_name.as_str() == bus_name)
            .ok_or_else(|| Error::failed("callback module is no longer loaded"))?;
        if !module
            .callback_state
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(state, &binding.state))
        {
            return Err(Error::failed(
                "callback binding does not belong to the loaded module",
            ));
        }
        module._callback_binding = Some(binding);
        Ok(())
    }

    /// Create a private local recipient for at most 32 exact callback interfaces.
    /// No authority exists until a verified module is loaded with this binding.
    pub async fn prepare_host_callbacks(
        &self,
        endpoints: Vec<HostCallbackEndpoint>,
    ) -> Result<HostCallbackBinding> {
        self.prepare_host_callbacks_with_stream_endpoints(endpoints, Vec::new())
            .await
    }

    /// Create an exact callback binding with confidential endpoints and a
    /// separate allowlist of ordinary stream-carrying methods.
    ///
    /// Stream endpoints are never treated as confidential: their handles and
    /// payload use ordinary TinyBus calls. They remain restricted to the
    /// binding's exact destination, active lifetime, and verified module
    /// sender. Members cannot overlap the confidential endpoint declarations.
    pub async fn prepare_host_callbacks_with_stream_endpoints(
        &self,
        endpoints: Vec<HostCallbackEndpoint>,
        stream_endpoints: Vec<HostStreamCallbackEndpoint>,
    ) -> Result<HostCallbackBinding> {
        self.prepare_host_callbacks_with_signals_and_stream_endpoints(
            endpoints,
            Vec::new(),
            stream_endpoints,
        )
        .await
    }

    /// Create a private callback recipient with a finite directed-signal set.
    ///
    /// Every signal declaration must share a path and interface with one of
    /// the callback endpoints. Directed signals target only the exact verified
    /// module peer loaded with the returned binding.
    ///
    /// # Errors
    /// Rejects malformed or duplicate declarations, undeclared signal
    /// endpoints, or a failed local TinyBus connection.
    pub async fn prepare_host_callbacks_with_signals(
        &self,
        endpoints: Vec<HostCallbackEndpoint>,
        signals: Vec<HostCallbackSignal>,
    ) -> Result<HostCallbackBinding> {
        self.prepare_host_callbacks_with_signals_and_stream_endpoints(
            endpoints,
            signals,
            Vec::new(),
        )
        .await
    }

    /// Create a callback binding with confidential methods, directed signals,
    /// and separately declared ordinary stream methods.
    pub async fn prepare_host_callbacks_with_signals_and_stream_endpoints(
        &self,
        endpoints: Vec<HostCallbackEndpoint>,
        signals: Vec<HostCallbackSignal>,
        stream_endpoints: Vec<HostStreamCallbackEndpoint>,
    ) -> Result<HostCallbackBinding> {
        let mut declared_interfaces = Vec::new();
        for (path, interface) in endpoints
            .iter()
            .map(|endpoint| (&endpoint.path, &endpoint.interface))
            .chain(
                stream_endpoints
                    .iter()
                    .map(|endpoint| (&endpoint.path, &endpoint.interface)),
            )
        {
            if !declared_interfaces
                .iter()
                .any(|(known_path, known_interface)| {
                    *known_path == path && *known_interface == interface
                })
            {
                declared_interfaces.push((path, interface));
            }
        }
        let all_endpoint_count = declared_interfaces.len();
        let invalid_stream_endpoint =
            stream_endpoints.iter().any(|endpoint| {
                endpoint.members.is_empty()
                    || endpoint.members.len() > 64
                    || endpoint
                        .members
                        .iter()
                        .enumerate()
                        .any(|(i, member)| endpoint.members[..i].contains(member))
            }) || stream_endpoints.iter().enumerate().any(|(i, endpoint)| {
                stream_endpoints[..i].iter().any(|previous| {
                    previous.path == endpoint.path && previous.interface == endpoint.interface
                }) || endpoints.iter().any(|confidential| {
                    confidential.path == endpoint.path
                        && confidential.interface == endpoint.interface
                        && endpoint
                            .members
                            .iter()
                            .any(|member| confidential.members.contains(member))
                })
            });
        if all_endpoint_count == 0
            || all_endpoint_count > 32
            || invalid_stream_endpoint
            || endpoints.is_empty() && !signals.is_empty()
            || endpoints.iter().any(|endpoint| {
                endpoint.members.is_empty()
                    || endpoint.members.len() > 64
                    || endpoint
                        .members
                        .iter()
                        .enumerate()
                        .any(|(i, member)| endpoint.members[..i].contains(member))
            })
            || endpoints.iter().enumerate().any(|(i, endpoint)| {
                endpoints[..i].iter().any(|previous| {
                    previous.path == endpoint.path && previous.interface == endpoint.interface
                })
            })
        {
            return Err(Error::failed("callback endpoint declaration is invalid"));
        }
        if signals.iter().any(|signal| {
            signal.members.is_empty()
                || signal.members.len() > 64
                || signal
                    .members
                    .iter()
                    .enumerate()
                    .any(|(i, member)| signal.members[..i].contains(member))
                || !endpoints.iter().any(|endpoint| {
                    endpoint.path == signal.path && endpoint.interface == signal.interface
                })
        }) || signals.iter().enumerate().any(|(i, signal)| {
            signals[..i].iter().any(|previous| {
                previous.path == signal.path && previous.interface == signal.interface
            })
        }) {
            return Err(Error::failed("callback signal declaration is invalid"));
        }
        let (host, peer) = MemoryTransport::pair();
        let unique = self.inner.broker.attach(Arc::new(peer));
        let transport = Arc::new(host);
        let mut rollback = PeerRollback {
            broker: self.inner.broker.clone(),
            peer: unique,
            transport: transport.clone(),
            committed: false,
        };
        let connection = Connection::attach(transport.clone());
        connection.handshake().await?;
        let destination = connection
            .unique_name()
            .ok_or_else(|| Error::failed("callback connection has no unique name"))?;
        rollback.committed = true;
        Ok(HostCallbackBinding {
            owner: Arc::downgrade(&self.inner.callback_owner),
            broker: self.inner.broker.clone(),
            connection,
            transport,
            #[cfg(test)]
            after_attach: Mutex::new(None),
            #[cfg(test)]
            fixture_controller: Mutex::new(None),
            #[cfg(test)]
            before_load_decision: Mutex::new(None),
            #[cfg(test)]
            after_load_termination: Mutex::new(None),
            state: Arc::new(CallbackState {
                destination,
                endpoints,
                stream_endpoints,
                signals,
                module_transport: Mutex::new(None),
                _parent: None,
                children: Mutex::new(Vec::new()),
                lifetime: Mutex::new(Lifetime {
                    active: false,
                    used: false,
                    revoked: false,
                    module: None,
                    running: 0,
                    served: Vec::new(),
                    registering: Vec::new(),
                }),
                drained: Notify::new(),
                physical_drained: std::sync::Condvar::new(),
                #[cfg(test)]
                physical_wait_started: Notify::new(),
            }),
        })
    }

    /// Load a hash-verified file and bind its exact peer before any initialization.
    ///
    /// All endpoints must already be served. The binding is single-use even on
    /// failure. Configuration is frozen for reinitialization. Generic load APIs
    /// grant no callback authority. The caller supplies the binding's unique
    /// destination in its domain-specific configuration; it cannot change scope.
    pub async fn load_file_with_host_callbacks(
        &self,
        path: impl AsRef<Path>,
        config: serde_json::Value,
        binding: &HostCallbackBinding,
    ) -> Result<ModuleInfo> {
        self.load_file_with_host_callbacks_checked(path, config, binding, None, None, None)
            .await
    }

    /// Load a verified module with callbacks only if its manifest has the exact
    /// registry identity expected by the caller. Identity is checked before the
    /// initializer runs, so the callback recipient cannot observe configuration
    /// intended for a different module.
    pub async fn load_file_with_host_callbacks_for(
        &self,
        path: impl AsRef<Path>,
        config: serde_json::Value,
        binding: &HostCallbackBinding,
        expected_bus_name: &str,
        expected_object_path: &str,
    ) -> Result<ModuleInfo> {
        self.load_file_with_host_callbacks_checked(
            path,
            config,
            binding,
            Some((expected_bus_name, expected_object_path)),
            None,
            None,
        )
        .await
    }

    /// Load a checksum-verified cached release with callbacks bound to the
    /// exact registry identity before initialization.
    pub(crate) async fn load_github_release_cached_with_host_callbacks(
        &self,
        release: &super::CachedRelease<'_>,
        config: serde_json::Value,
        binding: &Arc<HostCallbackBinding>,
        expected_bus_name: &str,
        expected_object_path: &str,
    ) -> Result<ModuleInfo> {
        let (module, archive_sha256) = crate::module::github::acquire_cached(release)?;
        self.load_file_with_host_callbacks_checked(
            module,
            config,
            binding,
            Some((expected_bus_name, expected_object_path)),
            Some(archive_sha256),
            release.expected_library_sha256.map(str::to_ascii_lowercase),
        )
        .await
    }

    /// Load an installer-shipped digest marker with callbacks bound to the
    /// exact registry identity before initialization.
    pub(crate) async fn load_bundled_marked_with_host_callbacks(
        &self,
        directory: &Path,
        asset_name: &str,
        pin: &str,
        config: serde_json::Value,
        binding: &Arc<HostCallbackBinding>,
        expected_identity: (&str, &str),
    ) -> Result<ModuleInfo> {
        let hit =
            crate::module::cache::find_marked(directory, asset_name, pin).ok_or_else(|| {
                Error::module_refused(
                    directory,
                    "bundled digest marker does not match the pin, or the extraction is unusable",
                )
            })?;
        self.load_file_with_host_callbacks_checked(
            hit.module,
            config,
            binding,
            Some(expected_identity),
            None,
            None,
        )
        .await
    }

    async fn load_file_with_host_callbacks_checked(
        &self,
        path: impl AsRef<Path>,
        config: serde_json::Value,
        binding: &HostCallbackBinding,
        expected_identity: Option<(&str, &str)>,
        pinned_sha256: Option<String>,
        pinned_library_sha256: Option<String>,
    ) -> Result<ModuleInfo> {
        if !Arc::ptr_eq(&self.inner.broker.router, &binding.broker.router)
            || !binding
                .owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, &self.inner.callback_owner))
        {
            return Err(Error::failed(
                "callback binding belongs to another host owner",
            ));
        }
        {
            let mut state = binding
                .state
                .lifetime
                .lock()
                .expect("callback lifetime lock");
            if state.used
                || state.revoked
                || state.served.len() != binding.state.required_endpoint_count()
            {
                return Err(Error::failed(
                    "callback binding is already used or endpoints are missing",
                ));
            }
            state.used = true;
        }
        let mut rollback = LoadRollback {
            binding,
            host: self,
            transport: None,
            committed: false,
            cause: AdmissionTermination::Failed,
        };
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            check_directory(parent)?;
        }
        check_file(path)?;
        let allowlisted = allowlisted_hash(path, std::fs::File::open(path)?)?;
        let hash = match (allowlisted, pinned_sha256.is_some()) {
            (Some(hash), _) => hash,
            (None, true) => crate::module::hash::file_hex(std::fs::File::open(path)?)?,
            (None, false) => {
                return Err(Error::module_refused(
                    path,
                    "scoped callbacks require a verified artifact",
                ));
            }
        };
        if pinned_library_sha256
            .as_ref()
            .is_some_and(|pinned| !pinned.eq_ignore_ascii_case(&hash))
        {
            return Err(Error::module_refused(
                path,
                "release library pin disagrees with the local module allowlist",
            ));
        }
        let library_pin = pinned_library_sha256.unwrap_or_else(|| hash.clone());
        let artifact = loader::load_verified(
            path,
            self.inner.strict.load(Ordering::Acquire),
            Some(&library_pin),
        )?;
        self.ensure_dependencies(&artifact.manifest, path)?;
        let current_library = allowlisted_hash(path, std::fs::File::open(path)?)?
            .or_else(|| crate::module::hash::file_hex(std::fs::File::open(path).ok()?).ok());
        if current_library.as_ref() != Some(&library_pin) {
            return Err(Error::module_refused(
                path,
                "verified artifact changed during loading",
            ));
        }
        let transport = {
            let _admission = self.inner.admission.lock().expect("module admission lock");
            let admitted = self.validate(path, &artifact.descriptor, &artifact.manifest)?;
            if expected_identity.is_some_and(|(bus_name, object_path)| {
                admitted.manifest.bus_name.as_str() != bus_name
                    || admitted.manifest.object_path.as_str() != object_path
            }) {
                return Err(Error::module_refused(
                    path,
                    "artifact identity does not match the requested module",
                ));
            }
            if self
                .inner
                .loaded
                .lock()
                .expect("module list lock")
                .iter()
                .any(|module| module.info.name == admitted.name)
            {
                return Err(Error::module_refused(path, "module name is already loaded"));
            }
            // Reservation happens inside attach_transport. Initialization is
            // deferred even for an eager module until routes and identity exist.
            let bytes = serde_json::to_vec(&config)
                .map_err(|_| Error::failed("module configuration is invalid"))?;
            let fingerprint = callback_config_fingerprint(&bytes);
            drop(config);
            let trusted_host = self.inner.broker.restricted_module_controller_binding();
            let pre_ready_routes = self
                .inner
                .pre_ready_confidential_routes
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let (transport, host) = ModuleTransport::new_with_controller_binding(
                admitted.name.clone(),
                bytes,
                trusted_host,
                pre_ready_routes,
            );
            transport.bind_host_callbacks(binding.state.clone())?;
            #[cfg(test)]
            {
                // Transfer the fixture config borrow before C can enter, so
                // setup unwind and loader cancellation retain the same owner.
                *transport
                    .fixture_controller
                    .lock()
                    .expect("fixture controller lock") = binding
                    .fixture_controller
                    .lock()
                    .expect("fixture controller lock")
                    .take();
            }
            transport.pause_initialization();
            transport.defer_initialize(artifact.init, host);
            let name = admitted.name.clone();
            rollback.transport = Some(transport.clone());
            self.attach_transport(
                path,
                admitted,
                artifact.manifest,
                transport.clone(),
                Activation {
                    lazy_init: true,
                    lazy_load: false,
                    descriptor_info: None,
                    pinned_sha256: Some(pinned_sha256.unwrap_or_else(|| hash.clone())),
                    pinned_library_sha256: Some(library_pin),
                },
            )?;
            rollback.transport = Some(transport.clone());
            #[cfg(test)]
            {
                let observer = binding
                    .after_attach
                    .lock()
                    .expect("test attach observer")
                    .take();
                if let Some(observer) = observer {
                    observer();
                }
            }
            let mut loaded = self.inner.loaded.lock().expect("module list lock");
            let module = loaded
                .iter_mut()
                .find(|module| module.info.name == name)
                .expect("module was attached");
            {
                let mut state = binding
                    .state
                    .lifetime
                    .lock()
                    .expect("callback lifetime lock");
                if state.revoked || state.module.is_some() {
                    return Err(Error::failed("callback binding was concurrently used"));
                }
                *binding
                    .state
                    .module_transport
                    .lock()
                    .expect("callback module lock") = Some(Arc::downgrade(&transport));
                state.module = Some(module.unique_name.clone());
                state.active = true;
            }
            module.callback_config = Some(fingerprint);
            module.callback_state = Some(binding.state.clone());
            self.inner
                .broker
                .router
                .lock()
                .expect("router lock")
                .add_host_callbacks(binding.state.clone())?;
            transport.release_initialization(true);
            transport
        };
        // All setup operations above are genuine failures. From this point a
        // dropped waiter is cancellation; actual init error/deadline is explicit.
        rollback.cause = AdmissionTermination::Canceled;
        let initialized = transport.ensure_initialized().await;
        let failure = match initialized {
            Err(Error::ConnectionClosed) => Some(AdmissionTermination::Canceled),
            Err(_) => Some(AdmissionTermination::Failed),
            Ok(()) => {
                match tokio::time::timeout(Duration::from_secs(5), transport.wait_ready()).await {
                    Err(_) => Some(AdmissionTermination::Failed),
                    Ok(()) if !transport.is_ready() => Some(AdmissionTermination::Canceled),
                    Ok(()) => None,
                }
            }
        };
        if let Some(cause) = failure {
            rollback.cause = cause;
        }
        #[cfg(test)]
        {
            let observer = binding
                .before_load_decision
                .lock()
                .expect("test decision observer")
                .take();
            if let Some((entered, release)) = observer {
                entered.notify_one();
                release.await.expect("test decision observer released");
            }
        }
        if let Some(cause) = failure {
            let fault = transport.commit_admission_termination(cause);
            binding.revoke();
            transport.request_terminal(fault);
            #[cfg(test)]
            if let Some(entered) = binding.after_load_termination.lock().unwrap().take() {
                entered.notify_one();
            }
            let _ = transport.close().await;
            self.inner
                .loaded
                .lock()
                .expect("module list lock")
                .retain(|module| !Arc::ptr_eq(&module.transport, &transport));
            return Err(Error::module_refused(path, "module initialization failed"));
        }
        let loaded = self.inner.loaded.lock().expect("module list lock");
        let info = loaded
            .iter()
            .find(|module| Arc::ptr_eq(&module.transport, &transport))
            .ok_or_else(|| {
                Error::module_refused(path, "module was removed before load completion")
            })?
            .snapshot();
        let lifetime = binding
            .state
            .lifetime
            .lock()
            .expect("callback lifetime lock");
        // Snapshot first, then validate liveness at the commit point. A fault
        // racing this point may be a subsequent lifecycle event, but must never
        // turn the returned successful snapshot into Faulted or Stopped.
        if lifetime.revoked
            || !lifetime.active
            || transport.admission_closed()
            || !transport.is_ready()
            || !matches!(info.state, ModuleState::Ready)
        {
            return Err(Error::module_refused(
                path,
                "module became unavailable before load completion",
            ));
        }
        rollback.committed = true;
        Ok(info)
    }
}

struct LoadRollback<'a> {
    binding: &'a HostCallbackBinding,
    host: &'a ModuleHost,
    transport: Option<Arc<ModuleTransport>>,
    committed: bool,
    cause: AdmissionTermination,
}
impl Drop for LoadRollback<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let fault = self
            .transport
            .as_ref()
            .map(|transport| transport.commit_admission_termination(self.cause));
        self.binding.revoke();
        self.binding.transport.close_outbound();
        self.binding
            .broker
            .detach_unique(&self.binding.state.destination);
        if let Some(transport) = &self.transport {
            transport.release_initialization(false);
            let peer = self.binding.state.module_peer().or_else(|| {
                self.host
                    .inner
                    .loaded
                    .lock()
                    .expect("module list lock")
                    .iter()
                    .find(|module| Arc::ptr_eq(&module.transport, transport))
                    .map(|module| module.unique_name.clone())
            });
            if let Some(peer) = peer {
                self.host.inner.broker.detach_unique(&peer);
            }
            // A timed-out initializer may still run. Stop is best effort;
            // revocation/detach removes authority without claiming cessation.
            transport.request_terminal(fault.expect("rollback transport cause"));
            self.host
                .inner
                .loaded
                .lock()
                .expect("module list lock")
                .retain(|module| !Arc::ptr_eq(&module.transport, transport));
        }
    }
}

impl Drop for ModuleHostInner {
    fn drop(&mut self) {
        let modules = self.loaded.get_mut().expect("module list lock");
        let transports: Vec<_> = modules
            .iter()
            .map(|module| module.transport.clone())
            .collect();
        for transport in &transports {
            transport.commit_terminal(false);
        }
        {
            let mut router = self.broker.router.lock().expect("router lock");
            for module in modules.iter() {
                router.revoke_callbacks_for_peer(&module.unique_name);
            }
        }
        for transport in transports {
            transport.request_terminal(false);
        }
    }
}

struct PeerRollback {
    broker: Broker,
    peer: BusName,
    transport: Arc<MemoryTransport>,
    committed: bool,
}
impl Drop for PeerRollback {
    fn drop(&mut self) {
        if !self.committed {
            self.transport.close_outbound();
            self.broker.detach_unique(&self.peer);
        }
    }
}

#[cfg(test)]
#[path = "host_callbacks_tests.rs"]
mod tests;
