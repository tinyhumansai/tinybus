//! Adapters for the [`Transport`](crate::ports::Transport) and
//! [`Listener`](crate::ports::Listener) ports.
//!
//! [`memory`] is always compiled and is what the test suite runs on. [`unix`]
//! is the production transport and sits behind the `uds` feature, so a kernel
//! that only wants an in-process bus never links `tokio/net`.

pub mod memory;

#[cfg(all(feature = "uds", unix))]
pub mod unix;

/// Intrinsic downcast: an overridable `as_any` hook could lie about the concrete
/// receiver while injecting forged frames from another transport. This uses
/// `Any`'s blanket implementation and works before trait upcasting stabilized.
pub(crate) fn downcast<T: crate::ports::Transport>(
    transport: &dyn crate::ports::Transport,
) -> Option<&T> {
    if std::any::Any::type_id(transport) != std::any::TypeId::of::<T>() {
        return None;
    }
    // SAFETY: intrinsic TypeId above proves that the trait object's data pointer
    // is a T. The borrow retains exactly the original lifetime.
    Some(unsafe { &*(std::ptr::from_ref(transport).cast::<T>()) })
}
