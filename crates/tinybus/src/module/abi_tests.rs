use super::*;

#[test]
fn the_current_descriptor_describes_this_build_and_truncates_long_fields() {
    let descriptor = TbAbiDescriptor::current(
        "a-module-name-that-is-deliberately-longer-than-the-fixed-abi-field-can-hold",
        "0.1.0",
    );

    assert_eq!(descriptor.magic, ABI_MAGIC);
    assert_eq!(descriptor.abi_revision, ABI_REVISION);
    assert_eq!(
        descriptor.descriptor_size as usize,
        size_of::<TbAbiDescriptor>()
    );
    assert_eq!(descriptor.pointer_width, usize::BITS);
    assert_eq!(field_bytes(&descriptor.module_name).len(), 64);
    assert_eq!(field_bytes(&descriptor.module_version), b"0.1.0");
}

#[test]
fn field_bytes_stops_at_nul_or_uses_the_entire_field() {
    assert_eq!(field_bytes(b"abc\0tail"), b"abc");
    assert_eq!(field_bytes(b"whole"), b"whole");
}

#[test]
fn an_uninitialized_module_vtable_refuses_calls() {
    let vtable = TbModuleVtable::default();
    assert_eq!(
        TB_MODULE_VTABLE_BASE_SIZE as usize,
        std::mem::offset_of!(TbModuleVtable, reinitialize)
    );
    assert_eq!(vtable.size as usize, size_of::<TbModuleVtable>());
    assert_eq!(
        unsafe { (vtable.deliver)(vtable.module_ctx, std::ptr::null(), 0) },
        TB_CLOSED
    );
    assert_eq!(
        unsafe { (vtable.shutdown)(vtable.module_ctx, 0) },
        TB_CLOSED
    );
}

// These are the original ABI allocations, not truncated new vtables. Keeping
// their declarations independent detects accidental prefix or tail growth.
#[repr(C)]
struct FrozenHostPrefix {
    size: u32,
    reserved: u32,
    ctx: *mut c_void,
    send: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> i32,
    wake: unsafe extern "C" fn(*mut c_void),
    log: unsafe extern "C" fn(*mut c_void, u32, *const u8, usize),
    fault: unsafe extern "C" fn(*mut c_void, *const u8, usize),
    config: TbSlice,
    ready: unsafe extern "C" fn(*mut c_void),
}

#[repr(C)]
struct FrozenRoutingHost {
    prefix: FrozenHostPrefix,
    routing: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
}

unsafe extern "C" fn send(_: *mut c_void, _: *const u8, _: usize) -> i32 {
    TB_OK
}
unsafe extern "C" fn wake(_: *mut c_void) {}
unsafe extern "C" fn log(_: *mut c_void, _: u32, _: *const u8, _: usize) {}
unsafe extern "C" fn fault(_: *mut c_void, _: *const u8, _: usize) {}
unsafe extern "C" fn query(ctx: *mut c_void) -> u32 {
    unsafe { ctx.cast::<u32>().read() }
}
unsafe extern "C" fn trusted_host_query(
    _: *mut c_void,
    out: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> i32 {
    if out_len.is_null() || (out.is_null() && capacity != 0) {
        return TB_BAD_ARGUMENT;
    }
    let sender = b":1.42";
    unsafe { out_len.write(0) };
    if capacity < sender.len() {
        unsafe { out_len.write(sender.len()) };
        return TB_BUFFER_TOO_SMALL;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(sender.as_ptr(), out, sender.len());
        out_len.write(sender.len());
    }
    TB_OK
}

fn frozen_prefix(size: usize) -> FrozenHostPrefix {
    FrozenHostPrefix {
        size: size as u32,
        reserved: 0,
        ctx: std::ptr::null_mut(),
        send,
        wake,
        log,
        fault,
        config: TbSlice {
            ptr: std::ptr::null(),
            len: 0,
        },
        ready: wake,
    }
}

#[test]
fn routing_tail_survives_newer_optional_tail() {
    let value = 1u32;
    let mut old = FrozenRoutingHost {
        prefix: frozen_prefix(size_of::<FrozenRoutingHost>()),
        routing: Some(query),
    };
    old.prefix.ctx = std::ptr::from_ref(&value).cast_mut().cast();
    let normalized =
        unsafe { TbHostVtable::read_compatible(std::ptr::from_ref(&old).cast()) }.unwrap();
    let routing = normalized
        .broker_routing
        .expect("complete original routing tail is retained");
    assert_eq!(unsafe { routing(normalized.host_ctx) }, 1);
    assert!(normalized.native_terminal_state.is_none());
}

#[test]
fn prefix_hosts_normalize_both_tails() {
    assert_eq!(ABI_REVISION, 1);
    assert_eq!(
        size_of::<FrozenHostPrefix>(),
        TB_HOST_VTABLE_BASE_SIZE as usize
    );
    assert_eq!(
        size_of::<FrozenHostPrefix>(),
        std::mem::offset_of!(TbHostVtable, broker_routing)
    );
    assert!(unsafe { TbHostVtable::read_compatible(std::ptr::null()) }.is_none());
    // The rejected host owns only its declared bytes, with vtable alignment.
    let short = TB_HOST_VTABLE_BASE_SIZE - 1;
    let layout =
        std::alloc::Layout::from_size_align(short as usize, align_of::<TbHostVtable>()).unwrap();
    let storage = unsafe { std::alloc::alloc_zeroed(layout) };
    assert!(!storage.is_null());
    unsafe { storage.cast::<u32>().write(short) };
    let rejected = unsafe { TbHostVtable::read_compatible(storage.cast()) }.is_none();
    unsafe { std::alloc::dealloc(storage, layout) };
    assert!(rejected);
    let prefix = frozen_prefix(size_of::<FrozenHostPrefix>());
    let normalized =
        unsafe { TbHostVtable::read_compatible(std::ptr::from_ref(&prefix).cast()) }.unwrap();
    assert!(normalized.broker_routing.is_none());
    assert!(normalized.native_terminal_state.is_none());
}

#[test]
fn terminal_tail_requires_its_complete_field() {
    // Byte allocations are sized to the declared boundary and aligned to the
    // vtable. No reference to a full vtable is created from shorter storage.
    let routing_end = std::mem::offset_of!(TbHostVtable, broker_routing)
        + size_of::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>();
    let terminal_end = std::mem::offset_of!(TbHostVtable, native_terminal_state)
        + size_of::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>();
    let state = TB_NATIVE_TERMINAL_LIVE;
    for size in [
        routing_end - 1,
        routing_end,
        terminal_end - 1,
        terminal_end,
        terminal_end + 16,
    ] {
        for terminal_present in [false, true] {
            let layout =
                std::alloc::Layout::from_size_align(size, align_of::<TbHostVtable>()).unwrap();
            let storage = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!storage.is_null());
            let mut prefix = frozen_prefix(size);
            prefix.ctx = std::ptr::from_ref(&state).cast_mut().cast();
            unsafe { storage.cast::<FrozenHostPrefix>().write(prefix) };
            if size >= routing_end {
                unsafe {
                    storage
                        .add(std::mem::offset_of!(TbHostVtable, broker_routing))
                        .cast::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>()
                        .write(Some(query))
                };
            }
            if size >= terminal_end {
                unsafe {
                    storage
                        .add(std::mem::offset_of!(TbHostVtable, native_terminal_state))
                        .cast::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>()
                        .write(terminal_present.then_some(query))
                };
            }
            let normalized = unsafe { TbHostVtable::read_compatible(storage.cast()) }.unwrap();
            unsafe { std::alloc::dealloc(storage, layout) };
            assert_eq!(
                normalized.broker_routing.is_some(),
                size >= routing_end,
                "routing size {size}"
            );
            assert_eq!(
                normalized.native_terminal_state.is_some(),
                size >= terminal_end && terminal_present,
                "terminal size {size}"
            );
            if let Some(query) = normalized.native_terminal_state {
                assert_eq!(
                    unsafe { query(normalized.host_ctx) },
                    TB_NATIVE_TERMINAL_LIVE
                );
            }
        }
    }
}

#[test]
fn trusted_host_tail_requires_its_complete_field_and_preserves_older_tails() {
    let terminal_end = std::mem::offset_of!(TbHostVtable, native_terminal_state)
        + size_of::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>();
    let trusted_host_end = std::mem::offset_of!(TbHostVtable, trusted_host_binding)
        + size_of::<Option<unsafe extern "C" fn(*mut c_void, *mut u8, usize, *mut usize) -> i32>>();
    for size in terminal_end..=trusted_host_end {
        let layout = std::alloc::Layout::from_size_align(size, align_of::<TbHostVtable>()).unwrap();
        let storage = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!storage.is_null());
        unsafe {
            storage
                .cast::<FrozenHostPrefix>()
                .write(frozen_prefix(size))
        };
        unsafe {
            storage
                .add(std::mem::offset_of!(TbHostVtable, broker_routing))
                .cast::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>()
                .write(Some(query));
            storage
                .add(std::mem::offset_of!(TbHostVtable, native_terminal_state))
                .cast::<Option<unsafe extern "C" fn(*mut c_void) -> u32>>()
                .write(Some(query));
        }
        if size >= trusted_host_end {
            unsafe {
                storage
                    .add(std::mem::offset_of!(TbHostVtable, trusted_host_binding))
                    .cast::<Option<
                        unsafe extern "C" fn(*mut c_void, *mut u8, usize, *mut usize) -> i32,
                    >>()
                    .write(Some(trusted_host_query));
            }
        }
        let normalized = unsafe { TbHostVtable::read_compatible(storage.cast()) }.unwrap();
        unsafe { std::alloc::dealloc(storage, layout) };
        assert!(normalized.broker_routing.is_some());
        assert!(normalized.native_terminal_state.is_some());
        assert_eq!(
            normalized.trusted_host_binding.is_some(),
            size >= trusted_host_end,
            "declared host vtable size {size}"
        );
        if let Some(query) = normalized.trusted_host_binding {
            let mut bytes = [0; 8];
            let mut len = usize::MAX;
            assert_eq!(
                unsafe {
                    query(
                        normalized.host_ctx,
                        bytes.as_mut_ptr(),
                        bytes.len(),
                        &mut len,
                    )
                },
                TB_OK
            );
            assert_eq!(&bytes[..len], b":1.42");
        }
    }
}
