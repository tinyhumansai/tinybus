use tinybus::{Connection, broker::Broker, transport::memory::MemoryBus};

const NAME: &str = "ai.example.msrv.Echo";
const PATH: &str = "/ai/example/msrv/Echo";

struct Echo;

#[tinybus::interface(name = "ai.example.msrv.Echo")]
impl Echo {
    async fn echo(&self, value: String) -> tinybus::Result<String> {
        Ok(value)
    }
}

#[tokio::test]
async fn rust_185_client_roundtrips_over_the_real_memory_broker() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());

    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(NAME).await.unwrap();
    service
        .serve_at(tinybus::ObjectPath::new(PATH).unwrap(), Echo)
        .await
        .unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = client.proxy(NAME, PATH, NAME).unwrap();
    let reply = proxy
        .call::<String>("Echo", ("Rust 1.85".to_owned(),))
        .await
        .unwrap();

    assert_eq!(reply, "Rust 1.85");
}
