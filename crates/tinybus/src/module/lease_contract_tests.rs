use super::*;

struct ContractError;

impl std::fmt::Debug for ContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ContractError")
    }
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("contract error")
    }
}

impl std::error::Error for ContractError {}

struct ContractDriver;

#[async_trait::async_trait]
impl RuntimeLeaseDriver for ContractDriver {
    type Reservation = ();
    type Object = ();
    type Error = ContractError;

    async fn reserve(&self) -> Result<Self::Reservation, Self::Error> {
        Ok(())
    }

    fn validate(&self, _: &Self::Reservation) -> Result<(), Self::Error> {
        Ok(())
    }

    fn callback_address(
        &self,
        _: &Self::Reservation,
        _: &Self::Object,
    ) -> Result<(crate::BusName, crate::ObjectPath), Self::Error> {
        Ok((
            "ai.tinyhumans.tinybus.tests.Contract".parse().unwrap(),
            "/ai/tinyhumans/tinybus/tests/Contract".parse().unwrap(),
        ))
    }

    async fn object(&self, _: &Self::Reservation) -> Result<Self::Object, Self::Error> {
        Ok(())
    }

    async fn start(&self, _: &Self::Reservation) -> StartOutcome<Self::Error> {
        StartOutcome::Started
    }

    async fn stop(&self, _: &Self::Reservation) -> StopOutcome<Self::Error> {
        StopOutcome::Acknowledged
    }

    fn failed(&self, _: &Self::Reservation) -> bool {
        false
    }
}

#[test]
fn the_protocol_contract_compiles_without_the_module_loader_feature() {
    fn assert_driver<T: RuntimeLeaseDriver>() {}
    assert_driver::<ContractDriver>();
    assert_eq!(
        RetryPolicy::new(std::time::Duration::from_millis(1), vec![])
            .unwrap()
            .retry_delays(),
        &[]
    );
}
