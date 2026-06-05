use crate::types::DeliveryOutcome;

pub fn simulate_delivery(id:u64,retry_count:u64)->DeliveryOutcome{

    match id {
        1 => DeliveryOutcome::Success,
        2 => {
            if retry_count <3 {
                DeliveryOutcome::TemporaryFailure
            }else {
                DeliveryOutcome::Success
            }
        },
        3=>  DeliveryOutcome::PermanentFailure,
        4=>  DeliveryOutcome::TemporaryFailure,
        _ => DeliveryOutcome::Success,
    }
      
}