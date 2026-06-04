use crate::types::{SeedPayment,PaymentStatus,ModeOfPayment,CardType};




/*
======================================
CURRENT DATASET COVERS
======================================

1. Happy Path Payments
   - Normal successful transactions

2. Validation Failures
   - Zero amount
   - Negative amount
   - Extreme values

3. Payment Status Variations
   - Succeeded
   - Failed
   - Refunded

4. Merchant Variations
   - Different merchant IDs
   - Different payment methods

======================================
FUTURE DATASETS
======================================

1. Duplicate Requests
   Purpose:
   Test idempotency handling.

2. Retry Scenarios
   Purpose:
   Merchant fails N times then succeeds.

3. Dead Letter Queue Scenarios
   Purpose:
   Merchant never succeeds.

4. Concurrency Scenarios
   Purpose:
   Multiple workers processing events.

5. Shutdown Recovery Scenarios
   Purpose:
   Crash recovery testing.

6. High Volume Load Scenarios
   Purpose:
   Performance testing.

======================================
*/

pub fn seed_payments() -> Vec<SeedPayment> {
    vec![
        // =========================
        // SUCCESS PAYMENTS
        // =========================

        SeedPayment { merchant_id: 1, order_id: 1001, amount: 499, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 1, order_id: 1002, amount: 999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 1, order_id: 1003, amount: 1499, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },
        SeedPayment { merchant_id: 2, order_id: 1004, amount: 1999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Card(CardType::Debit) },
        SeedPayment { merchant_id: 2, order_id: 1005, amount: 2499, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::NetBanking },
        SeedPayment { merchant_id: 2, order_id: 1006, amount: 2999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Cash },
        SeedPayment { merchant_id: 3, order_id: 1007, amount: 3499, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 3, order_id: 1008, amount: 3999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },
        SeedPayment { merchant_id: 3, order_id: 1009, amount: 4999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Card(CardType::Debit) },
        SeedPayment { merchant_id: 4, order_id: 1010, amount: 5999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::NetBanking },

        // =========================
        // FAILED PAYMENTS
        // =========================

        SeedPayment { merchant_id: 5, order_id: 2001, amount: 500, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 5, order_id: 2002, amount: 1000, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },
        SeedPayment { merchant_id: 5, order_id: 2003, amount: 1500, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Card(CardType::Debit) },
        SeedPayment { merchant_id: 6, order_id: 2004, amount: 2000, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::NetBanking },
        SeedPayment { merchant_id: 6, order_id: 2005, amount: 2500, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Cash },
        SeedPayment { merchant_id: 6, order_id: 2006, amount: 3000, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 7, order_id: 2007, amount: 3500, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },
        SeedPayment { merchant_id: 7, order_id: 2008, amount: 4000, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Card(CardType::Debit) },
        SeedPayment { merchant_id: 7, order_id: 2009, amount: 4500, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::NetBanking },
        SeedPayment { merchant_id: 8, order_id: 2010, amount: 5000, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Cash },

        // =========================
        // REFUNDED PAYMENTS
        // =========================

        SeedPayment { merchant_id: 9, order_id: 3001, amount: 799, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 9, order_id: 3002, amount: 1299, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },
        SeedPayment { merchant_id: 9, order_id: 3003, amount: 1799, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Card(CardType::Debit) },
        SeedPayment { merchant_id: 10, order_id: 3004, amount: 2299, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::NetBanking },
        SeedPayment { merchant_id: 10, order_id: 3005, amount: 2799, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Cash },
        SeedPayment { merchant_id: 10, order_id: 3006, amount: 3299, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Upi },
        SeedPayment { merchant_id: 11, order_id: 3007, amount: 3799, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },
        SeedPayment { merchant_id: 11, order_id: 3008, amount: 4299, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Card(CardType::Debit) },
        SeedPayment { merchant_id: 11, order_id: 3009, amount: 4799, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::NetBanking },
        SeedPayment { merchant_id: 12, order_id: 3010, amount: 5299, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::Cash },

        // =========================
        // EDGE CASES
        // =========================

        SeedPayment { merchant_id: 13, order_id: 4001, amount: 1, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Upi },

        SeedPayment { merchant_id: 13, order_id: 4002, amount: 0, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Upi },

        SeedPayment { merchant_id: 13, order_id: 4003, amount: -1, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Upi },

        SeedPayment { merchant_id: 13, order_id: 4004, amount: i64::MAX, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::NetBanking },

        SeedPayment { merchant_id: 13, order_id: 4005, amount: i64::MIN, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Cash },

        SeedPayment { merchant_id: 0, order_id: 4006, amount: 500, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Upi },

        SeedPayment { merchant_id: u64::MAX, order_id: 4007, amount: 500, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Upi },

        SeedPayment { merchant_id: 999999, order_id: 4008, amount: 99999999, status: PaymentStatus::Succeeded, mode_of_payment: ModeOfPayment::Card(CardType::Credit) },

        SeedPayment { merchant_id: 42, order_id: 4009, amount: 123456, status: PaymentStatus::Refunded, mode_of_payment: ModeOfPayment::NetBanking },

        SeedPayment { merchant_id: 404, order_id: 4010, amount: 4040, status: PaymentStatus::Failed, mode_of_payment: ModeOfPayment::Cash },
    ]
}
