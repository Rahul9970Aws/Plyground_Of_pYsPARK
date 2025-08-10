##FACT LOG INFOMATION

| Column                                         | Description                                                                              | Example               |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------- |
| **fact\_id** (PK)                              | Surrogate primary key for the fact table                                                 | `1`                   |
| **user\_key** (FK → Dim\_User)                 | Foreign key linking to the user dimension                                                | `10`                  |
| **product\_key** (FK → Dim\_Product, nullable) | Foreign key linking to the product dimension; nullable for non-product events            | `501`                 |
| **event\_date**                                | Calendar date of the event (useful for partitioning and date-based joins)                | `2025-08-01`          |
| **event\_timestamp**                           | Exact timestamp when the event occurred                                                  | `2025-08-01 09:30:00` |
| **event\_type\_key** (FK → Dim\_EventType)     | Foreign key referencing the event type (e.g., `product_view`, `add_to_cart`, `checkout`) | `1`                   |
| **quantity**                                   | Quantity involved in the event; null for non-transaction events like views               | `2`                   |
| **unit\_price**                                | Price per unit at the time of the event                                                  | `75.20`               |
| **total\_amount**                              | Calculated as `quantity × unit_price` for transactional events                           | `150.40`              |
| **log\_id**                                    | Unique identifier from the source system for traceability                                | `log_005`             |



##FACT LOG EVENTS INFROMATION

| Column           | Description                                                | Example    |
| ---------------- | ---------------------------------------------------------- | ---------- |
| `user_key`       | FK to `Dim_User` identifying the user                      | 10         |
| `event_date`     | Date of the event (grouped)                                | 2025-08-01 |
| `login_count`    | Count of distinct `log_id` values for that user & date     | 3          |
| `total_quantity` | Sum of quantity purchased on that date (NULL treated as 0) | 5          |
| `total_amount`   | Sum of total purchase amount for that date                 | 450.60     |
