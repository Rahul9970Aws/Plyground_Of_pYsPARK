#FACT LOG INFOMATION

| Column            | Description                                                               | Data Type     | Notes                                                                                   |
| ----------------- | ------------------------------------------------------------------------- | ------------- | --------------------------------------------------------------------------------------- |
| `fact_event_id`   | Surrogate primary key                                                     | BIGINT / INT  | Auto-increment or generated unique ID                                                   |
| `log_id`          | Unique identifier for the log/event                                       | VARCHAR       | From source log                                                                         |
| `user_id`         | User identifier                                                           | VARCHAR       | Foreign key to `Dim_User` (optional)                                                    |
| `event_date`      | Date of the event (extracted from timestamp)                              | DATE          | Useful for partitioning and reporting                                                   |
| `event_timestamp` | Exact timestamp of the event                                              | TIMESTAMP     |                                                                                         |
| `event_type`      | Type of event (`product_view`, `add_to_cart`, `checkout`)                 | VARCHAR       | FK to `Dim_EventType` if normalized                                                     |
| `product_id`      | Product involved                                                          | VARCHAR       | Nullable for `checkout` (because multiple products per event)                           |
| `category`        | Product category                                                          | VARCHAR       | Nullable                                                                                |
| `quantity`        | Quantity involved                                                         | INT / DECIMAL | Nullable for `product_view`                                                             |
| `unit_price`      | Price per unit                                                            | DECIMAL(10,2) | Nullable                                                                                |
| `total_amount`    | Total amount for the line item (quantity × unit\_price)                   | DECIMAL(10,2) | Nullable                                                                                |



#FACT LOG EVENTS INFROMATION

| Column           | Description                                                | Example    |
| ---------------- | ---------------------------------------------------------- | ---------- |
| `user_key`       | FK to `Dim_User` identifying the user                      | 10         |
| `event_date`     | Date of the event (grouped)                                | 2025-08-01 |
| `login_count`    | Count of distinct `log_id` values for that user & date     | 3          |
| `total_quantity` | Sum of quantity purchased on that date (NULL treated as 0) | 5          |
| `total_amount`   | Sum of total purchase amount for that date                 | 450.60     |
