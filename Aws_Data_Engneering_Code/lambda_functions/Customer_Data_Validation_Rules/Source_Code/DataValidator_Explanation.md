# DataValidator — Step-by-Step Explanation

## What Happens Step by Step

### `validate_customer_id`
- **Rule:** Must be `int > 0`  
- **Examples:** `1` ✅, `-2` ❌

### `validate_full_name`
- **Rule:** Only alphabets and spaces  
- **Examples:** `"John Doe"` ✅, `"Michael123"` ❌

### `validate_email`
- **Rule:** Standard email regex  
- **Examples:** `"john.doe@example.com"` ✅, `"alice.smith@example"` ❌ *(no domain)*

### `validate_age_range`
- **Rule:** Must be between `1` and `120`  
- **Examples:** `30` ✅, `200` ❌

### `validate_date_of_birth`
- **Rule:** Must be `YYYY-MM-DD`, **after 1900**, and **before today**  
- **Examples:** `"1993-05-10"` ✅, `"1800-01-01"` ❌

### `validate_phone_number`
- **Rule:** Must be **10 digits only**  
- **Examples:** `"1234567890"` ✅, `"98765abc12"` ❌

### `validate_country`
- **Rule:** Must be in `{ "USA", "UK", "India", "Canada", "Australia", "Ireland" }`  
- **Examples:** `"USA"` ✅, `"Mars"` ❌

### `check_missing_values`
- **Rule:** No nulls in mandatory fields

### `check_duplicates`
- **Rule:** No duplicate **full name + DOB** combination

### `check_age_consistency`
- **Rule:** Age must match DOB *(±1 year tolerance)*


---

## Each validation returns a Series of booleans

**Example:** `validate_customer_id`

```python
def validate_customer_id(self):
    return self._log_wrapper(
        lambda: self.df["customer_id"].apply(lambda x: isinstance(x, int) and x > 0),
        "Customer ID"
    )
```

- `self.df["customer_id"].apply(...)` runs the lambda on each row.
- It returns `True` if condition is satisfied, `False` otherwise.

**If** `customer_id = [1, -2, 3]`, the output will be:

```
0     True
1    False
2     True
dtype: bool
```

---

## Step 2: `run_all_validations` builds a DataFrame of all checks

```python
results = pd.DataFrame({
    "customer_id_valid": self.validate_customer_id(),
    "full_name_valid": self.validate_full_name(),
    "email_valid": self.validate_email(),
    # ... other checks ...
}, index=self.df.index)
```

- This creates **one column per validation**, filled with booleans.
- Each row matches the original DataFrame’s index.

**For the example dataset:**

|   | customer_id_valid | full_name_valid | email_valid | age_valid | dob_valid | phone_valid | country_valid | not_null | no_duplicates | age_consistency |
|---|-------------------|-----------------|-------------|-----------|-----------|-------------|---------------|----------|---------------|-----------------|
| 0 | True              | True            | True        | True      | True      | True        | True          | True     | True          | True            |
| 1 | False             | False           | False       | False     | False     | False       | False         | True     | True          | False           |
| 2 | True              | True            | True        | False     | True      | True        | True          | True     | True          | True            |

---

## Step 3: Combine with original DataFrame

```python
combined_df = self.df.join(results)
```

Now your original data gets **extra columns** showing which checks passed:

```
   customer_id    full_name                 email  age date_of_birth phone_number country  customer_id_valid  full_name_valid ...
0            1     John Doe   john.doe@example.com   30   1993-05-10   1234567890     USA               True             True ...
1           -2  Michael123    alice.smith@example   200   1800-01-01   98765abc12    Mars              False            False ...
2            3   Ana Smith  ana.smith@example.com   25   2000-08-15   9876543210   India               True             True ...
```

---

## Step 4: Mask valid vs invalid

```python
mask_valid = results.all(axis=1)
```

- `results.all(axis=1)` checks if **all columns in that row are `True`**.

**Example:**  
- Row 0: All `True` → **valid**  
- Row 1: At least one `False` → **invalid**  
- Row 2: One `False` (`age_valid`) → **invalid**

Finally, split:

```python
valid_df = combined_df[mask_valid]
invalid_df = combined_df[~mask_valid]
```

---

## TL;DR
- Each validator returns a boolean Series.  
- `run_all_validations` collects them into a results DataFrame.  
- You **join** results to the original data and use `all(axis=1)` to split **valid** vs **invalid** rows.
