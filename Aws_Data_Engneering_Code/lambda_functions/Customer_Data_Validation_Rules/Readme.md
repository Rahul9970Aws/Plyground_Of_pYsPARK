# Data Validation Rules

## V1. customer_id

-   **Validation Rule:** Must be unique, integer, \> 0\
-   **Example Valid:** 1, 2, 30\
-   **Example Invalid:** Duplicate IDs\
-   **Test Case Description:** Check duplicates and non-numeric IDs

## V2. full_name

-   **Validation Rule:** Only alphabets and spaces\
-   **Example Valid:** "John Doe"\
-   **Example Invalid:** "Michael123", "Ana@na"\
-   **Test Case Description:** Validate names

## V3. email

-   **Validation Rule:** Must match standard email format\
-   **Example Valid:** "john.doe@example.com"\
-   **Example Invalid:** "alice.smith@example", "invalid@@user"\
-   **Test Case Description:** Regex email validation

## V4. age

-   **Validation Rule:** Integer, 1--120\
-   **Example Valid:** 29, 40\
-   **Example Invalid:** -5, 150, 0\
-   **Test Case Description:** Range validation

## V5. date_of_birth

-   **Validation Rule:** Valid date, not in future, realistic\
-   **Example Valid:** 1996-04-12\
-   **Example Invalid:** 2028-01-01, 1995-14-10\
-   **Test Case Description:** Check format and logical correctness

## V6. phone_number

-   **Validation Rule:** 10 digits only\
-   **Example Valid:** 9876543210\
-   **Example Invalid:** 12345, abcdefghij\
-   **Test Case Description:** Numeric and length validation

## V7. country

-   **Validation Rule:** Must be in allowed list \[USA, UK, India,
    Canada, Australia, Ireland\]\
-   **Example Valid:** USA, UK\
-   **Example Invalid:** "Mars", blank\
-   **Test Case Description:** Reference data check

## V8. Missing values

-   **Validation Rule:** No mandatory field should be null\
-   **Example Valid:** Full name present\
-   **Example Invalid:** Blank name\
-   **Test Case Description:** Null check

## V9. Duplicates

-   **Validation Rule:** full_name + date_of_birth combination must be
    unique\
-   **Example Valid:** Emily Davis\
-   **Example Invalid:** John Doe repeated\
-   **Test Case Description:** Duplicate check

## V10. Consistency

-   **Validation Rule:** Age should match date_of_birth (approx.)\
-   **Example Valid:** DOB: 1996 → Age: 29\
-   **Example Invalid:** DOB: 1870 with age 150\
-   **Test Case Description:** Consistency check
